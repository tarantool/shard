/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <module.h>

#include <stdio.h>
#include <stdlib.h>

#include <lua.h>
#include <lauxlib.h>

#include "ibuf.h"
#include "msgpuck.h"

struct source {
	struct ibuf *buf;
	struct tuple *tuple;
};

static uint32_t comparator_type_id = 0;

struct comparator {
	struct key_def *key_def;
	box_tuple_format_t *format;
};

struct cmp_arg {
	struct comparator *comparator;
	int order;
};

static inline void
source_shift(struct source *source, box_tuple_format_t *format)
{
	if (source->tuple != NULL || ibuf_used(source->buf) == 0)
		return;
	const char *tuple_beg = source->buf->rpos;
	const char *tuple_end = tuple_beg;
	mp_next(&tuple_end);
	assert(tuple_end <= source->buf->wpos);
	source->buf->rpos = (char *)tuple_end;
	source->tuple = box_tuple_new(format, tuple_beg, tuple_end);
	box_tuple_ref(source->tuple);
}

static int
source_cmp(struct source *left, struct source *right, struct cmp_arg *cmp_arg)
{
	if (left->tuple == NULL && right->tuple == NULL)
		return 0;
	if (left->tuple == NULL)
		return 1;
	if (right->tuple == NULL)
		return -1;
	return cmp_arg->order *
	       box_tuple_compare(left->tuple, right->tuple,
				 cmp_arg->comparator->key_def);
}

static int
lbox_merge(struct lua_State *L)
{
	struct comparator **comparator;
	uint32_t cdata_type;
	if (lua_gettop(L) != 5 || lua_istable(L, 1) != 1 ||
	    lua_isnumber(L, 2) != 1 || lua_isnumber(L, 3) != 1 ||
	    lua_isnumber(L, 4) != 1 ||
	    (comparator = luaL_checkcdata(L, 5, &cdata_type)) == NULL ||
	    cdata_type != comparator_type_id) {
		return luaL_error(L, "Bad params, use: merge({buffers}, "
				  "skip, limit, order, comparator)");
	}
	uint32_t skip = lua_tointeger(L, 2);
	uint32_t limit = lua_tointeger(L, 3);

	struct cmp_arg arg = {
		*comparator,
		lua_tointeger(L, 4) >= 0? 1: -1};
	if (arg.comparator == NULL)
		return luaL_error(L, "Invalid comparator");

	struct source *sources;
	uint32_t count = 0, capacity = 8;
	sources = (struct source *)malloc(capacity * sizeof(*sources));
	if (sources == NULL)
		return luaL_error(L, "Can not alloc merge buffer");

	/* Fetch all sources */
	while (true) {
		lua_pushinteger(L, count + 1);
		lua_gettable(L, 1);
		if (lua_isnil(L, -1))
			break;
		struct ibuf *buf = (struct ibuf *)lua_topointer(L, -1);
		if (buf == NULL)
			break;
		if (ibuf_used(buf) == 0)
			continue;
		if (count == capacity) {
			capacity *= 2;
			struct source *old_sources = sources;
			sources = (struct source *)realloc(sources,
				capacity * sizeof(*sources));
			if (sources == NULL) {
				free(old_sources);
				return luaL_error(L, "Can not alloc merge buffer");
			}
		}
		sources[count].buf = buf;
		sources[count].buf->rpos += 7;
		sources[count].tuple = NULL;
		source_shift(sources + count, (*comparator)->format);
		++count;
	}
	/* Initial sort sources */
	qsort_r(sources, count, sizeof(*sources),
		(int (*)(const void *, const void *, void *))source_cmp, &arg);

	uint32_t pos, pushed = 0;
	for (pos = 0; pos < skip + limit && sources->tuple != NULL; ++pos) {
		if (pos >= skip) {
			luaT_pushtuple(L, sources[0].tuple);
			++pushed;
		}

		box_tuple_unref(sources[0].tuple);
		sources[0].tuple = NULL;
		source_shift(sources, (*comparator)->format);
;		/* Find first source greather than current first source */
		uint32_t beg = 1, end = count, mid;
		while (end - beg > 1) {
			mid = (beg + end) / 2;
			int cmp = source_cmp(sources, sources + mid, &arg);
			if (cmp == 0)
				break;
			if (cmp < 0)
				end = mid;
			else
				beg = mid;
		}
		mid = (beg + end) / 2;
		if (source_cmp(sources, sources + mid, &arg) >= 0)
			++mid;
		if (mid == 1) {
			/* Nothing to move */
			continue;
		}
		struct source tmp = sources[0];
		memmove(sources, sources + 1, sizeof(*sources) * (mid - 1));
		sources[mid - 1] = tmp;
	}
	/* unref all created tuples */
	for (pos = 0; pos < count; ++pos)
		if (sources[pos].tuple != NULL)
			box_tuple_unref(sources[pos].tuple);
	free(sources);
	return pushed;
}

static int
lbox_merge_new(struct lua_State *L)
{
	if (lua_gettop(L) != 1 || lua_istable(L, 1) != 1) {
		return luaL_error(L, "Bad params, use: new({"
				  "{fieldno = fieldno, type = type}, ...}");
	}
	uint16_t count = 0, capacity = 8;
	uint32_t *fieldno = NULL;
	enum field_type *type = NULL;
	fieldno = (uint32_t *)malloc(sizeof(*fieldno) * capacity);
	if (fieldno == NULL)
		return luaL_error(L, "Can not alloc fieldno buffer");
	type = (enum field_type *)malloc(sizeof(*type) * capacity);
	if (type == NULL) {
		free(fieldno);
		return luaL_error(L, "Can npt alloc type buffer");
	}
	while (true) {
		lua_pushinteger(L, count + 1);
		lua_gettable(L, 1);
		if (lua_isnil(L, -1))
			break;
		if (count == capacity) {
			capacity *= 2;
			uint32_t *old_fieldno = fieldno;
			fieldno = (uint32_t *)realloc(fieldno,
				sizeof(*fieldno) * capacity);
			if (fieldno == NULL) {
				free(old_fieldno);
				free(type);
				return luaL_error(L, "Can not alloc fieldno buffer");
			}
			enum field_type *old_type = type;
			type = (enum field_type *)realloc(type,
				sizeof(*type) * capacity);
			if (type == NULL) {
				free(fieldno);
				free(old_type);
				return luaL_error(L, "Can not alloc type buffer");
			}
		}
		lua_pushstring(L, "fieldno");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1))
			break;
		fieldno[count] = lua_tointeger(L, -1);
		lua_pop(L, 1);
		lua_pushstring(L, "type");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1))
			break;
		type[count] = lua_tointeger(L, -1);
		lua_pop(L, 1);
		++count;
	}

	struct comparator *comparator = malloc(sizeof(*comparator));
	if (comparator == NULL) {
		free(fieldno);
		free(type);
		return luaL_error(L, "Can not alloc comparator");
	}
	comparator->key_def = (box_key_def_t *)malloc(box_key_def_size(count));
	if (comparator->key_def == NULL) {
		free(fieldno);
		free(type);
		return luaL_error(L, "Can not alloc key_def");
	}
	box_key_def_create(comparator->key_def, fieldno, type, count);
	free(fieldno);
	free(type);

	comparator->format = box_tuple_format_new(&comparator->key_def, 1);
	if (comparator->format == NULL) {
		free(comparator->key_def);
		free(comparator);
		return luaL_error(L, "Can not create tuple format");
	}

	*(struct comparator **)luaL_pushcdata(L, comparator_type_id) = comparator;
	return 1;
}

static int
lbox_merge_del(lua_State *L)
{
	struct comparator **comparator;
	uint32_t cdata_type;
	if ((comparator = luaL_checkcdata(L, 5, &cdata_type)) == NULL ||
	    cdata_type != comparator_type_id)
		return 0;
	free((*comparator)->key_def);
	box_tuple_format_ref((*comparator)->format, -1);
	free(*comparator);
	return 0;
}


LUA_API int
luaopen_driver(lua_State *L)
{
	luaL_cdef(L, "struct comparator;");
	comparator_type_id = luaL_ctypeid(L, "struct comparator&");
	lua_newtable(L);
	static const struct luaL_reg meta [] = {
		{"merge_new", lbox_merge_new},
		{"merge", lbox_merge},
		{"merge_del", lbox_merge_del},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
