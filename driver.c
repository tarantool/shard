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

#define HEAP_FORWARD_DECLARATION
#include "heap.h"

#define IPROTO_DATA 0x30

struct source {
	struct heap_node hnode;
	struct ibuf *buf;
	struct tuple *tuple;
};

static uint32_t merger_type_id = 0;

struct merger {
	heap_t heap;
	uint32_t count;
	uint32_t capacity;
	struct source **sources;
	struct key_def *key_def;
	box_tuple_format_t *format;
	int order;
};

static bool
source_less(const heap_t *heap, const struct heap_node *a,
	    const struct heap_node *b)
{
	struct source *left = container_of(a, struct source, hnode);
	struct source *right = container_of(b, struct source, hnode);
	if (left->tuple == NULL && right->tuple == NULL)
		return false;
	if (left->tuple == NULL)
		return false;
	if (right->tuple == NULL)
		return true;
	struct merger *merger = container_of(heap, struct merger, heap);
	return merger->order *
	       box_tuple_compare(left->tuple, right->tuple, merger->key_def) < 0;
}

#define HEAP_NAME merger_heap
#define HEAP_LESS source_less
#include "heap.h"

static inline void
source_fetch(struct source *source, box_tuple_format_t *format)
{
	source->tuple = NULL;
	if (ibuf_used(source->buf) == 0)
		return;
	const char *tuple_beg = source->buf->rpos;
	const char *tuple_end = tuple_beg;
	mp_next(&tuple_end);
	assert(tuple_end <= source->buf->wpos);
	source->buf->rpos = (char *)tuple_end;
	source->tuple = box_tuple_new(format, tuple_beg, tuple_end);
	box_tuple_ref(source->tuple);
}

static void
free_sources(struct merger *merger)
{
	for (uint32_t i = 0; i < merger->count; ++i) {
		if (merger->sources[i]->tuple != NULL)
			box_tuple_unref(merger->sources[i]->tuple);
		free(merger->sources[i]);
	}
	merger->count = 0;
	free(merger->sources);
	merger->capacity = 0;
	merger_heap_destroy(&merger->heap);
	merger_heap_create(&merger->heap);
}

static int
lbox_merger_start(struct lua_State *L)
{
	struct merger **merger_ptr;
	uint32_t cdata_type;
	if (lua_gettop(L) != 3 || lua_istable(L, 2) != 1 ||
	    lua_isnumber(L, 3) != 1 ||
	    (merger_ptr = luaL_checkcdata(L, 1, &cdata_type)) == NULL ||
	    cdata_type != merger_type_id) {
		return luaL_error(L, "Bad params, use: start(merger, {buffers}, "
				  "order)");
	}
	struct merger *merger = *merger_ptr;
	merger->order =	lua_tointeger(L, 3) >= 0? 1: -1;
	free_sources(merger);

	merger->capacity = 8;
	merger->sources = (struct source **)malloc(merger->capacity *
						   sizeof(struct source));
	if (merger->sources == NULL)
		return luaL_error(L, "Can't alloc sources buffer");
	/* Fetch all sources */
	while (true) {
		lua_pushinteger(L, merger->count + 1);
		lua_gettable(L, 2);
		if (lua_isnil(L, -1))
			break;
		struct ibuf *buf = (struct ibuf *)lua_topointer(L, -1);
		if (buf == NULL)
			break;
		if (ibuf_used(buf) == 0)
			continue;
		if (merger->count == merger->capacity) {
			merger->capacity *= 2;
			struct source **new_sources;
			new_sources =
				(struct source **)realloc(merger->sources,
					merger->capacity * sizeof(struct source *));
			if (new_sources == NULL) {
				free_sources(merger);
				return luaL_error(L, "Can't alloc sources buffer");
			}
			merger->sources = new_sources;
		}
		merger->sources[merger->count] =
			(struct source *)malloc(sizeof(struct source *));
		if (merger->sources[merger->count] == NULL) {
			free_sources(merger);
			return luaL_error(L, "Can't alloc merge source");
		}
		if (mp_typeof(*buf->rpos) != MP_MAP ||
		    mp_decode_map((const char **)&buf->rpos) != 1 ||
		    mp_typeof(*buf->rpos) != MP_UINT ||
		    mp_decode_uint((const char **)&buf->rpos) != IPROTO_DATA ||
		    mp_typeof(*buf->rpos) != MP_ARRAY) {
			free_sources(merger);
			return luaL_error(L, "Invalid merge source");
		}
		mp_decode_array((const char **)&buf->rpos);
		merger->sources[merger->count]->buf = buf;
		merger->sources[merger->count]->tuple = NULL;
		source_fetch(merger->sources[merger->count], merger->format);
		if (merger->sources[merger->count]->tuple != NULL)
			merger_heap_insert(&merger->heap,
					   &merger->sources[merger->count]->hnode);
		++merger->count;
	}
	lua_pushboolean(L, true);
	return 1;
}

static int
lbox_merge_next(struct lua_State *L)
{
	struct merger **merger_ptr;
	uint32_t cdata_type;
	if (lua_gettop(L) != 1 ||
	    (merger_ptr = luaL_checkcdata(L, 1, &cdata_type)) == NULL ||
	    cdata_type != merger_type_id) {
		return luaL_error(L, "Bad params, use: next(merger)");
	}
	struct merger *merger = *merger_ptr;
	struct heap_node *hnode = merger_heap_top(&merger->heap);
	if (hnode == NULL) {
		lua_pushnil(L);
		return 1;
	}
	struct source *source = container_of(hnode, struct source, hnode);
	luaT_pushtuple(L, source->tuple);
	box_tuple_unref(source->tuple);
	source_fetch(source, merger->format);
	if (source->tuple == NULL)
		merger_heap_delete(&merger->heap, hnode);
	else
		merger_heap_update(&merger->heap, hnode);
	return 1;
}

static int
lbox_merger_new(struct lua_State *L)
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
		return luaL_error(L, "Can not alloc type buffer");
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

	struct merger *merger = calloc(1, sizeof(*merger));
	if (merger == NULL) {
		free(fieldno);
		free(type);
		return luaL_error(L, "Can not alloc merger");
	}
	merger->key_def = box_key_def_new(fieldno, type, count);
	if (merger->key_def == NULL) {
		free(fieldno);
		free(type);
		return luaL_error(L, "Can not alloc key_def");
	}
	free(fieldno);
	free(type);

	merger->format = box_tuple_format_new(&merger->key_def, 1);
	if (merger->format == NULL) {
		box_key_def_delete(merger->key_def);
		free(merger);
		return luaL_error(L, "Can not create tuple format");
	}

	*(struct merger **)luaL_pushcdata(L, merger_type_id) = merger;
	return 1;
}

static int
lbox_merger_cmp(lua_State *L)
{
	struct merger **merger_ptr;
	uint32_t cdata_type;
	if (lua_gettop(L) != 2 ||
	    (merger_ptr = luaL_checkcdata(L, 1, &cdata_type)) == NULL ||
	    cdata_type != merger_type_id)
		return luaL_error(L, "Bad params, use: cmp(merger, key)");
	const char *key = lua_tostring(L, 2);
	struct merger *merger = *merger_ptr;
	struct heap_node *hnode = merger_heap_top(&merger->heap);
	if (hnode == NULL) {
		lua_pushnil(L);
		return 1;
	}
	struct source *source = container_of(hnode, struct source, hnode);
	lua_pushinteger(L, box_tuple_compare_with_key(source->tuple, key,
						      merger->key_def) *
			   merger->order);
	return 1;
}

static int
lbox_merger_del(lua_State *L)
{
	struct merger **merger_ptr;
	uint32_t cdata_type;
	if ((merger_ptr = luaL_checkcdata(L, 1, &cdata_type)) == NULL ||
	    cdata_type != merger_type_id)
		return 0;
	struct merger *merger = *merger_ptr;
	free_sources(merger);
	box_key_def_delete(merger->key_def);
	box_tuple_format_unref(merger->format);
	free(merger);
	return 0;
}


LUA_API int
luaopen_driver(lua_State *L)
{
	luaL_cdef(L, "struct merger;");
	merger_type_id = luaL_ctypeid(L, "struct merger&");
	lua_newtable(L);
	static const struct luaL_Reg meta [] = {
		{"merge_new", lbox_merger_new},
		{"merge_start", lbox_merger_start},
		{"merge_cmp", lbox_merger_cmp},
		{"merge_next", lbox_merge_next},
		{"merge_del", lbox_merger_del},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
