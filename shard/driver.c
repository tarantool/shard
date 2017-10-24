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
#include <stdio.h>
#include <stdlib.h>
#include <lua.h>
#include <lauxlib.h>
#include <module.h>

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
	struct source *sources;
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
	if (source->tuple != NULL)
		box_tuple_unref(source->tuple);
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
		if (merger->sources[i].tuple != NULL)
			box_tuple_unref(merger->sources[i].tuple);
	}
	merger->count = 0;
	free(merger->sources);
	merger->sources = NULL;
	merger_heap_destroy(&merger->heap);
	merger_heap_create(&merger->heap);
}

static inline struct source *
merger_create_source(struct merger *merger, uint32_t i, struct ibuf *buf)
{
	assert(i < merger->count);
	const char **p = (const char **)&buf->rpos;
	if (mp_typeof(**p) != MP_MAP || mp_decode_map(p) != 1 ||
	    mp_typeof(**p) != MP_UINT || mp_decode_uint(p) != IPROTO_DATA ||
	    mp_typeof(**p) != MP_ARRAY) {
		free_sources(merger);
		return NULL;
	}
	mp_decode_array(p);
	struct source *source = &merger->sources[i];
	source->buf = buf;
	return source;
}

static inline struct merger *
luaL_merger(struct lua_State *L, int index)
{
	struct merger **merger_ptr;
	uint32_t cdata_type;
	merger_ptr = luaL_checkcdata(L, index, &cdata_type);
	if (merger_ptr == NULL || cdata_type != merger_type_id) {
		luaL_error(L, "Expected merger");
		return NULL;
	}
	return *merger_ptr;
}

static int
lbox_merger_start(struct lua_State *L)
{
	if (lua_gettop(L) != 3 || !lua_istable(L, 2) || !lua_isnumber(L, 3)) {
		return luaL_error(L, "Bad params, use: start(merger, "\
				     "{buffers}, order)");
	}
	struct merger *merger = luaL_merger(L, 1);
	merger->order =	lua_tointeger(L, 3) >= 0? 1: -1;
	uint32_t src_count = lua_objlen(L, 2);
	free_sources(merger);

	merger->sources = (struct source *)calloc(src_count,
						  sizeof(struct source));
	if (merger->sources == NULL)
		return luaL_error(L, "Can't alloc sources buffer");
	merger->count = src_count;
	/* Fetch all sources */
	for (uint32_t i = 0; i < src_count; ++i) {
		lua_rawgeti(L, 2, i + 1);
		assert(! lua_isnil(L, -1));
		struct ibuf *buf = (struct ibuf *)lua_topointer(L, -1);
		lua_pop(L, 1);
		assert(buf != NULL);
		/* At least iproto header exists. */
		assert(ibuf_used(buf) != 0);
		struct source *source = merger_create_source(merger, i, buf);
		if (source == NULL) {
			free_sources(merger);
			return luaL_error(L, "Incorrect response");
		}
		source_fetch(source, merger->format);
		if (source->tuple != NULL)
			merger_heap_insert(&merger->heap, &source->hnode);
	}
	lua_pushboolean(L, true);
	return 1;
}

static int
lbox_merge_next(struct lua_State *L)
{
	struct merger *merger = luaL_merger(L, 1);
	struct heap_node *hnode = merger_heap_top(&merger->heap);
	if (hnode == NULL) {
		lua_pushnil(L);
		return 1;
	}
	struct source *source = container_of(hnode, struct source, hnode);
	luaT_pushtuple(L, source->tuple);
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
	const char *error_msg = NULL;
	if (lua_gettop(L) != 1 || lua_istable(L, 1) != 1) {
		return luaL_error(L, "Bad params, use: new({"\
				     "{fieldno = fieldno, type = type}, ...}");
	}
	uint32_t part_count = lua_objlen(L, 1);
	uint32_t *fieldno = (uint32_t *)malloc(sizeof(*fieldno) * part_count);
	if (fieldno == NULL)
		return luaL_error(L, "Can not alloc fieldno buffer");
	enum field_type *type =
		(enum field_type *)malloc(sizeof(*type) * part_count);
	if (type == NULL) {
		free(fieldno);
		return luaL_error(L, "Can not alloc type buffer");
	}
	for (uint32_t i = 0; i < part_count; ++i) {
		lua_rawgeti(L, 1, i + 1);
		if (lua_isnil(L, -1)) {
			error_msg = "Incorrect parts array";
			goto error;
		}
		lua_getfield(L, -1, "fieldno");
		if (lua_isnil(L, -1)) {
			error_msg = "Fieldno not found";
			goto error;
		}
		fieldno[i] = lua_tointeger(L, -1);
		lua_pop(L, 1);
		lua_getfield(L, -1, "type");
		if (lua_isnil(L, -1)) {
			error_msg = "Type not found";
			goto error;
		}
		type[i] = lua_tointeger(L, -1);
		lua_pop(L, 1);
		lua_pop(L, 1);
	}

	struct merger *merger = calloc(1, sizeof(*merger));
	if (merger == NULL) {
		error_msg = "Can not alloc merger";
		goto error;
	}

	merger->key_def = box_key_def_new(fieldno, type, part_count);
	if (merger->key_def == NULL) {
		error_msg = "Can not alloc key_def";
		goto error;
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

error:
	free(fieldno);
	free(type);
	return luaL_error(L, error_msg);
}

static int
lbox_merger_cmp(lua_State *L)
{
	if (lua_gettop(L) != 2)
		return luaL_error(L, "Bad params, use: cmp(merger, key)");
	struct merger *merger = luaL_merger(L, 1);
	const char *key = lua_tostring(L, 2);
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
	struct merger *merger = luaL_merger(L, 1);
	free_sources(merger);
	box_key_def_delete(merger->key_def);
	box_tuple_format_unref(merger->format);
	free(merger);
	return 0;
}


LUA_API int
luaopen_shard_driver(lua_State *L)
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
