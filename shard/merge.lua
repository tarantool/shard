local ffi = require('ffi')
local bheap = require('shard.bheap')
local msgpackffi  = require('msgpackffi')
local pickle = require('pickle')

ffi.cdef[[

typedef struct key_def box_key_def_t;
box_key_def_t *
box_key_def_new(uint32_t *fields, uint32_t *types, uint32_t part_count);

void
box_key_def_delete(box_key_def_t *key_def);

typedef struct tuple_format box_tuple_format_t;
box_tuple_format_t *
box_tuple_format_new(box_key_def_t **keys, uint16_t key_count);

void
box_tuple_format_unref(box_tuple_format_t *format);

typedef struct tuple box_tuple_t;
int
box_tuple_compare(const box_tuple_t *tuple_a, const box_tuple_t *tuple_b,
                  const box_key_def_t *key_def);
int
box_tuple_compare_with_key(const box_tuple_t *tuple_a, const char *key_b,
                           const box_key_def_t *key_def);

box_tuple_t *
box_tuple_new(box_tuple_format_t *format, const char *data, const char *end);

]]

-- field type map
local field_types = {
    any       = 0,
    unsigned  = 1,
    string    = 2,
    array     = 3,
    number    = 4,
    integer   = 5,
    scalar    = 6
}

local function decode_iproto_header(source)
    if source.wpos - source.rpos < 7 then
        return error('Invalid data stream')
    end
    local map_id = pickle.unpack('b', ffi.string(source.rpos, 1))
    if map_id ~= 0x81 then -- one pair sized map
        return error('Invalid data stream')
    end
    source.rpos = source.rpos + 1
    local key = pickle.unpack('b', ffi.string(source.rpos, 1))
    if key ~= 0x30 then -- IPROTO_DATA key
        return error('Invalid data stream')
    end
    source.rpos = source.rpos + 1
    local arr_id = pickle.unpack('b', ffi.string(source.rpos, 1))
    if arr_id ~= 0xdc and arr_id ~= 0xdd then
        return error('Invalid data stream')
    end
    source.rpos = source.rpos + 1
    local arr_size = pickle.unpack(arr_id == 0xdc and 'n' or 'N',
                                   ffi.string(source.rpos,
                                              arr_id == 0xdc and 2 or 4))
    source.rpos = source.rpos + (arr_id == 0xdc and 2 or 4)
    assert(source.rpos <= source.wpos)
    return arr_size
end

local function merge_fetch(merge, item)
    item.lua_tuple = nil
    item.box_tuple = nil
    if item.data.rpos >= item.data.wpos then
        return false
    end
    local new_rpos
    -- there is no api for mp_next, just decode tuple one time
    item.lua_tuple, new_rpos = msgpackffi.decode(item.data.rpos)
    item.box_tuple = ffi.C.box_tuple_new(merge.format, item.data.rpos, new_rpos)
    ffi.C.box_tuple_ref(item.box_tuple)
    -- cast tuple cdata type to native tarantool tuple representation
    item.box_tuple = ffi.cast('const struct tuple &', item.box_tuple)
    ffi.gc(item.box_tuple, ffi.C.box_tuple_unref)
    item.data.rpos = ffi.cast('char *', new_rpos)
    assert(item.data.rpos <= item.data.wpos)
    return true
end

local function merge_start(merge, sources, order)
    merge.order = order
    merge.bheap = bheap.new(merge.cmp)
    for _, source in ipairs(sources) do
         local len = decode_iproto_header(source)
         local item = {data = source}
         if merge_fetch(merge, item) then
             merge.bheap:insert(item)
         end
    end
    merge.bheap:start()
end

local function merge_next(merge)
    local item = merge.bheap:top()
    if item == nil then
        return nil
    end
    local tuple = item.box_tuple
    if merge_fetch(merge, item) then
        merge.bheap:replace(item)
    else
        merge.bheap:pop()
    end
    return tuple
end

local function merge_cmp(merge, key)
    local item = merge.bheap:top()
    if item == nil then
        return nil
    end
    return ffi.C.box_tuple_compare_with_key(item.box_tuple, key,
                                             merge.key_defs[0]) * merge.order
end

local function merge_new(parts)
    local merge = {start = merge_start, next = merge_next}
    -- prepare partitions
    local part_count = #parts
    local field_no = ffi.new('uint32_t[' .. tostring(part_count) .. ']')
    local field_type = ffi.new('uint32_t[' .. tostring(part_count) .. ']')
    for i = 1, part_count do
        field_no[i - 1] = parts[i].fieldno - 1
        field_type[i - 1] = field_types[parts[i].type] or 0
    end
    
    -- create key_def and format
    merge.key_defs = ffi.new('box_key_def_t*[1]')
    merge.key_defs[0] = ffi.C.box_key_def_new(field_no, field_type, part_count)
    ffi.gc(merge.key_defs[0], ffi.C.box_key_def_delete)
    merge.format = ffi.C.box_tuple_format_new(merge.key_defs, 1);
    ffi.gc(merge.format, ffi.C.box_tuple_format_unref)
    merge.cmp = function (left, right)
        return ffi.C.box_tuple_compare(left.box_tuple, right.box_tuple,
                                       merge.key_defs[0]) * merge.order < 0
    end
    return merge
end

return {
    new = merge_new
}
