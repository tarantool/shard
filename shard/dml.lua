local ffi = require('ffi')
local buffer = require('buffer')
local merge = require('shard.merge')

local iterator_direction = {
    [box.index.EQ] = 1,
    [box.index.GT] = 1,
    [box.index.GE] = 1,
    [box.index.ALL] = 1,

    [box.index.REQ] = -1,
    [box.index.LT] = -1,
    [box.index.LE] = -1,
}

local iterator_type_from_str = {
    EQ = box.index.EQ,
    GT = box.index.GT,
    GE = box.index.GE,
    ALL = box.index.ALL,
    REQ = box.index.REQ,
    LT = box.index.LT,
    LE = box.index.LE,
}

local function server_request(server, operation, netbox_opts, ...)
    local c = server.conn
    local ok, ret = pcall(c._request, c, operation, netbox_opts, ...)
    if not ok then
        error(string.format('failed to execute operation on %s: %s',
                            server.uri, ret))
    end
    return ret
end

local function dml_module_cfg(shard_module)
    local merger = {}
    local function get_merger(space_obj, index_id)
        if merger[space_obj.name] == nil then
            merger[space_obj.name] = {}
        end
        if merger[space_obj.name][index_id] == nil then
            local index = space_obj.index[index_id]
            merger[space_obj.name][index_id] = merge.new(index.parts)
        end
        return merger[space_obj.name][index_id]
    end

    -- shards request function
    local function request(tuple_id, operation, netbox_opts, ...)
        local server = shard_module.server_by_key(tuple_id)
        if server == nil then
            error('A requested replica set is down')
        end
        return server_request(server, operation, netbox_opts, ...)
    end

    local function mr_select(space_id, index_id, iterator, offset, limit, key,
                             netbox_opts)
        local results = {}
        local merge_obj = nil
        if limit == nil then
            limit = 1000
        end
        for _, replica_set in ipairs(shard_module.replica_sets) do
            local server = replica_set.master
            if server ~= nil then
                netbox_opts.buffer = buffer.ibuf()
                if merge_obj == nil then
                    merge_obj = get_merger(server.conn.space[space_id],
                                           index_id)
                end
                -- A response is stored in buf.
                server_request(server, 'select', netbox_opts, space_id,
                               index_id, iterator, offset, limit, key)
                table.insert(results, netbox_opts.buffer)
                netbox_opts.buffer = nil
            end
        end
        merge_obj:start(results, iterator_direction[iterator])
        local tuples = {}
        while #tuples < limit do
            local tuple = merge_obj:next()
            if tuple == nil then
                break
            end
            table.insert(tuples, tuple)
        end
        return tuples
    end

    local key_extract = {}
    local function extract_key(space, data)
        if key_extract[space.name] == nil then
            local pk = space.index[0]
            key_extract[space.name] = function(tuple)
                local key = {}
                for _, part in ipairs(pk.parts) do
                    table.insert(key, tuple[part.fieldno])
                end
                return key
            end
        end
        return key_extract[space.name](data)
    end

    local space_methods = {}
    function space_methods:insert(tuple, netbox_opts)
        box.internal.check_space_arg(self, 'insert')
        local tuple_id = extract_key(self, tuple)
        return request(tuple_id, 'insert', netbox_opts, self.id, tuple)[1]
    end

    function space_methods:replace(tuple, netbox_opts)
        box.internal.check_space_arg(self, 'replace')
        local tuple_id = extract_key(self, tuple)
        return request(tuple_id, 'replace', netbox_opts, self.id, tuple)[1]
    end

    function space_methods:select(key, select_opts, netbox_opts)
        box.internal.check_space_arg(self, 'select')
        return self.index[0]:select(key, select_opts, netbox_opts)
    end

    function space_methods:delete(key, netbox_opts)
        box.internal.check_space_arg(self, 'delete')
        local ret = request(key, 'delete', netbox_opts, self.id, 0, key)
        if ret ~= nil then
            return ret[1]
        else
            return nil
        end
    end

    function space_methods:update(key, oplist, netbox_opts)
        box.internal.check_space_arg(self, 'update')
        return request(key, 'update', netbox_opts, self.id, 0, key, oplist)[1]
    end

    function space_methods:upsert(tuple, oplist, netbox_opts)
        box.internal.check_space_arg(self, 'upsert')
        local tuple_id = extract_key(self, tuple)
        request(tuple_id, 'upsert', netbox_opts, self.id, tuple, oplist)
    end

    function space_methods:get(key, netbox_opts)
        box.internal.check_space_arg(self, 'get')
        local ret = request(key, 'select', netbox_opts, self.id, 0,
                            box.index.EQ, 0, 2, key)
        if ret[2] ~= nil then box.error(box.error.MORE_THAN_ONE_TUPLE) end
        return ret[1]
    end

    local index_methods = {}
    function index_methods:select(key, select_opts, netbox_opts)
        box.internal.check_index_arg(self, 'select')
        select_opts = select_opts or {}
        key = key or {}
        local key_is_empty = #key == 0
        local iter = select_opts.iterator
        if iter == nil then
            if key_is_empty then
                iter = box.index.ALL
            else
                iter = box.index.EQ
            end
        elseif type(iter) == 'string' then
            iter = iterator_type_from_str[iter]
            if iter == nil then
                error('Unknow iterator type')
            end
        end
        if self.id == 0 and not key_is_empty and
           (iter == box.index.EQ or iter == box.index.REQ) then
            return request(key, 'select', netbox_opts, self.space_id, 0,
                           box.index.EQ, 0, 0xFFFFFFFF, key)
        end
        netbox_opts = netbox_opts or {}
        return mr_select(self.space_id, self.id, iter, select_opts.offset,
                         select_opts.limit, key, netbox_opts)
    end

    for space_id, space in pairs(shard_module.space) do
        if type(space_id) == 'number' then
            setmetatable(space, { __index = space_methods })
            for index_id, index in pairs(space.index) do
                if type(index_id) == 'number' then
                    setmetatable(index, { __index = index_methods })
                end
            end
        end
    end
end

return { init_schema_methods = dml_module_cfg }
