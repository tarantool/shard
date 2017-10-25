local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local remote = require('net.box')
local uuid = require('uuid')
local ffi = require('ffi')
local buffer = require('buffer')
local config_util = require('shard.config_util')

-- tuple array merge driver
local driver = require('shard.driver')

-- field type map
local field_types = {
    any       = 0,
    unsigned  = 1,
    string    = 2,
    number    = 3,
    integer   = 4,
    boolean   = 5,
    scalar    = 6,
    array     = 7,
    map       = 8,
}

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

local epoch_counter = 1
local self_server = nil
local request_timeout = nil
local space_routers = nil

local merger = {}
local function merge_new(key_parts)
    local parts = {}
    for _, v in pairs(key_parts) do
        if v.fieldno <= 0 then
            error('Invalid field number')
        end
        if field_types[v.type] ~= nil then
            table.insert(parts, {
                fieldno = v.fieldno - 1, type = field_types[v.type]
            })
        else
            error('Unknow field type: ' .. v.type)
        end
    end
    local merger = driver.merge_new(parts)
    ffi.gc(merger, driver.merge_del)
    return {
        start = function (sources, order)
            return driver.merge_start(merger, sources, order)
        end,
        cmp = function (key)
            return driver.merge_cmp(merger, key)
        end,
        next = function ()
            return driver.merge_next(merger)
        end
    }
end

local replica_sets = {}
local replica_sets_n = 0

local RECONNECT_AFTER = nil

local shard_obj

-- helpers
local function is_connected()
    for _, replica_set in ipairs(replica_sets) do
        for _, server in ipairs(replica_set) do
            if not server.conn or not server.conn:is_connected() then
                return false
            end
        end
    end
    return true
end

local function wait_connection()
    while not is_connected() do
        fiber.sleep(0.01)
    end
end

local maintenance = {}

local function shard_key_num(key)
    -- compability cases
    if type(key) == 'number' and math.floor(key) == key then
        return key
    end
    if type(key) == 'table' and #key == 1 and
       type(key[1]) == 'number' and math.floor(key[1]) == key[1] then
        return key[1]
    end
    if type(key) ~= 'table' then
        return digest.crc32(tostring(key))
    end
    -- eval key partitions crc32
    local crc = digest.crc32.new()
    for i, part in pairs(key) do
        crc:update(tostring(part))
    end
    return crc:result()
end

local function shard_function(key)
    return 1 + digest.guava(shard_key_num(key), replica_sets_n)
end

-- Get a first active server from a replica set.
local function server_by_key(key)
    local replica_set = replica_sets[shard_function(key)]
    if replica_set == nil then
        return nil
    end
    for _, server in ipairs(replica_set) do
        if not maintenance[server.id] then
            assert(server.conn:is_connected())
            return server
        end
    end
    return nil
end

-- For API details see net_box.call().
local function call(key, function_name, args, opts)
    local server = server_by_key(key)
    if server == nil then
        return nil
    end
    return server.conn:call(function_name, args, opts)
end

local function shard_status()
    local result = {
        online = {},
        offline = {},
        maintenance = maintenance
    }
    for _, replica_set in ipairs(replica_sets) do
         for _, server in ipairs(replica_set) do
             local s = { uri = server.uri, id = server.id }
             if server.conn:is_connected() then
                 table.insert(result.online, s)
             else
                 table.insert(result.offline, s)
             end
         end
    end
    return result
end

local function wait_server_is_connected(conn)
    local try = 1
    while try < 20 do
        if conn:ping() then
            return true
        end
        try = try + 1
        fiber.sleep(0.01)
    end
    return false
end

local function server_connect(server)
    if server.conn:is_connected() then
        maintenance[server.id] = nil
        return true
    end
    local uri = server.login .. ':' .. server.password .. '@' .. server.uri

    -- try to join replica
    server.conn = remote:connect(uri, { reconnect_after = RECONNECT_AFTER })
    -- ping node
    local joined = wait_server_is_connected(server.conn)

    local msg = nil
    if joined then
        msg = 'Succesfully joined shard %d with url "%s"'
    else
        msg = 'Failed to join shard %d with url "%s"'
    end
    log.info(msg, server.id, server.uri)
    -- remove from maintenance table
    maintenance[server.id] = nil
    return joined
end

-- join node by id in this shard
local function join_server(id)
    for _, replica_set in ipairs(replica_sets) do
         for _, server in ipairs(replica_set) do
             if server.id == id then
                 -- try to join replica
                 return server_connect(server)
             end
         end
    end
    return false
end

local function unjoin_server(id)
    -- In maintenance mode shard is available
    -- but client will recive erorrs using this shard
    maintenance[id] = true
    return true
end

local function cluster_operation(func_name, id)
    local jlog = {}
    local all_ok = true

    for _, replica_set in ipairs(replica_sets) do
         for _, server in ipairs(replica_set) do
            if server.id ~= id then
                local result, err = pcall(function()
                    log.info("Trying to '%s' shard %d with shard %s",
                             func_name, server.id, server.uri)
                    log.info({id = id})
                    local conn = server.conn
                    return conn:call(func_name, {id},
                                     {timeout = request_timeout})
                end)
                if not result then
                    local msg = string.format('"%s" error: %s', func_name,
                                              tostring(err))
                    table.insert(jlog, msg)
                    log.error(msg)
                    all_ok = false
                else
                    local msg = string.format("Operaion '%s' for shard %d in "..
                                              "node '%s' applied", func_name,
                                              server.id, server.uri)
                    table.insert(jlog, msg)
                    log.info(msg)
                end
            end
        end
    end
    return all_ok, jlog
end

-- join node by id in cluster:
-- 1. Create new replica and wait lsn
-- 2. Join storage cluster
-- 3. Join front cluster
local function remote_join(id)
    return cluster_operation("join_server", id)
end

local function remote_unjoin(id)
    return cluster_operation("unjoin_server", id)
end

local function get_merger(space_obj, index_id)
    if merger[space_obj.name] == nil then
        merger[space_obj.name] = {}
    end
    if merger[space_obj.name][index_id] == nil then
        local index = space_obj.index[index_id]
        merger[space_obj.name][index_id] = merge_new(index.parts)
    end
    return merger[space_obj.name][index_id]
end

local function server_request(server, operation, netbox_opts, ...)
    local c = server.conn
    local ok, ret = pcall(c._request, c, operation, netbox_opts, ...)
    if not ok then
        error(string.format('failed to execute operation on %s: %s',
                            server.uri, ret))
    end
    return ret
end

-- shards request function
local function request(tuple_id, operation, netbox_opts, ...)
    local server = server_by_key(tuple_id)
    if server == nil then
        error('A requested shard is down')
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
    for _, replica_set in ipairs(replica_sets) do
        local server = nil
        for _, srv in ipairs(replica_set) do
            if not maintenance[srv.id] then
                server = srv
                assert(srv.conn:is_connected())
                break
            end
        end
        if server ~= nil then
            netbox_opts.buffer = buffer.ibuf()
            if merge_obj == nil then
                merge_obj = get_merger(server.conn.space[space_id], index_id)
            end
            -- A response is stored in buf.
            server_request(server, 'select', netbox_opts, space_id, index_id,
                           iterator, offset, limit, key)
            table.insert(results, netbox_opts.buffer)
            netbox_opts.buffer = nil
        end
    end
    merge_obj.start(results, iterator_direction[iterator])
    local tuples = {}
    local i = 0
    while i < limit do
        table.insert(tuples, merge_obj.next())
        i = i + 1
    end
    return tuples
end

local function next_id(space)
    local server_id = self_server.id
    local s = box.space[space]
    if s == nil then
        box.error(box.error.NO_SUCH_SPACE, tostring(space))
    end
    if s.index[0].parts[1].type == 'string' then
        return uuid.str()
    end
    local key = s.name .. '_max_id'
    local _schema = box.space._schema
    local tuple = _schema:get{key}
    local next_id
    if tuple == nil then
        tuple = box.space[space].index[0]:max()
        if tuple == nil then
            next_id = server_id
        else
            next_id = math.floor((tuple[1] + 2 * replica_sets_n + 1) / replica_sets_n)
            next_id = next_id * replica_sets_n + server_id
        end
        _schema:insert{key, next_id}
    else
        next_id = tuple[2] + replica_sets_n
        tuple = _schema:update({key}, {{'=', 2, next_id}})
    end
    return next_id
end

local key_extract = {}

local function extract_key(space_name, data)
    if key_extract[space_name] then
        return key_extract[space_name](data)
    end
    local parts = {}
    local pk = box.space[space_name].index[0]
    for _, part in pairs(pk.parts) do
        parts[#parts + 1] = part.fieldno
    end
    key_extract[space_name] = function(tuple)
        local key = {}
        for _, i in pairs(parts) do
            key[#key + 1] = tuple[i]
        end
        return key
    end
    return key_extract[space_name](data)
end

local function shard_mapping(servers)
    -- iterate over all zones, and build shards, aka replica set
    -- each replica set has 'redundancy' servers from different
    -- zones
    local server_id = 1
    replica_sets_n = 0
    local replica_set_to_i = {}
    for _, server in ipairs(servers) do
        local replica_set_i = replica_set_to_i[server.replica_set]
        if replica_set_i == nil then
            replica_sets_n = replica_sets_n + 1
            replica_set_i = replica_sets_n
            replica_set_to_i[server.replica_set] = replica_set_i
            replica_sets[replica_set_i] = {}
        end
        log.info('Adding %s to replica set %d', server.uri, replica_set_i)
        server.id = server_id
        server_id = server_id + 1
        table.insert(replica_sets[replica_set_i], server)
    end
    log.info("shard count = %d", replica_sets_n)
end

local space_router_methods = {}
function space_router_methods:insert(tuple, netbox_opts)
    box.internal.check_space_arg(self, 'insert')
    local tuple_id = extract_key(self.name, tuple)
    return request(tuple_id, 'insert', netbox_opts, self.id, tuple)[1]
end

function space_router_methods:replace(tuple, netbox_opts)
    box.internal.check_space_arg(self, 'replace')
    local tuple_id = extract_key(self.name, tuple)
    return request(tuple_id, 'replace', netbox_opts, self.id, tuple)[1]
end

function space_router_methods:auto_increment(tuple, netbox_opts)
    box.internal.check_space_arg(self, 'auto_increment')
    local id = next_id(self.name)
    table.insert(tuple, 1, id)
    return request(id, 'insert', netbox_opts, self.id, tuple)[1]
end

function space_router_methods:select(key, select_opts, netbox_opts)
    box.internal.check_space_arg(self, 'select')
    return self.index[0]:select(key, select_opts, netbox_opts)
end

function space_router_methods:delete(key, netbox_opts)
    box.internal.check_space_arg(self, 'delete')
    local ret = request(key, 'delete', netbox_opts, self.id, 0, key)
    if ret ~= nil then
        return ret[1]
    else
        return nil
    end
end

function space_router_methods:update(key, oplist, netbox_opts)
    box.internal.check_space_arg(self, 'update')
    return request(key, 'update', netbox_opts, self.id, 0, key, oplist)[1]
end

function space_router_methods:upsert(tuple, oplist, netbox_opts)
    box.internal.check_space_arg(self, 'upsert')
    local tuple_id = extract_key(self.name, tuple)
    request(tuple_id, 'upsert', netbox_opts, self.id, tuple, oplist)
end

function space_router_methods:get(key, netbox_opts)
    box.internal.check_space_arg(self, 'get')
    local ret = request(key, 'select', netbox_opts, self.id, 0, box.index.EQ,
                        0, 2, key)
    if ret[2] ~= nil then box.error(box.error.MORE_THAN_ONE_TUPLE) end
    return ret[1]
end

local index_router_methods = {}
function index_router_methods:select(key, select_opts, netbox_opts)
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

local function on_server_connected(conn)
    join_server(conn.id)
end

local function on_server_disconnected(conn)
    epoch_counter = epoch_counter + 1
    unjoin_server(conn.id)
end

-- init shard, connect with servers
local function init(cfg, callback)
    cfg = config_util.check_cfg(cfg)
    request_timeout = cfg.request_timeout
    log.info('Sharding initialization started...')

    shard_mapping(cfg.servers)
    local min_redundancy = 999999999
    for _, replica_set in ipairs(replica_sets) do
        if min_redundancy > #replica_set then
            min_redundancy = #replica_set
        end
    end
    if min_redundancy < cfg.redundancy then
        local msg = string.format('Minimal redundancy found %s, but specified %s',
                                  min_redundancy, cfg.redundancy)
        log.error(msg)
        error(msg)
    end
    log.info("redundancy = %d", min_redundancy)
    self_server = config_util.connect(replica_sets, cfg, on_server_connected,
                                      on_server_disconnected)

    wait_connection()
    space_routers = config_util.build_schema(replica_sets, cfg)
    for space_name, space in pairs(space_routers) do
        if type(space_name) == 'string' then
            setmetatable(space, { __index = space_router_methods })
            for idx_name, index in pairs(space.index) do
                if type(idx_name) == 'string' then
                    setmetatable(index, { __index = index_router_methods })
                end
            end
        end
    end
    shard_obj.space = space_routers
    log.info('Done')
    return true
end

local function len(self)
    return self.replica_sets_n
end

local function get_epoch()
    return epoch_counter
end

local function wait_epoch(epoch)
    while epoch_counter < epoch do
        fiber.sleep(0.01)
    end
end

-- declare global functions
_G.join_server       = join_server
_G.unjoin_server     = unjoin_server
_G.remote_join       = remote_join
_G.remote_unjoin     = remote_unjoin

_G.cluster_operation = cluster_operation

shard_obj = {
    RECONNECT_AFTER = RECONNECT_AFTER,

    replica_sets = replica_sets,
    len = len,
    is_connected = is_connected,
    wait_connection = wait_connection,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    init = init,
    server_by_key = server_by_key,
    call = call,
    shard_function = shard_function,
    status = shard_status,
}

return shard_obj
