local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local remote = require('net.box')
local uuid = require('uuid')
local lib_pool = require('shard.connpool')
local ffi = require('ffi')
local buffer = require('buffer')
local mpffi = require'msgpackffi'

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

local merger = {}
local function merge_new(key_parts)
    local parts = {}
    local part_no = 1
    for _, v in pairs(key_parts) do
        if v.fieldno <= 0 then
            error('Invalid field number')
        end
        if field_types[v.type] ~= nil then
            parts[part_no] = {
                fieldno = v.fieldno - 1,
                type = field_types[v.type]
            }
            part_no = part_no + 1
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

local function key_create(data)
    return mpffi.encode(data)
end

local redundancy = 3
local replica_sets = {}
local replica_sets_n = 0

local REMOTE_TIMEOUT = 210
local HEARTBEAT_TIMEOUT = 500
local DEAD_TIMEOUT = 10
local RECONNECT_AFTER = nil

local pool = lib_pool.new()
local shard_obj

-- 1.6 and 1.7 netbox compat
local compat = string.sub(require('tarantool').version, 1,3)
local nb_call = 'call'
if compat ~= '1.6' then
    nb_call = 'call_16'
end

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
        if not maintenance[server.id] and pool:server_is_ok(server) then
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
    server.conn = remote:new(uri, { reconnect_after = RECONNECT_AFTER })
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

-- default on join/unjoin event
local function on_action(self, msg)
    log.info(msg)
end

local function cluster_operation(func_name, id)
    local jlog = {}
    local all_ok = true

    for _, replica_set in ipairs(replica_sets) do
         for _, server in ipairs(replica_set) do
             if server.id ~= id then
                 local result, err = pcall(function()
                     log.info(
                         "Trying to '%s' shard %d with shard %s",
                         func_name, server.id, server.uri
                     )
                     local conn = server.conn
                     return conn[nb_call](conn, func_name, id)[1][1]
                 end)
                 if not result then
                     local msg = string.format(
                        '"%s" error: %s',
                        func_name, tostring(err)
                     )
                     table.insert(jlog, msg)
                     log.error(msg)
                     all_ok = false
                 else
                     local msg = string.format(
                         "Operaion '%s' for shard %d in node '%s' applied",
                         func_name, server.id, server.uri
                     )
                     table.insert(jlog, msg)
                     shard_obj:on_action(msg)
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

-- base remote operation on space
local function space_call(self, space, server, fun, ...)
    local result = nil
    local status, reason = pcall(function(...)
        local conn = server.conn
        local space_obj = conn.space[space]
        if space_obj == nil then
            conn:reload_schema()
            space_obj = conn.space[space]
        end
        result = fun(space_obj, ...)
    end, ...)
    if not status then
        local err = string.format(
            'failed to execute operation on %s: %s',
            server.uri, reason
        )
        error(err)
    end
    return result
end

-- primary index wrapper on space_call
local function single_call(self, space, server, operation, ...)
    return self:space_call(space, server, function(space_obj, ...)
        return space_obj[operation](space_obj, ...)
    end, ...)
end

local function index_call(self, space, server, operation,
                          index_no, ...)
    return self:space_call(space, server, function(space_obj, ...)
        local index = space_obj.index[index_no]
        return index[operation](index, ...)
    end, ...)
end

local function merge_sort(results, index_fields, limits, cut_index)
    local result = {}
    if cut_index == nil then
        cut_index = {}
    end
    -- sort by all key parts
    table.sort(results, function(a ,b)
        for i, part in pairs(index_fields) do
            if a[part.fieldno] ~= b[part.fieldno] then
                return a[part.fieldno] > b[part.fieldno]
            end
        end
        return false
    end)
    for i, tuple in pairs(results) do
        local insert = true
        for j, elem in pairs(cut_index) do
            local part = index_fields[j]
            insert = insert and tuple[part.fieldno] == elem
        end
        if insert then
            table.insert(result, tuple)
        end

        -- check limit condition
        if #result >= limits.limit then
            break
        end
    end
    return result
end

local function get_merger(space_obj, index_no)
    if merger[space_obj.name] == nil then
        merger[space_obj.name] = {}
    end
    if merger[space_obj.name][index_no] == nil then
        local index = space_obj.index[index_no]
        merger[space_obj.name][index_no] = merge_new(index.parts)
    end
    return merger[space_obj.name][index_no]
end

local function mr_select(self, space, index_no, index, limits, key)
    local results = {}
    local merge_obj = nil
    if limits == nil then
        limits = {}
    end
    if limits.offset == nil then
        limits.offset = 0
    end
    if limits.limit == nil then
        limits.limit = 1000
    end
    for _, replica_set in ipairs(replica_sets) do
        local server = nil
        for _, srv in ipairs(replica_set) do
            if not maintenance[srv.id] and pool:server_is_ok(srv) then
                server = srv
                break
            end
        end
        if server ~= nil then
            local buf = buffer.ibuf()
            limits.buffer = buf
            if merge_obj == nil then
                merge_obj = get_merger(server.conn.space[space], index_no)
            end
            local part = index_call(
                self, space, server, 'select',
                index_no, index, limits
            )
            table.insert(results, buf)
        end
    end
    merge_obj.start(results, -1)
    local tuples = {}
    local cmp_key = key_create(key or index)
    while merge_obj.cmp(cmp_key) == 0 do
        local tuple = merge_obj.next()
        table.insert(tuples, tuple)
        if #tuples >= limits.limit then
            break
        end
    end
    return tuples
end

local function secondary_select(self, space, index_no, index, limits, key)
    return mr_select(self, space, index_no, index, limits, key)
end

local function direct_call(self, server, func_name, ...)
    local result = nil
    local status, reason = pcall(function(...)
        local conn = server.conn
        result = conn[nb_call](conn, func_name, ..., {timeout = REMOTE_TIMEOUT})
    end, ...)
    if not status then
        log.error('failed to call %s on %s: %s', func_name, server.uri, reason)
        if not server.conn:is_connected() then
            log.error("server %s is offline", server.uri)
        end
    end
    return result
end

-- shards request function
local function request(self, space, operation, tuple_id, ...)
    local server = server_by_key(tuple_id)
    if server == nil then
        return nil
    end
    return single_call(self, space, server, operation, ...)
end

local function next_id(space)
    local server_id = pool.self_server.id
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

-- default request wrappers for db operations
local function insert(self, space, data)
    local tuple_id = extract_key(space, data)
    return request(self, space, 'insert', tuple_id, data)
end

local function auto_increment(self, space, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    return request(self, space, 'insert', {id}, data)
end

local function select(self, space, key, args)
    return request(self, space, 'select', key, key, args)
end

local function replace(self, space, data)
    local tuple_id = extract_key(space, data)
    return request(self, space, 'replace', tuple_id, data)
end

local function delete(self, space, key)
    return request(self, space, 'delete', key, key)
end

local function update(self, space, key, data)
    return request(self, space, 'update', key, key, data)
end

-- function to check a connection after it's established
local function check_connection(conn)
    return true
end

local function is_table_filled()
    return pool:is_table_filled()
end

local function wait_table_fill()
    return pool:wait_table_fill()
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
        table.insert(replica_sets[replica_set_i], server)
    end
    log.info("shard count = %d", replica_sets_n)
end

local function get_heartbeat()
    return pool:get_heartbeat()
end

local function enable_operations()
    -- set base operations
    shard_obj.single_call = single_call
    shard_obj.space_call = space_call
    shard_obj.index_call = index_call
    shard_obj.secondary_select = secondary_select
    shard_obj.mr_select = mr_select
    shard_obj.direct_call = direct_call
    shard_obj.request = request

    -- set 1-phase operations
    shard_obj.insert = insert
    shard_obj.auto_increment = auto_increment
    shard_obj.select = select
    shard_obj.replace = replace
    shard_obj.update = update
    shard_obj.delete = delete

    -- set helpers
    shard_obj.on_action = on_action
    shard_obj.get_heartbeat = get_heartbeat

    -- enable easy spaces access
    setmetatable(shard_obj, {
        __index = function (self, space)
            return {
                name = space,
                auto_increment = function(this, ...)
                    return self.auto_increment(self, space, ...)
                end,
                insert = function(this, ...)
                    return self.insert(self, space, ...)
                end,
                select = function(this, ...)
                    return self.select(self, space, ...)
                end,
                replace = function(this, ...)
                    return self.replace(self, space, ...)
                end,
                delete = function(this, ...)
                    return self.delete(self, space, ...)
                end,
                update = function(this, ...)
                    return self.update(self, space, ...)
                end,

                                single_call = function(this, ...)
                    return self.single_call(self, space, ...)
                end,
                space_call = function(this, ...)
                    return self.space_call(self, space, ...)
                end,
                index_call = function(this, ...)
                    return self.index_call(self, space, ...)
                end,
                secondary_select = function(this, ...)
                    return self.secondary_select(self, space, ...)
                end,
                mr_select = function(this, ...)
                    return self.mr_select(self, space, ...)
                end
            }
        end
    })
end

local function check_cfg(template, default, cfg)
    cfg = table.deepcopy(cfg)
    -- Set default options.
    for k, v in pairs(default) do
        if cfg[k] == nil then
            cfg[k] = default[k]
        end
    end
    -- Check specified options.
    for k, value in pairs(cfg) do
        if template[k] == nil then
            error("Unknown cfg option "..k)
        end
        if type(template[k]) == 'function' then
            template[k](value)
        elseif template[k] ~= type(value) then
            error("Incorrect type of cfg option "..k..": expected "..
                  template[k])
        end
    end
    return cfg
end

local cfg_server_template = {
    uri = 'string',
    replica_set = 'string',
}

local cfg_template = {
    servers = function(value)
        if type(value) ~= 'table' then
            error('Option "servers" must be table')
        end
        for _, server in ipairs(value) do
            if type(server) ~= 'table' then
                error('Each server must be table')
            end
            check_cfg(cfg_server_template, {}, server)
        end
    end,
    login = 'string',
    password = 'string',
    monitor = 'boolean',
    pool_name = 'string',
    redundancy = 'number',
    rsd_max_rps = 'number',
    binary = 'number'
}

local cfg_default = {
    servers = {},
    monitor = true,
    pool_name = 'sharding_pool',
    redundancy = 2,
    rsd_max_rps = 1000,
}

-- init shard, connect with servers
local function init(cfg, callback)
    cfg = check_cfg(cfg_template, cfg_default, cfg)
    log.info('Sharding initialization started...')
    -- set constants
    pool.REMOTE_TIMEOUT = shard_obj.REMOTE_TIMEOUT
    pool.HEARTBEAT_TIMEOUT = shard_obj.HEARTBEAT_TIMEOUT
    pool.DEAD_TIMEOUT = shard_obj.DEAD_TIMEOUT
    pool.RECONNECT_AFTER = shard_obj.RECONNECT_AFTER

    --init connection pool
    pool:init(cfg)
    shard_mapping(pool.servers)
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
    redundancy = min_redundancy
    log.info("redundancy = %d", redundancy)

    -- servers mappng

    enable_operations()
    log.info('Done')
    return true
end

local function len(self)
    return self.replica_sets_n
end

local function get_epoch()
    return pool:get_epoch()
end

local function wait_epoch(epoch)
    return pool:wait_epoch(epoch)
end

-- declare global functions
_G.join_server       = join_server
_G.unjoin_server     = unjoin_server
_G.remote_join       = remote_join
_G.remote_unjoin     = remote_unjoin

_G.cluster_operation = cluster_operation
_G.merge_sort        = merge_sort
_G.shard_status      = shard_status

shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,

    replica_sets = replica_sets,
    len = len,
    redundancy = redundancy,
    is_connected = is_connected,
    wait_connection = wait_connection,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,
    init = init,
    server_by_key = server_by_key,
    pool = pool,
    call = call,
    shard_function = shard_function,
}

return shard_obj
