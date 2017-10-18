local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')
local uuid = require('uuid')
local json = require('json')
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
    array     = 3,
    number    = 4,
    integer   = 5,
    scala     = 5
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

local shards = {}
local shards_n

local redundancy = 3

local REMOTE_TIMEOUT = 210
local HEARTBEAT_TIMEOUT = 500
local DEAD_TIMEOUT = 10
local RECONNECT_AFTER = msgpack.NULL

local pool = lib_pool.new()
local STATE_NEW = 0
local STATE_INPROGRESS = 1
local STATE_HANDLED = 2

local init_complete = false
local configuration = {}
local shard_obj

-- 1.6 and 1.7 netbox compat
local compat = string.sub(require('tarantool').version, 1,3)
local nb_call = 'call'
if compat ~= '1.6' then
    nb_call = 'call_16'
end

-- helpers
local function is_connected()
    for j = 1, #shards do
        local srd = shards[j]
        for i = 1, redundancy do
            if not srd[i].conn or not srd[i].conn:is_connected() then
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

-- main shards search function
local function shard(key)
    local num = type(key) == 'number' and key or digest.crc32(key)
    local max_shards = shards_n
    local shard = shards[1 + digest.guava(num, max_shards)]
    local res = {}
    local k = 1

    if shard == nil then
        return nil
    end
    for i = 1, redundancy do
        local srv = shard[redundancy - i + 1]
        -- GH-72
        -- we need to save shards state during maintenance
        -- client will recive error "shard is unavailable"
        if not maintenance[srv.id] and pool:server_is_ok(srv) then
            res[k] = srv
            k = k + 1
        end
    end
    return res
end

local function shard_status()
    local result = {
        online = {},
        offline = {},
        maintenance = maintenance
    }
    for j = 1, #shards do
         local srd = shards[j]
         for i = 1, redundancy do
             local s = { uri = srd[i].uri, id = srd[i].id }
             if srd[i].conn:is_connected() then
                 table.insert(result.online, s)
             else
                 table.insert(result.offline, s)
             end
         end
    end
    return result
end

local function get_zones()
    local result = {}
    for z_name, zone in pairs(shards) do
        result[z_name] = {}
        for _, server in pairs(zone) do
            table.insert(result[z_name], server.uri)
        end
    end
    return result
end

local function is_shard_connected(uri, conn)
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

local function shard_connect(s_obj)
    if s_obj.conn:is_connected() then
        maintenance[s_obj.id] = nil
        return true
    end
    local msg = 'Failed to join shard %d with url "%s"'
    local uri = s_obj.login .. ':' .. s_obj.password .. '@' .. s_obj.uri

    -- try to join replica
    s_obj.conn = remote:new(uri, { reconnect_after = RECONNECT_AFTER })
    -- ping node
    local joined = is_shard_connected(uri, s_obj.conn)

    if joined then
        msg = 'Succesfully joined shard %d with url "%s"'
    end
    log.info(msg, s_obj.id, s_obj.uri)
    -- remove from maintenance table
    maintenance[s_obj.id] = nil
    return joined
end

local function shard_swap(group_id, shard_id)
    -- save old shard
    local zone = shards[group_id]
    local ro_shard = zone[shard_id]

    -- find current working RW shard
    local i = 1
    while not pool:server_is_ok(zone[redundancy - i + 1]) do
        i = i + 1
        -- GH-72/73
        -- do not swap dead pairs (case #3 master and replica are
        -- offline)
        if zone[redundancy - i + 1] == nil then
            return
        end
    end
    local rw_shard = zone[redundancy - i + 1]
    log.info('RW: %s', rw_shard.uri)
    log.info('RO: %s', ro_shard.uri)

    -- swap old and new
    zone[redundancy - i + 1] = ro_shard
    zone[#zone] = rw_shard
end

-- join node by id in this shard
local function join_shard(id)
    for j = 1, #shards do
         local srd = shards[j]
         for i = 1, redundancy do
             if srd[i].id == id then
                 -- swap RO/RW shards
                 local to_join = srd[i]
                 to_join.ignore = true
                 shard_swap(j, i)
                 to_join.ignore = false
                 -- try to join replica
                 return shard_connect(to_join)
             end
         end
    end
    return false
end

local function unjoin_shard(id)
    -- In maintenance mode shard is available
    -- but client will recive erorrs using this shard
    maintenance[id] = true
    return true
end

-- default on join/unjoin event
local function on_action(self, msg)
    log.info(msg)
end

local function cluster_operation(func_name, ...)
    local jlog = {}
    local q_args = {...}
    local all_ok = true

    -- we need to check shard id for direct operations
    local id = nil
    if type(q_args[1]) == 'number' then
        id = q_args[1]
    end

    for j=1, #shards do
         local srd =shards[j]
         for i=1, redundancy do
             if srd[i].id ~= id then
                 local result = false
                 local ok, err = pcall(function()
                     log.info(
                         "Trying to '%s' shard %d with shard %s",
                         func_name, srd[i].id, srd[i].uri
                     )
                     local conn = srd[i].conn
                     local is_replica = i == 1
                     result = conn[nb_call](conn, func_name, unpack(q_args), is_replica)[1][1]
                 end)
                 if not ok or not result then
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
                         func_name, srd[i].id, srd[i].uri
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
    return cluster_operation("join_shard", id)
end

local function remote_unjoin(id)
    return cluster_operation("unjoin_shard", id)
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

local function mr_select(self, space, nodes, index_no,
                        index, limits, key)
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
    for _, node in pairs(nodes) do
        local j = #node
        local srd = node[j]
        local buf = buffer.ibuf()
        limits.buffer = buf
        if merge_obj == nil then
            merge_obj = get_merger(srd.conn.space[space], index_no)
        end
        local part = index_call(
            self, space, srd, 'select',
            index_no, index, limits
        )

        while part == nil and j >= 0 do
            j = j - 1
            srd = node[j]
            part = index_call(
                self, space, srd, 'select',
                index_no, index, limits
            )
        end
        table.insert(results, buf)
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

local function secondary_select(self, space, index_no,
                                index, limits, key)
    return mr_select(
        self, space, shards, index_no,
        index, limits, key
    )
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
    local result = {}
    local nodes = {}
    nodes = shard(tuple_id)

    for _, server in ipairs(nodes) do
        table.insert(result, single_call(self, space, server, operation, ...))
        if configuration.replication == true then
            break
        end
    end
    return result
end

local function next_id(space)
    local server_id = pool.self_server.id
    local s = box.space[space]
    if s == nil then
        box.error(box.error.NO_SUCH_SPACE, tostring(space))
    end
    if s.index[0].parts[1].type == 'STR' then
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
            next_id = math.floor((tuple[1] + 2 * shards_n + 1) / shards_n)
            next_id = next_id * shards_n + server_id
        end
        _schema:insert{key, next_id}
    else
        next_id = tuple[2] + pool:len()
        tuple = _schema:update({key}, {{'=', 2, next_id}})
    end
    return next_id
end

-- default request wrappers for db operations
local function insert(self, space, data)
    local tuple_id = data[1]
    return request(self, space, 'insert', tuple_id, data)
end

local function auto_increment(self, space, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    return request(self, space, 'insert', id, data)
end

local function select(self, space, key, args)
    return request(self, space, 'select', key[1], key, args)
end

local function replace(self, space, data)
    local tuple_id = data[1]
    return request(self, space, 'replace', tuple_id, data)
end

local function delete(self, space, key)
    return request(self, space, 'delete', key[1], key)
end

local function update(self, space, key, data)
    return request(self, space, 'update', key[1], key, data)
end

-- function for shard checking after init
local function check_shard(conn)
    return true
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

local function shard_mapping(zones)
    -- iterate over all zones, and build shards, aka replica set
    -- each replica set has 'redundancy' servers from different
    -- zones
    local server_id = 1
    shards_n = 1
    local prev_shards_n = 0
    while shards_n ~= prev_shards_n do
        prev_shards_n = shards_n
        for name, zone in pairs(zones) do
            if shards[shards_n] == nil then
                shards[shards_n] = {}
            end
            local shard = shards[shards_n]
            local non_arbiters = {}

            for _, candidate in ipairs(zone.list) do
                if not candidate.arbiter then
                    table.insert(non_arbiters, candidate)
                end
            end

            local srv = non_arbiters[server_id]

            -- we must double check that the same
            for _, v in pairs(shard) do
                if srv == nil then
                    break
                end
                if v.zone and v.zone.list == srv.zone then
                    log.error('not using server %s', srv.uri)
                    srv = nil
                end
            end
            if srv ~= nil then
                log.info("Adding %s to shard %d", srv.uri, shards_n)
                srv.zone_name = name
                table.insert(shards[shards_n], srv)
                if #shards[shards_n] == redundancy then
                    shards_n = shards_n + 1
                end
            end
        end
        server_id = server_id + 1
    end
    if shards[shards_n] == nil or #shards[shards_n] < redundancy then
        if shards[shards_n] ~= nil then
            for _, srv in pairs(shards[shards_n]) do
                log.error('not using server %s', srv.uri)
            end
        end
        shards[shards_n] = nil
        shards_n = shards_n - 1
    end
    log.info("shards = %d", shards_n)
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

-- init shard, connect with servers
local function init(cfg, callback)
    configuration = cfg
    log.info('Sharding initialization started...')
    -- set constants
    pool.REMOTE_TIMEOUT = shard_obj.REMOTE_TIMEOUT
    pool.HEARTBEAT_TIMEOUT = shard_obj.HEARTBEAT_TIMEOUT
    pool.DEAD_TIMEOUT = shard_obj.DEAD_TIMEOUT
    pool.RECONNECT_AFTER = shard_obj.RECONNECT_AFTER

    --init connection pool
    pool:init(cfg)

    redundancy = cfg.redundancy or pool:len()
    if redundancy > pool.zones_n then
        log.error("the number of zones (%d) must be greater than %d",
                  pool.zones_n, redundancy)
        error("the number of zones must be greater than redundancy")
    end
    log.info("redundancy = %d", redundancy)

    -- servers mappng
    shard_mapping(pool.servers)

    enable_operations()
    log.info('Done')
    init_complete = true
    return true
end

local function len(self)
    return self.shards_n
end

local function get_epoch()
    return pool:get_epoch()
end

local function wait_epoch(epoch)
    return pool:wait_epoch(epoch)
end

-- declare global functions
_G.join_shard        = join_shard
_G.unjoin_shard      = unjoin_shard
_G.remote_join       = remote_join
_G.remote_unjoin     = remote_unjoin

_G.cluster_operation = cluster_operation
_G.get_zones         = get_zones
_G.merge_sort        = merge_sort
_G.shard_status      = shard_status

shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,

    shards = shards,
    shards_n = shards_n,
    len = len,
    redundancy = redundancy,
    is_connected = is_connected,
    wait_connection = wait_connection,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,
    init = init,
    shard = shard,
    pool = pool,
    check_shard = check_shard,
}

return shard_obj
-- vim: ts=4:sw=4:sts=4:et
