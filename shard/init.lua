local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local remote = require('net.box')
local config_util = require('shard.config_util')
local dml_module = require('shard.dml')

local epoch_counter = nil
local replica_sets = {}
local shard_obj = {}
local cfg = nil

local replica_set_resurrection_fiber = nil
local resurrect_replica_set_cond = nil
local master_is_found_cond = nil

local errinj = {}

------------------------------ Utils -----------------------------

-- Remove all keys from a lua table. Useful when there is a
-- reference to this table in another module and to keep it the
-- table must not be recreated using t = {} syntax.
local function clear_table(t)
    local to_delete = {}
    for k in pairs(t) do
        table.insert(to_delete, k)
    end
    for _, k in ipairs(to_delete) do
        t[k] = nil
    end
end

local function get_epoch()
    return epoch_counter
end

-- Epoch counter is incremented on each node down.
local function wait_epoch(epoch)
    while epoch_counter < epoch do
        fiber.sleep(0.01)
    end
end

local function shard_key_num(key)
    if type(key) == 'number' and math.floor(key) == key then
        return key
    end
    if type(key) == 'table' and #key == 1 and type(key[1]) == 'number'
       and math.floor(key[1]) == key[1] then
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
    return 1 + digest.guava(shard_key_num(key), #replica_sets)
end

-- Get a master of a replica set by calculating shard function
-- of a given key.
local function server_by_key(key)
    local replica_set = replica_sets[shard_function(key)]
    if replica_set ~= nil and replica_set.master ~= nil then
        assert(replica_set.master.conn:is_connected())
        return replica_set.master
    end
    return nil
end

-- Close connection of a server with no triggers running. There is
-- no neccessarity to run triggers, because kill_connection() is
-- called in a case of already dead connection. Its task is to
-- remove server.conn and destroy connection object.
local function kill_connection(server)
    local conn = server.conn
    server.conn = nil
    assert(conn ~= nil)
    -- Delete a trigger before closing the connection.
    local t = conn:on_disconnect()
    if #t == 1 then
        conn:on_disconnect(nil, t[1])
    end
    pcall(conn.close, conn)
end

-- Call a function on a replica set by its key. For API details
-- see net_box.call().
local function call(key, function_name, args, opts)
    local server = server_by_key(key)
    if server == nil then
        return nil
    end
    return server.conn:call(function_name, args, opts)
end

-- Collect info about replica sets. For each replica set its
-- master and slaves are collected.
local function shard_status()
    local result = {}
    for _, replica_set in ipairs(replica_sets) do
        local stat = {}
        local master = replica_set.master
        if master ~= nil then
            assert(master.conn:is_connected())
            stat.master = master.uri
        end
        for _, server in ipairs(replica_set) do
            if server.conn ~= nil then
                assert(server == master)
            else
                if stat.slaves == nil then
                    stat.slaves = {}
                end
                table.insert(stat.slaves, server.uri)
            end
        end
        table.insert(result, stat)
    end
    return result
end

local function find_server_by_conn(conn)
    for _, replica_set in ipairs(replica_sets) do
        for _, server in ipairs(replica_set) do
            if server.conn == conn then
                return server, replica_set
            end
        end
    end
    return nil
end

------------------ Replica set master management -----------------
-- Forward declaration.
local find_master

-- Trigger on master disconnection. On_disconnect is called only
-- for masters, because only masters have connections.
local function on_disconnect(conn)
    assert(conn ~= nil)
    epoch_counter = epoch_counter + 1
    local server, replica_set = find_server_by_conn(conn)
    log.info('A server %s disconnected', server.uri)
    assert(server ~= nil)
    assert(server.conn == conn)
    kill_connection(server)
    assert(server == replica_set.master)
    replica_set.master = nil
    find_master(replica_set, true)
end

-- Periodically check replica sets without masters and initiate
-- master search for them.
local function replica_set_resurrection_f()
    while true do
        resurrect_replica_set_cond:wait()
        while errinj.delay_replica_set_resurrection do
            fiber.sleep(0.1)
        end
::restart_resurrection::
        local failed = false
        for _, replica_set in ipairs(replica_sets) do
            if replica_set.master == nil and
               find_master(replica_set, true) == nil then
                failed = true
            end
        end
        if failed then
            fiber.sleep(0.5)
            goto restart_resurrection
        end
    end
end

find_master = function(replica_set, need_set_triggers)
    log.info('New master search has started for replica set %d',
             replica_set.id)
    assert(replica_set.master == nil)
    for _, server in ipairs(replica_set) do
        server.conn = remote:connect(server.access_uri, {
            wait_connected = cfg.sharding_timeout
        })
        if server.conn:is_connected() then
            replica_set.master = server
            if need_set_triggers then
                server.conn:on_disconnect(on_disconnect)
            end
            log.info('%s is master for replica set %d', server.uri,
                     replica_set.id)
            master_is_found_cond:signal()
            return server
        else
            kill_connection(server)
        end
    end
    -- Can not find a master now. Lets do it in background.
    resurrect_replica_set_cond:signal()
    return nil
end

------------------------- Initialization -------------------------

-- Wait until all replica sets have masters.
local function wait_connection()
    local all_are_connected
    repeat
        all_are_connected = true
        for _, replica_set in ipairs(replica_sets) do
            if replica_set.master == nil then
                all_are_connected = false
                master_is_found_cond:wait()
                break
            end
        end
    until all_are_connected
end

local function initial_connect()
::find_masters::
    for _, replica_set in ipairs(replica_sets) do
        if replica_set.master == nil then
            find_master(replica_set)
        end
    end

    -- Try delay between masters search. A master can fail during
    -- search of masters for other replica sets. In such a case
    -- the search must be restarted below.
    while errinj.delay_after_initial_master_search do
        fiber.sleep(0.1)
    end

    -- Check, if during connections establishment some of them
    -- did disconnect.
    local is_search_failed = false
    for _, replica_set in ipairs(replica_sets) do
        local master = replica_set.master
        -- Do not finish the cycle here. At first, all
        -- disconnected masters must be killed.
        if master == nil then
            is_search_failed = true
        elseif not master.conn:is_connected() then
            is_search_failed = true
            kill_connection(master)
            replica_set.master = nil
        end
    end
    if is_search_failed then
        goto find_masters
    end
    -- All found masters are ok, set triggers on them.
    for _, replica_set in ipairs(replica_sets) do
        assert(replica_set.master ~= nil)
        replica_set.master.conn:on_disconnect(on_disconnect)
    end
end

-- Forward declaration.
local reset

local function init(config)
    -- Reset if a cfg() is called not a first time.
    reset()
    log.info('Sharding initialization started...')
    cfg = config_util.build_config(config)
    replica_sets = config_util.build_replica_sets(cfg)

    initial_connect()
    replica_set_resurrection_fiber = fiber.create(replica_set_resurrection_f)

    -- Build schema representaion in space objects.
    config_util.check_schema(replica_sets, cfg)
    shard_obj.space = config_util.build_schema(replica_sets, cfg)
    shard_obj.replica_sets = replica_sets
    shard_obj.wait_connection = wait_connection
    shard_obj.get_epoch = get_epoch
    shard_obj.wait_epoch = wait_epoch
    shard_obj.server_by_key = server_by_key
    shard_obj.call = call
    shard_obj.shard_function = shard_function
    shard_obj.status = shard_status
    -- Init space and index metatables.
    dml_module.init_schema_methods(shard_obj)
    log.info('Done')
end

-- Close all connections, drop internal variables to their
-- default values. Useful to restart shard module, especially in
-- tests.
reset = function()
    if replica_set_resurrection_fiber ~= nil then
        replica_set_resurrection_fiber:cancel()
    end
    replica_set_resurrection_fiber = nil
    resurrect_replica_set_cond = fiber.cond()
    master_is_found_cond = fiber.cond()
    epoch_counter = 1
    for _, replica_set in ipairs(replica_sets) do
        if replica_set.master ~= nil then
            kill_connection(replica_set.master)
            replica_set.master = nil
        end
    end
    errinj.delay_after_initial_master_search = false
    errinj.delay_replica_set_resurrection = false
    replica_sets = {}
    cfg = nil
    -- Do not create a new shard obj, becase the exising shard obj
    -- must be invalidated.
    clear_table(shard_obj)
    shard_obj.cfg = init
    shard_obj.errinj = errinj
end

reset()

return shard_obj
