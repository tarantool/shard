#!/usr/bin/env tarantool

local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')
local uuid = require('uuid')

local servers = {}
local servers_n
local redundancy = 3
local REMOTE_TIMEOUT = 500
local HEARTBEAT_TIMEOUT = 500
local DEAD_TIMEOUT = 10
local RECONNECT_AFTER = msgpack.NULL

local self_server

heartbeat_state = {}

local STATE_NEW = 0
local STATE_INPROGRESS = 1
local STATE_HANDLED = 2

local init_complete = false
local epoch_counter = 1
local configuration = {}


--queue implementation
local function queue_handler(self, fun)
    fiber.name('queue/handler')
    while true do
        local args = self.ch:get()
        if not args then
            break
        end
        local status, reason = pcall(fun, args)
        if not status then
            if not self.error then
                self.error = reason
                -- stop fiber queue
                self.ch:close()
            else
                self.error = self.error.."\n"..reason
            end
            break
        end
    end
    self.chj:put(true)
end

local queue_mt
local function queue(fun, workers)
    -- Start fiber queue to processes transactions in parallel
    local channel_size = math.min(workers, 1000)
    local ch = fiber.channel(workers)
    local chj = fiber.channel(workers)
    local self = setmetatable({ ch = ch, chj = chj, workers = workers }, queue_mt)
    for i=1,workers do
        fiber.create(queue_handler, self, fun)
    end
    return self
end

local function queue_join(self)
    log.debug("queue.join(%s)", self)
    -- stop fiber queue
    while self.ch:is_closed() ~= true and self.ch:is_empty() ~= true do
        fiber.sleep(0)
    end
    self.ch:close()
    -- wait until fibers stop

    for i = 1, self.workers do
        self.chj:get()
    end
    log.debug("queue.join(%s): done", self)
    if self.error then
        return error(self.error) -- re-throw error
    end
end

local function queue_put(self, arg)
    self.ch:put(arg)
end

queue_mt = {
    __index = {
        join = queue_join;
        put = queue_put;
    }
}

-- sharding heartbeat monitoring function
function heartbeat()
    log.debug('ping')
    return heartbeat_state
end

-- main shards search function
local function shard(key, dead)
    local num = digest.crc32(key)
    local shards = {}
    local k = 1
    for i=1,redundancy do
        local zone = servers[tonumber(1 + (num + i) % servers_n)]
        local server = zone[1 + digest.guava(num, #zone)]
        -- ignore died servers
        if not server.ignore and (server.conn:is_connected() or dead ~= nil) then
            shards[k] = server
            k = k + 1
        end
    end
    return shards
end

-- shard monitoring fiber
local function monitor_fiber()
    fiber.name("monitor")
    local i = 0
    while true do
        i = i + 1
        local server = shard(i, true)[1]
        local uri = server.uri
        local dead = false
        for k, v in pairs(heartbeat_state) do
            -- true only if there is stuff in heartbeat_state
            if k ~= uri then
                dead = true
                log.debug("monitoring: %s", uri)
                break
            end
        end
        for k, v in pairs(heartbeat_state) do
            -- kill only if DEAD_TIMEOUT become in all servers
            if k ~= uri and (v[uri] == nil or v[uri].try < DEAD_TIMEOUT) then
                log.debug("%s is alive", uri)
                dead = false
                break
            end
        end

        if dead then
            log.info("kill %s by dead timeout", uri)
            server.conn:close()
            server.ignore = true
            heartbeat_state[uri] = nil
            epoch_counter = epoch_counter + 1
        end
        fiber.sleep(math.random(100)/1000)
    end
end

local function is_ignored(uri)
    local result = false
    for _, zone in ipairs(servers) do
        for _, server in ipairs(zone) do
            if server.uri == uri and server.ignore then
                result = true
                break
            end
        end
    end
    return result
end

-- merge node response data with local table by fiber time
local function merge_tables(response)
    if response == nil then
        return
    end
    for seen_by_uri, node_table in pairs(response) do
        local node_data = heartbeat_state[seen_by_uri]
        if not is_ignored(seen_by_uri) then
            if not node_data then
                heartbeat_state[seen_by_uri] = node_table
                node_data = heartbeat_state[seen_by_uri]
                epoch_counter = epoch_counter + 1
            end
            for uri, data in pairs(node_table) do
                -- update if not data or if we have fresh time
                local empty_row = not node_data[uri] or not node_data[uri].ts
                if empty_row or data.ts > node_data[uri].ts then
                    log.debug('merged heartbeat from ' .. seen_by_uri .. ' with ' .. uri)
                    node_data[uri] = data
                end
            end
        end
    end
end

-- heartbeat table and opinions management
local function update_heartbeat(uri, response, status)
    -- set or update opinions and timestamp
    local opinion = heartbeat_state[self_server.uri]
    if not opinion[uri] then
        opinion[uri] = {
            try= 0,         -- number of errors
            ts=fiber.time() -- time of try
        }
    end
    if not status then
        opinion[uri].try = opinion[uri].try + 1
    else
        opinion[uri].try = 0
    end
    opinion[uri].ts = fiber.time()

    -- update local heartbeat table
    merge_tables(response)
end

-- heartbeat worker
local function heartbeat_fiber()
    fiber.name("heartbeat")
    heartbeat_state[self_server.uri] = {}
    local i = 0
    while true do
        i = i + 1
        -- random select node to check
        local server = shard(i, true)[1]
        local uri = server.uri
        log.debug("checking %s", uri)

        -- get heartbeat from node
        local response
        local status, err_state = pcall(function()
            response = server.conn:timeout(
                HEARTBEAT_TIMEOUT):eval("return heartbeat()")
        end)
        -- update local heartbeat table
        update_heartbeat(uri, response, status)
        log.debug("%s", yaml.encode(heartbeat_state))
        -- randomized wait for next check
        fiber.sleep(math.random(1000)/1000)
    end
end

-- base remote operation call

function deepcopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[deepcopy(orig_key)] = deepcopy(orig_value)
        end
        setmetatable(copy, deepcopy(getmetatable(orig)))
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

local function single_call(self, space, server, operation, ...)
    result = nil
    local status, reason = pcall(function(...)
        local args = deepcopy({...})
        self = server.conn:timeout(5 * REMOTE_TIMEOUT).space[space]
        result = self[operation](self, unpack(args))
    end, ...)
    if not status then
        log.error('failed to %s on %s: %s', operation, server.uri, reason)
        if not server.conn:is_connected() then
            log.error("server %s is offline", server.uri)
        end
    end
    return result
end

-- shards request function
local function request(self, space, operation, tuple_id, ...)
    local result = {}
    k = 0
    for i, server in ipairs(shard(tuple_id)) do
        result[k] = single_call(self, space, server, operation, ...)
        k = k + 1
    end
    return result
end

-- execute operation, call from remote node
function execute_operation(operation_id)
    log.debug('EXEC_OP')
    -- execute batch
    box.begin()
    local tuple = box.space.operations:update(
        operation_id, {{'=', 2, STATE_INPROGRESS}}
    )
    local batch = tuple[3]
    for _, operation in ipairs(batch) do    
        local space = operation[1]
        local func_name = operation[2]
        local args = operation[3]
        local self = box.space[space]
        self[func_name](self, unpack(args))
    end
    box.space.operations:update(operation_id, {{'=', 2, STATE_HANDLED}})
    box.commit()
end

-- process operation in queue
local function push_operation(task)
    local server = task.server
    local tuple = task.tuple
    log.debug('PUSH_OP')
    server.conn:timeout(REMOTE_TIMEOUT).space.operations:insert(tuple)
end

local function ack_operation(task)
    log.debug('ACK_OP')
    local server = task.server
    local operation_id = task.id
    server.conn:timeout(REMOTE_TIMEOUT):call(
        'execute_operation', operation_id
    )
end

local function push_queue(obj)
    for server, data in pairs(obj.batch) do
        local tuple = {
            obj.batch_operation_id;
            STATE_NEW;
            data;
        }
        local task = {tuple = tuple, server = server}
        obj.q:put(task)
    end
    obj.q:join()
    -- all shards ready - start workers
    obj.q = queue(ack_operation, redundancy)
    for server, _ in pairs(obj.batch) do
        obj.q:put({id=obj.batch_operation_id, server=server})
    end
    obj.batch = {}
    obj.batch_mode = false
end

local function q_end(self)
    -- check begin/end order
    if not self.batch_mode then
        log.error('Cannot run q_end without q_begin')
        return
    end
    push_queue(self)
end

-- fast request with queue
local function queue_request(self, space, operation, operation_id, tuple_id, ...)
    -- queued push operation in shards
    local batch
    local batch_mode = type(self.q_end) == 'function'
    if batch_mode then
        batch = self.batch
        self.batch_operation_id = tostring(operation_id)
    else
        batch = {}
    end

    for _, server in ipairs(shard(tuple_id)) do
        if batch[server] == nil then
            batch[server] = {}
        end
        local data = { box.space[space].id; operation; {...}; }
        batch[server][#batch[server] + 1] = data
    end
    if not batch_mode then
        local obj = {
            q = queue(push_operation, redundancy),
            batch_operation_id = tostring(operation_id),
            batch = batch,
        }
        push_queue(obj)
    end
end

function find_operation(id)
    log.debug('FIND_OP')
    return box.space.operations:get(id)[2]
end

local function check_operation(self, space, operation_id, tuple_id)
    local delay = 0.001
    operation_id = tostring(operation_id)
    for i=1,100 do
        local failed = nil
        local task_status = nil
        for _, server in pairs(shard(tuple_id)) do
            -- check that transaction is queued to all hosts
            local status, reason = pcall(function()
                task_status = server.conn:timeout(REMOTE_TIMEOUT):call(
                    'find_operation', operation_id
                )[1][1]
            end)
            if not status or task_status == nil then
                -- wait until transaction will be queued on all hosts
                failed = server.uri
                break
            end
        end
        if failed == nil then
            if task_status == STATE_INPROGRESS then
                q = queue(ack_operation, redundancy)
                for _, server in ipairs(shard(tuple_id)) do
                    q:put({id=operation_id, server=server})
                end
                q:join()
            end
            return true
        end
        -- operation does not exist
        if task_status == nil then
            break
        end
        log.debug('FAIL')
        fiber.sleep(delay)
        delay = math.min(delay * 2, 5)
    end
    return false
end

local function next_id(space)
    local server_id = self_server.id
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
    if tuple == nil then
        tuple = _schema:insert{key, server_id}
    else
        local next_id = tuple[2] + server_id
        tuple = _schema:update({key}, {{'=', 2, next_id}})
    end
    return tuple[2]
end

-- default request wrappers for db operations
local function insert(self, space, data)
    tuple_id = data[1]
    return request(self, space, 'insert', tuple_id, data)
end

local function auto_increment(self, space, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    return request(self, space, 'insert', id, data)
end

local function select(space, tuple_id)
    return request(self, space, 'select', tuple_id, tuple_id)
end

local function replace(self, space, data)
    tuple_id = data[1]
    return request(self, space, 'replace', tuple_id, data)
end

local function delete(self, space, tuple_id)
    return request(self, space, 'delete', tuple_id, tuple_id)
end

local function update(self, space, key, data)
    return request(self, space, 'update', key, key, data)
end

local function q_insert(self, space, operation_id, data)
    tuple_id = data[1]
    queue_request(self, space, 'insert', operation_id, tuple_id, data)
    return box.tuple.new(data)
end

local function q_auto_increment(self, space, operation_id, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    queue_request(self, space, 'insert', operation_id, id, data)
    return box.tuple.new(data)
end

local function q_replace(self, space, operation_id, data)
    tuple_id = data[1]
    queue_request(self, space, 'replace', operation_id, tuple_id, data)
    return box.tuple.new(data)
end

local function q_delete(self, space, operation_id, tuple_id)
    return queue_request(self, space, 'delete', operation_id, tuple_id, tuple_id)
end

local function q_update(self, space, operation_id, key, data)
    return queue_request(self, space, 'update', operation_id, key, key, data)
end

-- function for shard checking after init
local function check_shard(conn)
    return true
end

local function q_begin()
    local batch_obj = {
        batch = {},
        q = queue(push_operation, redundancy),
        batch_mode = true,
        q_insert = q_insert,
        q_auto_increment = q_auto_increment,
        q_replace = q_replace,
        q_update = q_update,
        q_delete = q_delete,
        q_end = q_end,
    }
    setmetatable(batch_obj, {
        __index = function (self, space)
            return {
                q_auto_increment = function(this, ...)
                    return self.q_auto_increment(self, space, ...)
                end,
                q_insert = function(this, ...)
                    return self.q_insert(self, space, ...)
                end,
                q_replace = function(this, ...)
                    return self.q_replace(self, space, ...)
                end,
                q_delete = function(this, ...)
                    return self.q_delete(self, space, ...)
                end,
                q_update = function(this, ...)
                    return self.q_update(self, space, ...)
                end
        }
    end
    })
    return batch_obj
end

local function create_spaces(cfg)
    if not box.space.operations then
        local operations = box.schema.create_space('operations')
        operations:create_index('primary', {type = 'hash', parts = {1, 'str'}})
        operations:create_index('queue', {type = 'tree', parts = {2, 'num', 1, 'str'}})
    end
    configuration = cfg
end


-- init shard, connect with servers
local function init(cfg, callback)
    create_spaces(cfg)
    log.info('establishing connection to cluster servers...')
    servers = {}
    
    -- math.randomseed(os.time())
    local zones = {}
    for id, server in pairs(cfg.servers) do
        local conn
        log.info(' - %s - connecting...', server.uri)
        while true do
            conn = remote:new(
                cfg.login..':'..cfg.password..'@'..server.uri,
                { reconnect_after = RECONNECT_AFTER }
            )
            if conn:ping() and check_shard(conn) then
                local zone = zones[server.zone]
                if not zone then
                    zone = {}
                    zones[server.zone] = zone
                    table.insert(servers, zone)
                end
                local srv = {
                    uri = server.uri, conn = conn,
                    login=cfg.login, password=cfg.password,
                    id = id
                }
                if callback ~= nil then
                    callback(srv)
                end
                if conn:eval("return box.info.server.uuid") ==
                            box.info.server.uuid then
                    self_server = srv
                end
                table.insert(zone, srv)
                break
            end
            conn:close()
            log.warn(" - %s - shard check failure", server.uri)
            fiber.sleep(1)
        end
        log.info(' - %s - connected', server.uri)
    end
    log.info('connected to all servers')
    redundancy = cfg.redundancy or #servers
    servers_n = #servers
    log.info("redundancy = %d", redundancy)

    -- run monitoring and heartbeat fibers by default
    if cfg.monitor == nil or cfg.monitor then
        fiber.create(heartbeat_fiber)
        fiber.create(monitor_fiber)
    end
    log.info('started')
    init_complete = true
    return true
end

local function len()
    return servers_n
end

local function get_heartbeat()
    return heartbeat_state
end

local function is_connected()
    return init_complete
end

local function wait_connection()
    while not is_connected() do
        fiber.sleep(0.1)
    end
end

local function has_unfinished_operations()
    local tuple = box.space.operations.index.queue:min()
    return tuple ~= nil and tuple[2] ~= STATE_HANDLED
end

local function wait_operations()
    while has_unfinished_operations() do
        fiber.sleep(0.1)
    end
end

local function get_epoch()
    return epoch_counter
end

local function wait_epoch(epoch)
    while get_epoch() < epoch do
        fiber.sleep(0.1)
    end
end

local function is_table_filled()
    local result = true
    for id, server in pairs(configuration.servers) do
        if heartbeat_state[server.uri] == nil then
            result = false
            break
        end
    end
    return result
end

local shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,
    servers = servers,
    len = len,
    redundancy = redundancy,
    is_connected = is_connected,
    wait_connection = wait_connection,
    wait_operations = wait_operations,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,

    shard = shard,
    random_shard = random_shard,
    single_call = single_call,
    request = request,
    queue_request = queue_request,
    init = init,
    check_shard = check_shard,
    insert = insert,
    auto_increment = auto_increment,
    select = select,
    replace = replace,
    update = update,
    delete = delete,
    q_insert = q_insert,
    q_auto_increment = q_auto_increment,
    q_replace = q_replace,
    q_update = q_update,
    q_delete = q_delete,
    q_begin = q_begin,
    check_operation = check_operation,
    get_heartbeat = get_heartbeat,
}



setmetatable(shard_obj, {
    __index = function (self, space)
        return {
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

            q_auto_increment = function(this, ...)
                return self.q_auto_increment(self, space, ...)
            end,
            q_insert = function(this, ...)
                return self.q_insert(self, space, ...)
            end,
            q_replace = function(this, ...)
                return self.q_replace(self, space, ...)
            end,
            q_delete = function(this, ...)
                return self.q_delete(self, space, ...)
            end,
            q_update = function(this, ...)
                return self.q_update(self, space, ...)
            end,
            check_operation = function(this, ...)
                return self.check_operation(self, space, ...)
            end,

            single_call = function(this, ...)
                return self.single_call(self, space, ...)
           end
        }
    end
})

return shard_obj
-- vim: ts=4:sw=4:sts=4:et


