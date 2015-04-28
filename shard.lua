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
local WORKERS = 10
local RECONNECT_AFTER = msgpack.NULL

local self_server

heartbeat_state = {}

local STATE_NEW = 0
local STATE_INPROGRESS = 1
local STATE_HANDLED = 2


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
        end
        fiber.sleep(math.random(100)/1000)
    end
end

-- merge node response data with local table by fiber time
local function merge_tables(response)
    if response == nil then
        return
    end
    for seen_by_uri, node_table in pairs(response) do
        local node_data = heartbeat_state[seen_by_uri]
        if not node_data then
            heartbeat_state[seen_by_uri] = node_table
            node_data = heartbeat_state[seen_by_uri]
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
local function single_call(space, server, operation, ...)
    result = nil
    local status, reason = pcall(function(...)
        self = server.conn:timeout(5 * REMOTE_TIMEOUT).space[space]
        result = self[operation](self, ...)
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
local function request(space, operation, tuple_id, ...)
    local result = {}
    k = 0
    for i, server in ipairs(shard(tuple_id)) do
        result[k] = single_call(space, server, operation, ...)
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
        local status, reason = pcall(function()
            local self = box.space[space]
            self[func_name](self, unpack(args))
        end)
        if not status then
            log.error('failed to %s in space %s: %s', operation,
                  space, reason)
        end
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

local batch_mode = false
local batch = {}
local batch_operation_id

local function q_begin()
    batch = {}
    batch_mode = true
    return queue(push_operation, WORKERS)
end

local function push_queue(q)
    --q = queue(push_operation, WORKERS)
    for server, data in pairs(batch) do
        local tuple = {
            batch_operation_id;
            STATE_NEW;
            data;
        }
        local task = {tuple = tuple, server = server}
        q:put(task)
    end
    q:join()
    -- all shards ready - start workers
    q = queue(ack_operation, WORKERS)
    for server, _ in pairs(batch) do
        q:put({id=batch_operation_id, server=server})
    end
    batch = {}
    batch_mode = false
end

local function q_end(q)
    -- check begin/end order
    if not batch_mode then
        log.error('Cannot run q_end without q_begin')
        return
    end
    push_queue(q)
end

-- fast request with queue
local function queue_request(space, operation, operation_id, tuple_id, ...)
    -- queued push operation in shards
    batch_operation_id = tostring(operation_id)
    for _, server in ipairs(shard(tuple_id)) do
        if batch[server] == nil then
            batch[server] = {}
        end
        local data = { box.space[space].id; operation; {...}; }
        batch[server][#batch[server] + 1] = data
    end
    if not batch_mode then
        q = queue(push_operation, WORKERS)
        push_queue(q)
    end
end

function find_operation(id)
    log.debug('FIND_OP')
    return box.space.operations:get(id)[2]
end

local function check_operation(space, operation_id, tuple_id)
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
                q = queue(ack_operation, WORKERS)
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
local function insert(space, data)
    tuple_id = data[1]
    return request(space, 'insert', tuple_id, data)
end

local function auto_increment(space, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    return request(space, 'insert', id, data)
end

local function select(space, tuple_id)
    return request(space, 'select', tuple_id, tuple_id)
end

local function replace(space, data)
    tuple_id = data[1]
    return request(space, 'replace', tuple_id, data)
end

local function delete(space, tuple_id)
    return request(space, 'delete', tuple_id, tuple_id)
end

local function update(space, key, data)
    return request(space, 'update', key, key, data)
end

local function q_insert(space, operation_id, data)
    tuple_id = data[1]
    queue_request(space, 'insert', operation_id, tuple_id, data)
    return box.tuple.new(data)
end

local function q_auto_increment(space, operation_id, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    queue_request(space, 'insert', operation_id, id, data)
    return box.tuple.new(data)
end

local function q_replace(space, operation_id, data)
    tuple_id = data[1]
    queue_request(space, 'replace', operation_id, tuple_id, data)
    return box.tuple.new(data)
end

local function q_delete(space, operation_id, tuple_id)
    return queue_request(space, 'delete', operation_id, tuple_id, tuple_id)
end

local function q_update(space, operation_id, key, data)
    return queue_request(space, 'update', operation_id, key, key, data)
end

-- function for shard checking after init
local function check_shard(conn)
    return true
end

-- init shard, connect with servers
local function init(cfg, callback)
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
    return true
end

local function len()
    return servers_n
end

local function get_heartbeat()
    return heartbeat_state
end


local shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,
    WORKERS = WORKERS,
    servers = servers,
    len = len,
    redundancy = redundancy,

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
    q_end = q_end,
    check_operation = check_operation,
    get_heartbeat = get_heartbeat,
}

setmetatable(shard_obj, {
    __index = function(self, space)
        return {
            auto_increment = function(...)
                return self.auto_increment(space, ...)
            end,
            insert = function(...)
                return self.insert(space, ...)
            end,
            select = function(...)
                return self.select(space, ...)
            end,
            replace = function(...)
                return self.replace(space, ...)
            end,
            delete = function(...)
                return self.delete(space, ...)
            end,
            update = function(...)
                return self.update(space, ...)
            end,

            q_auto_increment = function(...)
                return self.q_auto_increment(space, ...)
            end,
            q_insert = function(...)
                return self.q_insert(space, ...)
            end,
            q_replace = function(...)
                return self.q_replace(space, ...)
            end,
            q_delete = function(...)
                return self.q_delete(space, ...)
            end,
            q_update = function(...)
                return self.q_update(space, ...)
            end,
            check_operation = function(...)
                return self.check_operation(space, ...)
            end,

            single_call = function(...)
                return self.single_call(space, ...)
            end
        }
    end
})

return shard_obj
-- vim: ts=4:sw=4:sts=4:et


