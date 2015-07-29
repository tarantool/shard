#!/usr/bin/env tarantool

local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')
local uuid = require('uuid')
local json = require('json')
local lib_pool = require('pool')

local shards = {}
local shards_n
local redundancy = 3

local REMOTE_TIMEOUT = 500
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


--queue implementation
local function queue_handler(self, fun)
    fiber.name('queue/handler')
    while true do
        local task = self.ch:get()
        if not task then
            break
        end
        local status, reason = pcall(fun, task)
        if not status then
            if not self.error then
                self.error = reason
                -- stop fiber queue
                self.ch:close()
            else
                self.error = self.error.."\n"..reason
            end
            if task.server then
                self.error = task.server.uri..": "..self.error
            end
            break
        end
    end
    self.chj:put(true)
end


local queue_list = {}

local queue_mt
local function create_queue(fun, workers)
    -- Start fiber queue to processes transactions in parallel
    local channel_size = math.min(workers, redundancy)
    local ch = fiber.channel(workers)
    local chj = fiber.channel(workers)
    local self = setmetatable({ ch = ch, chj = chj, workers = workers, fun=fun }, queue_mt)
    for i=1,workers do
        fiber.create(queue_handler, self, fun)
    end
    return self
end

local function free_queue(q)
    local list = queue_list[q.fun]
    list.n = list.n + 1
    list[list.n] = q
end

local function queue(fun, workers)
    if queue_list[fun] == nil then
        queue_list[fun] = { n = 0 }
    end
    local list = queue_list[fun]
    local len = list.n

    if len > 0 then
        local result = list[len]
        list[len] = nil
	list.n = list.n - 1
        return result
    end
    return create_queue(fun, workers)
end

local function queue_join(self)
    log.debug("queue.join(%s)", self)
    -- stop fiber queue
    while self.ch:is_closed() ~= true and self.ch:is_empty() ~= true do
        fiber.sleep(0)
    end

    while self.chj:is_empty() ~= true do
        self.chj:get()
    end
    log.debug("queue.join(%s): done", self)
    if self.error then
        return error(self.error) -- re-throw error
    end
    free_queue(self)
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

-- main shards search function
local function shard(key, include_dead)
    local num
    if type(key) == 'number' then
        num = key
    else
        num = digest.crc32(key)
    end
    local shard = shards[1 + digest.guava(num, shards_n)]
    local res = {}
    local k = 1
    for i = 1, redundancy do
        local srv = shard[i]
        if pool:server_is_ok(srv) or include_dead then
            res[k] = srv
            k = k + 1
        end
    end
    return res
end

-- base remote operation call
local function single_call(self, space, server, operation, ...)
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
local function request(self, space, operation, tuple_id, ...)
    local result = {}
    k = 1
    for i, server in ipairs(shard(tuple_id)) do
        result[k] = single_call(self, space, server, operation, ...)
        k = k + 1
    end
    return result
end

local function broadcast_select(task)
    local c = task.server.conn
    local index = c:timeout(REMOTE_TIMEOUT).space[task.space].index[task.index]
    local key = task.args[1]
    local offset =  task.args[2]
    local limit = task.args[3]
    local tuples = index:select(key, { offset = offset, limit = limit })
    for _, v in ipairs(tuples) do
        table.insert(task.result, v)
    end
end

local function find_server_in_shard(shard, hint)
    local srv = shard[hint]
    if server_is_ok(srv) then
        return srv
    end
    for i=1,redundancy do
        srv = shard[i]
        if server_is_ok(srv) then
           return srv
        end
    end
    return nil
end

local function q_select(self, space, index, args)
    local sk = box.space[space].index[index]
    local pk = box.space[space].index[0]
    if sk == pk or sk.parts[1].fieldno == pk.parts[1].fieldno then
        local key = args[1]
        if type(key) == 'table' then
            key = key[1]
        end
        local offset = args[2]
        local limit = args[3]
        local srv = shard(key)[1]
        local i = srv.conn:timeout(REMOTE_TIMEOUT).space[space].index[index]
        log.info('%s.space.%s.index.%s:select{%s, {offset = %s, limit = %s}',
            srv.uri, space, index, json.encode(key), offset, limit)
        local tuples = i:select(key, { offset = offset, limit = limit })
        if tuples == nil then
            tuples = {}
        end
        return tuples
    else
        log.info("%s.%s.id = %d",
            space, index, box.space[space].index[index].id)
    end
    local zone = math.floor(math.random() * redundancy) + 1
    local tuples = {}
        local q = queue(broadcast_select, shards_n)
    for i=1,shards_n do
        local srv = find_server_in_shard(shards[i], zone)
        local task = {server = srv, space = space, index = index,
                      args = args, result = tuples }
        q:put(task)
    end
    q:join()
    return tuples
end

local function broadcast_call(task)
    local c = task.server.conn
    local tuples = c:timeout(REMOTE_TIMEOUT):call(task.proc, unpack(task.args))
    for _, v in ipairs(tuples) do
        table.insert(task.result, v)
    end
end

local function q_call(proc, args)
        local q = queue(broadcast_call, shards_n)
    local tuples = {}
    local zone = math.floor(math.random() * redundancy) + 1
    for i=1,shards_n do
        local srv = find_server_in_shard(shards[i], zone)
        local task = { server = srv, proc = proc, args = args,
                      result = tuples }
        q:put(task)
    end
    q:join()
    return tuples
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
    -- fix for memory leaks
    obj.q:join()

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
            next_id = math.floor((tuple[1]+ 2 * servers_n+1)/servers_n)*servers_n + server_id
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
    tuple_id = data[1]
    return request(self, space, 'insert', tuple_id, data)
end

local function auto_increment(self, space, data)
    local id = next_id(space)
    table.insert(data, 1, id)
    return request(self, space, 'insert', id, data)
end

local function select(self, space, tuple_id)
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

local function create_spaces(cfg)
    if not box.space.operations then
        local operations = box.schema.create_space('operations')
        operations:create_index('primary', {type = 'hash', parts = {1, 'str'}})
        operations:create_index('queue', {type = 'tree', parts = {2, 'num', 1, 'str'}})
    end
    configuration = cfg
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
        for _, zone in pairs(zones) do
            log.info('Zone len=%d THERE', #zone.list)
            if shards[shards_n] == nil then
                shards[shards_n] = {}
            end
            local shard = shards[shards_n]
            local srv = zone.list[server_id]
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
    shard_obj.request = request
    shard_obj.queue_request = queue_request

    -- set 1-phase operations
    shard_obj.insert = insert
    shard_obj.auto_increment = auto_increment
    shard_obj.select = select
    shard_obj.replace = replace
    shard_obj.update = update
    shard_obj.delete = delete

    -- set 2-phase operations
    shard_obj.q_insert = q_insert
    shard_obj.q_auto_increment = q_auto_increment
    shard_obj.q_replace = q_replace
    shard_obj.q_update = q_update
    shard_obj.q_delete = q_delete
    shard_obj.q_begin = q_begin
    shard_obj.q_select = q_select
    shard_obj.q_call = q_call

    -- set helpers
    shard_obj.check_operation = check_operation
    shard_obj.get_heartbeat = get_heartbeat
 
    -- enable easy spaces access
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
                q_select = function(this, ...)
                    return self.q_select(self, space, ...)
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
end

-- init shard, connect with servers
local function init(cfg, callback)
    create_spaces(cfg)
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

local function len()
    return shards_n
end

local function is_connected()
    return init_complete
end

local function wait_connection()
    while not is_connected() do
        fiber.sleep(0.01)
    end
end

local function has_unfinished_operations()
    local tuple = box.space.operations.index.queue:min()
    return tuple ~= nil and tuple[2] ~= STATE_HANDLED
end

local function wait_operations()
    while has_unfinished_operations() do
        fiber.sleep(0.01)
    end
end

local function get_epoch()
    return pool:get_epoch()
end

local function wait_epoch(epoch)
    return pool:wait_epoch(epoch)
end

shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,

    shards = shards,
    shards_n = shards_n,
    servers_n = servers_n,
    len = len,
    redundancy = redundancy,
    is_connected = is_connected,
    wait_connection = wait_connection,
    wait_operations = wait_operations,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,
    queue = queue,
    init = init,
    shard = shard,
    check_shard = check_shard,
}

return shard_obj
-- vim: ts=4:sw=4:sts=4:et
