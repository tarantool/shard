#!/usr/bin/env tarantool

local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')
local uuid = require('uuid')
local json = require('json')
local lib_pool = require('connpool')
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
local RESHARDING_RPS = 1000
local RECONNECT_AFTER = msgpack.NULL

local pool = lib_pool.new()
local STATE_NEW = 0
local STATE_INPROGRESS = 1
local STATE_HANDLED = 2

local RSD_CURRENT = 'CUR_SPACE'
local RSD_FINISHED = 'FINISHED_SPACE'
local RSD_HANDLED = 'HANDLED_SPACES'
local RSD_STATE = 'RESHARDING'
local RDS_FLAG = 'RESHARDING_STATE'
local TUPLES_PER_ITERATION = 1000

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
    return init_complete
end

local function wait_connection()
    while not is_connected() do
        fiber.sleep(0.01)
    end
end


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
    local self = setmetatable({
        ch = ch, chj = chj, workers = workers, fun = fun
    }, queue_mt)
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

local maintenance = {}

local function reshard_works()
    -- check that resharing started (used by shard function)
    local mode = box.space.sharding:get{'RESHARDING'}
    return mode ~= nil and mode[2] > 0
end

local function reshard_ready()
    -- check that new nodes are connected after resharing start
    local mode = box.space.sharding:get{'RESHARDING'}
    return mode ~= nil and mode[2] > 1
end

local function lock_transfer()
    box.space.sharding:replace{'TRANSFER_LOCK', 1}
end

local function unlock_transfer()
    box.space.sharding:replace{'TRANSFER_LOCK', 0}
end

local function is_transfer_locked()
    local lock = box.space.sharding:get{'TRANSFER_LOCK'}
    if lock == nil then
        return false
    end
    return lock[2] > 0
end

-- main shards search function
local function shard(key, include_dead, use_old)
    local num = type(key) == 'number' and key or digest.crc32(key)
    local max_shards = shards_n
    -- if we want to find shard in old mapping
    if use_old then
        max_shards = max_shards - 1
    end
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
        if maintenance[srv.id] ~= nil or pool:server_is_ok(srv) or
           include_dead then
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

local function is_valid_index(name, index_data, index_no, strict)
    local index = box.space[name].index[index_no]
    if strict == nil then
        strict = true
    end

    if index_data == nil or index == nil then
        return false
    end

    if #index.parts ~= #index_data.parts then
        return false
    end
    for i, part in pairs(index.parts) do
        local to_check = index_data.parts[i]
        if to_check == nil or
                part.type ~= to_check.type or
                part.fieldno ~= to_check.fieldno then
            return false
        end
    end

    if not strict then
        return true
    end
    local check = {
        'unique', 'id', 'space_id', 'name', 'type'
    }
    for _, key in pairs(check) do
        if index[key] ~= index_data[key] then
            return false
        end
    end
    return true
end

local function schema_worker()
    fiber.name('schema_worker')

    local actions = {
        create_index = function(entity, args)
            local s_name, i_name = entity[1], entity[2]
            box.space[s_name]:create_index(i_name, args)
        end,
        create_space = function(entity, args)
            box.schema.create_space(entity, args)
        end,
        drop_index = function(entity, args)
            local s_name, i_name = entity[1], entity[2]
            box.space[s_name].index[i_name]:drop()
        end,
        drop_space = function(entity, args)
            box.space[entity]:drop()
        end
    }
    while true do
        for _, op in box.space.cluster_manager:pairs() do
            local status, func, entity, args = op[2], op[3], op[4], op[5]
            if status ~= STATE_HANDLED then
                local ok, err = pcall(function()
                    actions[func](entity, args)
                    box.space.cluster_manager:update(
                        op[1], {{'=', 2, STATE_HANDLED}}
                    )
                end)
                if not ok then
                    log.info('Failed to apply schema changes: %s', err)
                end
            end
        end

        fiber.sleep(0.01)
    end
end

local function update_space(space)
    local obj = box.space[space.name]
    for i, index in pairs(space.index) do
        if obj ~= nil and obj.index[index.id] == nil then
            local parts = {}
            for _, part in pairs(index.parts) do
                table.insert(parts, part.fieldno)
                table.insert(parts, part.type)
            end
            box.space.cluster_manager:auto_increment{STATE_NEW, 'create_index',
                {space.name, index.name}, {
                    unique=index.unique,
                    id=index.id,
                    type=index.type,
                    parts=parts
                }
            }
        end
    end
    return true
end

local function update_spaces(config)
    local result = true
    for _, space in pairs(config) do
        result = result and update_space(space)
    end
    return result
end

local function rollback(operation)
    local name, entity, args = operation[3], operation[4], operation[5]
    local manager = box.space.cluster_manager

    local rollback_actions = {
        create_space = function()
            box.space[entity]:drop()
        end,
        create_index = function()
            local s_name, i_name = entity[1], entity[2]
            box.space[s_name].index[i_name]:drop()
        end,
        drop_space = function()
            -- we don't need to create empty space
        end,
        drop_index = function()
            local s_name, i_name = entity[1], entity[2]
            local sp = box.space[s_name]
            if sp ~= nil then
                sp:create_index(i_name, args)
            end
        end
    }
    rollback_actions[name]()
end

local function rollback_schema()
    local manager = box.space.cluster_manager
    local ops = manager:select({}, {iterator='REQ'})
    for _, operation in pairs(ops) do
        rollback(operation)
    end
    manager:truncate()
    return true
end

local function commit_schema()
    box.space.cluster_manager:truncate()
    return true
end

local function drop_space(space_name)
    if box.space[space_name] == nil then
        return false, 'Space does not exists'
    end
    box.space.cluster_manager:auto_increment{
        STATE_NEW, 'drop_space',
        space_name
    }
    return true
end

local function drop_index(space_name, index_name)
    local obj = box.space[space_name]
    if obj == nil then
        return false, 'Space does not exists'
    end
    local index = obj.index[index_name]
    if index == nil then
        return false, 'Index does not exists'
    end
    local parts = {}
    for _, part in pairs(index.parts) do
        table.insert(parts, part.fieldno)
        table.insert(parts, part.type)
    end
    box.space.cluster_manager:auto_increment{STATE_NEW, 'drop_index',
        {space_name, index_name}, {
            unique=index.unique,
            type=index.type,
            parts=parts
        }
    }
    return true
end

local function create_spaces(config)
    local manager = box.space.cluster_manager

    for i, space in pairs(config) do
        if box.space[space.name] ~= nil then
            return false, 'Space already exists'
        end
        --_ = box.schema.create_space(space.name, space.options)
        manager:auto_increment{
            STATE_NEW, 'create_space',
            space.name, space.options
        }
        update_space(space)
    end
    return true
end

local function validate_sources(config, strict)
    for i, space in pairs(config) do
        if box.space[space.name] == nil then
            return false, {name=space.name, index=nil}
        end
        for k,v in pairs(box.space[space.name].index) do
            if type(k) == 'number' then
                local is_valid = is_valid_index(
                    space.name, space.index[k], k, strict
                )
                if space.index[k] == nil or not is_valid  then
                    return false, {name=space.name, index=k}
                end
            end
        end
        for k,v in pairs(space.index) do
            if type(k) == 'number' then
                local is_valid = is_valid_index(
                    space.name, space.index[k], k, strict
                )
                if space.index[k] == nil or not is_valid  then
                    return false, {name=space.name, index=k}
                end
            end
        end
    end
    return true
end

local function drop_rsd_index(worker, space)
    lock_transfer()
    if worker.index[2] ~= nil then
        worker.index[2]:drop()
    end
    --worker:truncate()
    local i = 0
    for _, w_tuple in worker:pairs() do
        i = i + 1
        worker:delete{w_tuple[1]}
        if i % 10000 == 0 then
            fiber.sleep(0.01)
        end
    end

    local parts = {}
    for i, part in pairs(space.index[0].parts) do
        -- shift main index for sh_worker space
        table.insert(parts, i + 2)
        table.insert(parts, part.type)
    end
    if worker.index.lookup == nil then
        worker:create_index(
            'lookup', {type = 'tree', parts = parts}
        )
    end
    unlock_transfer()
end

local function process_tuple(space, tuple, worker, lookup)
    local shard_id = tuple[space.index[0].parts[1].fieldno]
    local old_sh = shard(shard_id, false, true)[1]
    local new_sh = shard(shard_id)[1]
    if new_sh.id == old_sh.id then
        return false
    end

    local data = {}
    for i, part in pairs(space.index[0].parts) do
        table.insert(data, tuple[part.fieldno])
    end
    if lookup:get(data) == nil then
        table.insert(data, 1, STATE_NEW)
        worker:auto_increment(data)
    end
    return true
end

local function tree_iter(space, worker, lookup, fun)
    local tuples = 0
    local params = {limit=RESHARDING_RPS, iterator = 'GT'}
    local data = space.index[0]:select({}, params)
    local last_id = {}

    while #data > 0 do
        last_id = data[#data]
        for _, tuple in pairs(data) do
            if fun(space, tuple, worker, lookup) then
                tuples = tuples +1
            end
        end

        local key = {}
        for _, part in pairs(space.index[0].parts) do
            table.insert(key, last_id[part.fieldno])
        end
        data = space.index[0]:select(key, params)
        fiber.sleep(0.1)
    end
    return tuples
end

local function hash_iter(space, worker, lookup, fun)
    local tuples, i = 0, 0
    for _, tuple in space:pairs() do
        i = i + 1
        if fun(space, tuple, worker, lookup) then
            tuples = tuples +1
        end
        -- do not use 100% CPU
        if i == RESHARDING_RPS then
            i = 0
            fiber.sleep(0.1)
        end
    end
    return tuples
end

local function space_iteration(is_drop)
    local mode = box.space.sharding:get(RSD_CURRENT)
    if mode == nil or mode[2] == '' then
        return
    end
    -- wait for until current transfer is working
    while is_drop and is_transfer_locked() do
        fiber.sleep(0.001)
    end

    local space_name = mode[2]
    local space = box.space[space_name]
    local aux_space = 'sh_worker'
    if space.engine == 'vinyl' then
        aux_space = 'sh_worker_vinyl'
    end
    local worker = box.space[aux_space]

    -- drop prev space
    if is_drop then
        drop_rsd_index(worker, space)
    end
    local lookup = worker.index.lookup

    log.info('Space iteration for space %s', space_name)
    local tuples = 0

    if space.index[0].type == 'HASH' then
        tuples = hash_iter(space, worker, lookup, process_tuple)
    else
        tuples = tree_iter(space, worker, lookup, process_tuple)
    end

    log.info('Found %d tuples', tuples)
    box.space.sharding:replace{RSD_FINISHED, space_name}
    return true
end

local function contains(arr, elem)
    for _, item in pairs(arr) do
        if item == elem then
            return true
        end
    end
    return false
end

local function is_reshardable(space_name)
    if string.sub(space_name, 1, 1) == '_' then
        return false
    end
    local space = box.space[space_name]
    if space.engine == 'vinyl' and space:count() == 0 then
        return false
    end
    if space.engine == 'memtx' and space:len() == 0 then
        return false
    end
    local reserved = {
        operations=1, sharding=1,
        sh_worker=1, cluster_manager=1,
        sh_worker_vinyl=1
    }
    return reserved[space_name] == nil
end

local function transfer(self, space, worker, data, force)
    for _, meta in pairs(data) do
        local index = {}
        if force then
            index = meta
        else
            for i=3,#meta do
                table.insert(index, meta[i])
            end
            worker:update(meta[1], {{'=', 2, STATE_INPROGRESS}})
        end
        local tuple = space:get(index)
        local ok, err = pcall(function()
            local nodes = shard(tuple[1])
            self:single_call(space.name, nodes[1], 'replace', tuple)
        end)
        if ok then
            box.begin()
            if not force then
                worker:update(meta[1], {{'=', 2, STATE_HANDLED}})
            end
            space:delete(index)
            box.commit()
        else
            log.info('Transfer error: %s', err)
        end
    end
end

local function force_transfer(space_name, index)
    local space = box.space[space_name]
    if space == nil then
        return false
    end
    local aux_space = 'sh_worker'
    if space ~= nil and space.engine == 'vinyl' then
        aux_space = 'sh_worker_vinyl'
    end
    local worker = box.space[aux_space]
    local ok, err = pcall(function()
        transfer(shard_obj, space, worker, {index}, true)
    end)
    if not ok then
        log.info('Transfer failed: %s', err)
    end
    return ok
end

local function transfer_worker(self)
    fiber.name('transfer')
    wait_connection()
    while true do
        if reshard_ready() then
            local cur_space = box.space.sharding:get(RSD_CURRENT)
            local space = box.space[cur_space[2]]

            local aux_space = 'sh_worker'
            if space ~= nil and space.engine == 'vinyl' then
                aux_space = 'sh_worker_vinyl'
            end
            local worker = box.space[aux_space]

            lock_transfer()
            if cur_space ~= nil and cur_space[2] ~= ''
                    and worker.index[1] ~= nil and worker.index[2] ~= nil then

                local data = worker.index[1]:select(
                    {STATE_NEW}, {limit=TUPLES_PER_ITERATION}
                )
                while #data > 0 do
                    transfer(self, space, worker, data)
                    data = worker.index[1]:select(
                        {STATE_NEW}, {limit=TUPLES_PER_ITERATION}
                    )
                end
            end
            unlock_transfer()
        end
        fiber.sleep(0.01)
    end
end

local function wait_state(space, state)
    local index = space.index.status
    local iter = index:pairs(state)
    while iter.gen(iter.param, iter.state) ~= nil do
        fiber.sleep(0.1)
    end
end

local function wait_iteration()
    wait_state(box.space.sh_worker, STATE_NEW)
    wait_state(box.space.sh_worker_vinyl, STATE_NEW)
    wait_state(box.space.sh_worker, STATE_INPROGRESS)
    wait_state(box.space.sh_worker_vinyl, STATE_INPROGRESS)
end

local function rsd_init()
    local sh = box.space.sharding
    sh:replace{RSD_HANDLED, {}}
    sh:replace{RSD_CURRENT, ''}
    sh:replace{RSD_FINISHED, ''}
    unlock_transfer()
end

local function rsd_finalize()
    while is_transfer_locked() do
        fiber.sleep(0.1)
    end
    space_iteration(false)
    wait_iteration()

    log.info('Resharding complete')
    local sh = box.space.sharding
    sh:replace{RSD_STATE, 0}
    sh:replace{RSD_HANDLED, {}}
    sh:replace{RSD_CURRENT, ''}
    sh:replace{RSD_FINISHED, ''}
end

local function get_next_space(sh_state, cur_space)
    local handled = sh_state:get{RSD_HANDLED}[2]
    for _, obj in pairs(box.space) do
        local space_name = obj.name
        if is_reshardable(space_name) and
                not contains(handled, space_name) then
            -- update handled spaces list
            -- we must check twise space
            if cur_space ~= space_name then
                if cur_space ~= '' then
                    while is_transfer_locked() do
                        fiber.sleep(0.1)
                    end
                    space_iteration(false)
                    wait_iteration()
                    table.insert(handled, cur_space)
                    sh_state:replace{RSD_HANDLED, handled}
                end
                -- set current reshadring pointer to this space
                sh_state:replace{RSD_CURRENT, space_name}
                return true
            end
        end
    end
    return false
end

local function rsd_warmup(self)
    wait_connection()
    local cur_space = box.space.sharding:get(RSD_CURRENT)[2]
    local space = box.space[cur_space]
    local worker_name = 'sh_worker'
    if space.engine == 'vinyl' then
        worker_name = 'sh_worker_vinyl'
    end
    local worker = box.space[worker_name]

    local data = worker.index[1]:select(
        {STATE_INPROGRESS}
    )
    log.info('WARMUP')
    if #data > 0 then
        log.info('Resharding warmup: %d tuples', #data)
        transfer(self, space, worker, data)
    end
end

local function resharding_worker(self)
    fiber.name('resharding')
    local sh = box.space.sharding
    local rs_state = sh:get(RSD_STATE)
    if rs_state == nil or rs_state[2] == 0 then
        rsd_init()
    else
        rsd_warmup(self)
    end

    while true do
        if reshard_ready() then
            local cur_space = sh:get(RSD_CURRENT)[2]
            local finished = sh:get(RSD_FINISHED)[2]

            -- resharding finished, we can switch to the next space
            if finished == cur_space then
                local has_new_space = get_next_space(sh, cur_space)
                while is_transfer_locked() do
                    fiber.sleep(0.1)
                end

                if has_new_space then
                    space_iteration(true)
                else
                    rsd_finalize()
                end
            end
        end

        fiber.sleep(0.01)
    end
end

local function rsd_join()
    -- storages checker. Can be used in app servers
    fiber.name('Resharding waiter')
    while true do
        local cluster = _G.remote_resharding_state()
        local in_progress = 0
        for i, node in pairs(cluster) do
            local response = node.data[1][1]
            if response.status then
                in_progress = 1
                break
            end
        end
        box.space.sharding:replace{RDS_FLAG, in_progress}
        if in_progress == 0 then
            log.info('Resharding state: off')
            return
        end
        fiber.sleep(0.1)
    end
end

local function set_rsd()
    box.space.sharding:replace{RDS_FLAG, 1}
    fiber.create(rsd_join)
end

_G.remote_resharding_state = function()
    local result = {}
    for j = 1, #shards do
         local srd = shards[j]
         for i = 1, redundancy do
               local res = { uri = srd[i].uri }
               local ok, err = pcall(function()
                   local conn = srd[i].conn
                   res.data = conn[nb_call](
                       conn, 'resharding_status'
                   )
              end)
              table.insert(result, res)
         end
    end
    return result
end

local function resharding_status()
    local status = box.space.sharding:get{RSD_STATE}
    if status ~= nil then
        status = status[2] == 2
    else
        status = false
    end
    local spaces = box.space.sharding:get{RSD_HANDLED}[2]
    local current = box.space.sharding:get{RSD_CURRENT}[2]
    local worker_len = box.space.sh_worker.index[1]:count(STATE_NEW)
    worker_len = worker_len + box.space.sh_worker_vinyl.index[1]:count(STATE_NEW)
    return {
        status=status,
        spaces=spaces,
        in_progress=current,
        tasks=worker_len
    }
end

local function append_shard(servers, is_replica, start_waiter)
    local non_arbiters = {}

    for _, candidate in ipairs(servers) do
        if not candidate.arbiter then
            table.insert(non_arbiters, candidate)
        end
    end

    if #non_arbiters ~= redundancy then
        return false, 'Amount of servers is not equal redundancy'
    end
    -- Turn on resharding mode
    if not is_replica then
        box.space.sharding:replace{RSD_STATE, 1}
    end
    -- add new "virtual" shard and append server
    shards[shards_n + 1] = {}
    shards_n = shards_n + 1

    -- connect pair to shard
    for id, server in pairs(non_arbiters) do
        -- create zone if needed
        if pool.servers[server.zone] == nil then
            pool.zones_n = pool.zones_n + 1
            pool.servers[server.zone] = {
                id = pool.zones_n, n = 0, list = {}
            }
        end
        -- disable new shard by default
        --server.ignore = true
        -- append server to connection pool
        pool:connect(shards_n*redundancy - id + 1, server)
        local zone = pool.servers[server.zone]
        zone.list[zone.n].zone_name = server.zone

        if not server.arbiter then
            table.insert(shards[shards_n], zone.list[zone.n])
        end
    end
    if not is_replica then
        box.space.sharding:replace{RSD_STATE, 2}
    elseif start_waiter then
        set_rsd()
    end
    return true
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

local function remote_append(servers)
    return cluster_operation("append_shard", servers)
end

-- base remote operation on space
local function space_call(self, space, server, fun, ...)
    local result = nil
    local status, reason = pcall(function(...)
        local conn = server.conn:timeout(5 * REMOTE_TIMEOUT)
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
        log.error(err)
        result = {status=false, error=err}
        if not server.conn:is_connected() then
            log.error("server %s is offline", server.uri)
        end
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
        local conn = server.conn:timeout(REMOTE_TIMEOUT)
        result = conn[nb_call](conn, func_name, ...)
    end, ...)
    if not status then
        log.error('failed to call %s on %s: %s', func_name, server.uri, reason)
        if not server.conn:is_connected() then
            log.error("server %s is offline", server.uri)
        end
    end
    return result
end

local function lookup(self, space, tuple_id, ...)
    local q_args = {...}
    local key = {}
    if #q_args > 0 then
        key = q_args[1]
    else
        return false, 'Wrong params'
    end

    -- try to find in new shard
    local nodes = shard(tuple_id)
    local new_data = single_call(self, space, nodes[1], 'select', key)
    if #new_data > 0 then
        -- tuple transfer complete, we can use new shard for this request
        return nodes
    end

    local old_nodes = shard(tuple_id, false, true)
    local old_data = single_call(self, space, old_nodes[1], 'select', key)
    if #old_data == 0 then
        -- tuple not found in old shard:
        -- 1. it does not exists
        -- 2. space transfer complete and in moved to new shard
        -- Conclusion: we need to execute request in new shard
        return nodes
    end

    -- tuple was found let's check transfer state
    local result = index_call(
        self, 'sharding', old_nodes[1],
        'select', 3, key
    )
    if #result == 0 then
        -- we must use old shard
        -- FIXME: probably prev iteration is finished
        -- and we need to check CUR_SPACE or sharding:len()
        return old_nodes
    end

    if result[1][2] == STATE_INPROGRESS then
        -- transfer started, we mast wait for transfer
        local res = direct_call(
            self, old_nodes[1],
            'transfer_wait', space, key
        )
    end
    -- transfer complete we can execute request in new shard
    return nodes
end

local function is_transfered_space(space)
    local mode = box.space.sharding:get('CUR_SPACE')
    if mode == nil or mode[2] ~= space then
        return false
    end
    return true
end

--function ch_select(space, key)
--    return shard_obj[space]:select(key)
--end

local function transfer_wait(space, key)
    if not reshard_works() then
        return false
    end

    local result = {}
    while result ~= nil or result[2] ~= STATE_HANDLED do
        -- if space transfer is finished we don't need to wait
        if not is_transfered_space(space) then
            return true
        end
        local ok, err = pcall(function()
            result = box.space.sharding.index[3]:get(key)
        end)
        if not ok then
            -- if transfer finished it's not error
            -- but index can be dropped
            if not is_transfered_space(space) then
                return true
            end
            return false, err
        end
        fiber.sleep(0.01)
    end
    return true
end

-- shards request function
local function request(self, space, operation, tuple_id, ...)
    local result = {}
    local nodes = {}
    if operation == 'insert' or not reshard_works() then
        nodes = shard(tuple_id)
    else
        -- check where is the tuple
        nodes = lookup(self, space, tuple_id, ...)
    end

    for _, server in ipairs(nodes) do
        table.insert(result, single_call(self, space, server, operation, ...))
        if configuration.replication == true then
            break
        end
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
    if pool:server_is_ok(srv) then
        return srv
    end
    for i = 1, redundancy do
        srv = shard[i]
        if pool:server_is_ok(srv) then
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
    for i = 1, shards_n do
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
    local conn = c:timeout(REMOTE_TIMEOUT)
    local tuples = conn[nb_call](conn, task.proc, unpack(task.args))
    for _, v in ipairs(tuples) do
        table.insert(task.result, v)
    end
end

local function q_call(proc, args)
        local q = queue(broadcast_call, shards_n)
    local tuples = {}
    local zone = math.floor(math.random() * redundancy) + 1
    for i = 1, shards_n do
        local srv = find_server_in_shard(shards[i], zone)
        local task = { server = srv, proc = proc, args = args,
                      result = tuples }
        q:put(task)
    end
    q:join()
    return tuples
end

-- execute operation, call from remote node
local function execute_operation(operation_id)
    log.debug('EXEC_OP')
    -- execute batch
    box.begin()
    local tuple = box.space.operations:update(
        operation_id, {{'=', 2, STATE_INPROGRESS}}
    )
    local batch = tuple[3]

    local result

    for _, operation in ipairs(batch) do
        local space = operation[1]
        local func_name = operation[2]
        local args = operation[3]
        local self = box.space[space]
        result = self[func_name](self, unpack(args))
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
    local conn = server.conn:timeout(REMOTE_TIMEOUT)
    conn[nb_call](
        conn, 'execute_operation', operation_id
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

        -- insert into first server and break if we use replication
        if configuration.replication == true then
            break
        end
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

local function find_operation(id)
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
                local conn = server.conn:timeout(REMOTE_TIMEOUT)
                task_status = conn[nb_call](
                    conn, 'find_operation', operation_id
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
                local q = queue(ack_operation, redundancy)
                for _, server in ipairs(shard(tuple_id)) do
                    q:put({ id = operation_id, server = server })
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

local function q_insert(self, space, operation_id, data)
    local tuple_id = data[1]
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
    local tuple_id = data[1]
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

local function init_create_spaces(cfg)
    if box.space.operations == nil then
        local operations = box.schema.create_space('operations')
        operations:create_index('primary', {type = 'hash', parts = {1, 'str'}})
        operations:create_index('queue', {type = 'tree', parts = {2, 'num', 1, 'str'}})
    end
    if box.space.cluster_manager == nil then
        local cluster_ops = box.schema.create_space('cluster_manager')
        cluster_ops:create_index('primary', {type = 'tree', parts = {1, 'num'}})
        cluster_ops:create_index(
            'status', {type = 'tree', parts = {2, 'num'},
            unique=false}
        )
    end
    if box.space.sh_worker == nil then
        local sh_space = box.schema.create_space('sh_worker')
        sh_space:create_index('primary', {type = 'tree', parts = {1, 'num'}})
        sh_space:create_index('status', {type = 'tree', parts = {2, 'num'}, unique=false})
    end
    if box.space.sh_worker_vinyl == nil then
        local sh_space = box.schema.create_space('sh_worker_vinyl', {engine='vinyl'})
        sh_space:create_index('primary', {type = 'tree', parts = {1, 'num'}})
        sh_space:create_index('status', {type = 'tree', parts = {2, 'num'}, unique=false})
    end
    -- sharding manager settings
    if box.space.sharding == nil then
        local sharding = box.schema.create_space('sharding')
        sharding:create_index('primary', {type = 'tree', parts = {1, 'str'}})
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
    shard_obj.on_action = on_action

    -- set helpers
    shard_obj.check_operation = check_operation
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
    init_create_spaces(cfg)
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
    RESHARDING_RPS = cfg.rsd_max_rps or RESHARDING_RPS

    enable_operations()
    log.info('Done')
    init_complete = true
    return true
end

local function len(self)
    return self.shards_n
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

-- declare global functions
_G.append_shard      = append_shard
_G.create_spaces     = create_spaces
_G.update_spaces     = update_spaces
_G.join_shard        = join_shard
_G.unjoin_shard      = unjoin_shard
_G.commit_schema     = commit_schema
_G.rollback_schema   = rollback_schema
_G.resharding_status = resharding_status
_G.remote_append     = remote_append
_G.remote_join       = remote_join
_G.remote_unjoin     = remote_unjoin
_G.drop_space        = drop_space
_G.drop_index        = drop_index

_G.find_operation    = find_operation
_G.transfer_wait     = transfer_wait

_G.cluster_operation = cluster_operation
_G.execute_operation = execute_operation
_G.force_transfer    = force_transfer
_G.get_zones         = get_zones
_G.is_valid_index    = is_valid_index
_G.merge_sort        = merge_sort
_G.shard_status      = shard_status
_G.update_space      = update_space
_G.validate_sources  = validate_sources

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
    wait_operations = wait_operations,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,
    queue = queue,
    init = init,
    shard = shard,
    check_shard = check_shard,
    enable_resharding = function(self)
        fiber.create(schema_worker)
        fiber.create(resharding_worker, self)
        fiber.create(transfer_worker, self)
        log.info('Enabled resharding workers')
    end
}

return shard_obj
-- vim: ts=4:sw=4:sts=4:et
