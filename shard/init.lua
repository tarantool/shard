local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')
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
local RSD_FLAG = 'RESHARDING_STATE'
local TUPLES_PER_ITERATION = 1000

local connection_fiber = {
    fiber = nil,
    state = 'disconnected',
}
local init_complete = false
local configuration = {}
local shard_obj

--- 1.6 and 1.7 netbox compat
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
            if not srd[i]:is_connected() then
                return false
            end
        end
    end
    return true
end

--[[
Correct way to handle connection fiber
Arguments:
    1) url in host:port format
    2) optional arguments (as Lua table)
        - user
        - password
        - timeout in seconds
        - reconnect_after in seconds (default: 1)
    If timeout is nil, fiber will not wait for the connection
Usage:
    net_connect('localhost:3301', { user = 'admin', password = '123', timeout = 10 })
    net_connect('localhost:3301') -- do not wait guest login
Return:
    1) ok - true/false
    2) net.box connection object (ok == true) OR an error message (ok == false)
]]--
local function net_connect(uri, opts)
    local ok, conn = pcall(remote.new, remote, uri, {
            wait_connected = false,
            reconnect_after = opts.reconnect_after or 1,
            user = opts.user,
            password = opts.password
    })
    if not ok then
        return false, conn
    end
    local tm = opts.timeout or 0
    if not conn:wait_connected(tm) then
        local _error = conn.error
        conn:close()
        return false, _error
    end
    return true, conn
end

local function make_error(errno, fmt, ...)
   if type(fmt) ~= 'string' then
        fmt = tostring(fmt)
    end

    if select('#', ...) ~= 0 then
        local stat
        stat, fmt = pcall(string.format, fmt, ...)
        if not stat then
            error(fmt, 2)
        end
    end

    return nil, { errno = errno, error = fmt }
end

-- public API func that blocks invoked fiber until all shards are connected
local function wait_for_shards_to_go_online(timeout, delay)
    local wait_start_time = fiber.time()
    local delay = delay or 0.1
    local timeout = timeout or 10
    while fiber.time() - wait_start_time < timeout do
        if connection_fiber.state == 'connected' then
            return true
        end
        fiber.sleep(delay * 2)
    end
    return false, "Timed out waiting for shards to go up"
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

local function reshard_works(synchronizer_enabled)
    local mode
    if synchronizer_enabled then
        mode = box.space._shard:get{RSD_FLAG}
    else
        mode = box.space._shard:get{RSD_STATE}
    end
    return mode ~= nil and mode[2] == 1
end

local function lock_transfer()
    box.space._shard:replace{'TRANSFER_LOCK', 1}
end

local function unlock_transfer()
    box.space._shard:replace{'TRANSFER_LOCK', 0}
end

local function is_transfer_locked()
    local lock = box.space._shard:get{'TRANSFER_LOCK'}
    if lock == nil then
        return false
    end
    return lock[2] > 0
end

-- function determinates on which shard data is
-- @key - shard key
-- @include_dead - if a node in the maintenance mode and this option is true,
-- then this node would be added to shard function
-- @use_old - if resharding state is active and this option is true, then new node
-- would be excluded from shard function
-- @returns shard where data is or nil, {error = error_text}
local function shard(key, include_dead, use_old)
    local num = type(key) == 'number' and key or digest.crc32(key)
    local max_shards = shards_n
    -- if we want to find shard in old mapping
    if use_old then
        max_shards = max_shards - 1
    end
    local shard_id = 1 + digest.guava(num, max_shards)
    local shard = shards[shard_id]
    if shard == nil or shard[1] == nil then
        return make_error(nil, 'Shard function have returned an empty result. ' ..
                               'Please make sure that your storages are alive.')
    end
    local res = {}

    -- The master node and its replicas have been located in the reverse order.
    -- It means what master is located as shard[#shard].
    -- We should reverse the order of this table, because it's more obvious
    -- to get a master as first element
    for i = #shard, 1, -1 do
        local server = shard[i]
        -- A node in the maintenance mode may be in the read-only state.
        -- In this case if the node will be excluded from the output of the
        -- shard function, a recording to the cluster will be affected, because
        -- the shard function will be changed. It will cause an inconsistent state
        -- of the cluster.
        -- If a write request will be sent to the sever in the maintenance mode,
        -- it will finished with error.
        -- TODO: in future the entire cluster should be switched to the read-only
        -- mode in this case
        if server:is_ready() or include_dead then
                log.warn(
                    "Shard %d has node %s in maintenance",
                    shard_id, server.uri
                )
                table.insert(res, #shard, server)
        elseif pool:server_is_ok(server) then
            table.insert(res, server)
        end
    end

    if #res ~= #shard then
        log.warn("Shard %d is inconsistent state", shard_id)
        log.warn("Consider checking it manually")
        if #res == 0 then
            log.error("Shard %d is offline", shard_id)
        end
    end

    return res
end

local function shard_status()
    local result = {
        online = {},
        offline = {},
        uuids = {}
    }
    for _, shard_set in ipairs(shards) do
        for _, shard in ipairs(shard_set) do
            local s = { uri = shard.uri, id = shard.id }
            if shard:is_connected() then
                local ok, info = pcall(shard.conn.call, shard.conn, 'box.info')
                if not ok then
                    log.error(info)
                else
                    table.insert(result.online, s)
                    result.uuids[info.uuid] = s.uri
                end
            else
                table.insert(result.offline, s)
            end
        end
    end
    return result
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
        -- shift main index for _shard_worker space
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
    local old_sh = shard(shard_id, true)[1]
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
    local mode = box.space._shard:get(RSD_CURRENT)
    if mode == nil or mode[2] == '' then
        return
    end
    -- wait for until current transfer is working
    while is_drop and is_transfer_locked() do
        fiber.sleep(0.001)
    end

    local space_name = mode[2]
    local space = box.space[space_name]
    local aux_space = '_shard_worker'
    if space.engine == 'vinyl' then
        aux_space = '_shard_worker_vinyl'
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
    box.space._shard:replace{RSD_FINISHED, space_name}
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
    return true
end

local function transfer(self, space, worker, data, force)
    for _, meta in pairs(data) do
        -- data from worker may be deleted by the truncate function
        if worker:get{meta[1]} == nil then
            goto continue
        end
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
        -- tuple may be deleted by the 'truncate' function
        if not tuple then
            goto continue
        end
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
        ::continue::
    end
end

local function force_transfer(space_name, index)
    local space = box.space[space_name]
    if space == nil then
        return false
    end
    local aux_space = '_shard_worker'
    if space ~= nil and space.engine == 'vinyl' then
        aux_space = '_shard_worker_vinyl'
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
    fiber.name('_transfer_worker')
    if not wait_for_shards_to_go_online() then
        log.error("Transfer worker: failed waiting for shards to go up")
        return nil
    end
    while true do
        if reshard_works() then
            local cur_space = box.space._shard:get(RSD_CURRENT)
            local space = box.space[cur_space[2]]

            local aux_space = '_shard_worker'
            if space ~= nil and space.engine == 'vinyl' then
                aux_space = '_shard_worker_vinyl'
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
    wait_state(box.space._shard_worker, STATE_NEW)
    wait_state(box.space._shard_worker_vinyl, STATE_NEW)
    wait_state(box.space._shard_worker, STATE_INPROGRESS)
    wait_state(box.space._shard_worker_vinyl, STATE_INPROGRESS)
end

local function rsd_init()
    local sh = box.space._shard
    sh:replace{RSD_STATE, 0}
    sh:replace{RSD_HANDLED, {}}
    sh:replace{RSD_CURRENT, ''}
    sh:replace{RSD_FINISHED, ''}
    unlock_transfer()
end

local function rsd_finalize()
    while is_transfer_locked() do
        fiber.sleep(0.1)
    end
    space_iteration(true)
    wait_iteration()

    log.info('Resharding complete')
    local sh = box.space._shard
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
    if not wait_for_shards_to_go_online() then
        log.error("Resharding warmup: failed waiting for shards to go up")
        return nil
    end
    local cur_space = box.space._shard:get(RSD_CURRENT)[2]
    local space = box.space[cur_space]
    local worker_name = '_shard_worker'
    if space.engine == 'vinyl' then
        worker_name = '_shard_worker_vinyl'
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
    fiber.name('_resharding_worker')
    local sh = box.space._shard
    local rs_state = sh:get(RSD_STATE)
    if rs_state == nil or rs_state[2] == 0 then
        rsd_init()
    else
        rsd_warmup(self)
    end

    while true do
        if reshard_works() then
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

local function is_synchronizer_enabled()
    return box.space._shard:get{RSD_FLAG} ~= nil
end

local function resharding_status_syncronizer()
    fiber.name('Resharding status synchronizer')
    while true do
        local cluster = _G.remote_resharding_state()
        local current_in_progress = box.space._shard:get{RSD_FLAG}[2]
        local in_progress = 0
        for _, node in pairs(cluster) do
            if node.data and node.data[1] and node.data[1][1] then
                local response = node.data[1][1]
                if response.status then
                    in_progress = 1
                    break
                end
            end
        end
        if current_in_progress ~= in_progress then
            box.space._shard:replace{RSD_FLAG, in_progress}
            local state = in_progress ~= 0 and 'on' or 'off'
            log.info('Resharding state: %s', state)
        end

        fiber.sleep(0.1)
    end
end

-- synchronizer used to pass a resharding status from storages(nodes
-- where a data kept) to application nodes
local function init_synchronizer()
    box.space._shard:replace{RSD_FLAG, 0}
    fiber.create(resharding_status_syncronizer)
    log.info("Resharding synchronizer enabled")
end

_G.remote_resharding_state = function()
    local result = {}
    for _, shard_set in ipairs(shards) do
        for _, shard in ipairs(shard_set) do
            local res = { uri = shard.uri }
            local ok, err = pcall(function()
                local conn = shard.conn
                if not conn then
                    error("Server is unavailable")
                end
                res.data = conn[nb_call](conn, 'resharding_status')
            end)
            if not ok then
                log.debug("operation resharding_status failed on node: %s with: %s",
                          res.uri, err)
            end
            table.insert(result, res)
        end
    end
    return result
end

local function resharding_status()
    local status = box.space._shard:get{RSD_STATE}
    if status ~= nil then
        status = status[2] == 1
    else
        status = false
    end
    local spaces = box.space._shard:get{RSD_HANDLED}[2]
    local current = box.space._shard:get{RSD_CURRENT}[2]
    local worker_len = box.space._shard_worker.index[1]:count(STATE_NEW)
    worker_len = worker_len + box.space._shard_worker_vinyl.index[1]:count(STATE_NEW)
    return {
        status=status,
        spaces=spaces,
        in_progress=current,
        tasks=worker_len
    }
end

local function append_shard(servers)
    local non_arbiters = {}

    for _, candidate in ipairs(servers) do
        if not candidate.arbiter then
            table.insert(non_arbiters, candidate)
        end
    end

    if #non_arbiters ~= redundancy then
        return false, 'Amount of servers is not equal redundancy'
    end

    -- get updated shard list from a new shard
    local login    = configuration.login
    local password = configuration.password
    local ok, conn = net_connect(non_arbiters[1].uri, {
        user = login,
        password = password,
        timeout = 10,
        reconnect_after = RECONNECT_AFTER
    })
    if not ok then
        return false, string.format(
            "Server '%s' is unavailable, error: %s",
            non_arbiters[1].uri, conn
        )
    end
    local replica_sets = conn:call("get_server_list")
    conn:close()

    local zones = {}
    -- connect pair to shard
    for id, server in ipairs(non_arbiters) do
        -- retrieve zone from configs
        local server_in_config = false
        for _, srv in ipairs(replica_sets) do
            if server.uri == srv.uri then
                if type(srv.zone) ~= 'string' then
                    return false, string.format(
                        "Config is incorrect. " ..
                        "Expected 'string' for zone, got '%s'", type(srv.zone)
                    )
                end
                server_in_config = true
                server.zone = srv.zone
            end
        end
        if not server_in_config then
            return false, string.format(
                "Config is incorrect. Server is not found for '%s'", server.uri
            )
        end
        if not server.zone then
            return false, string.format(
                "Config is incorrect. Zone is missing for '%s'", server.uri
            )
        end
        -- check distinction of zones
        if zones[server.zone] then
            return false, "Config is incorrect. Zones must vary in replica set"
        else
            zones[server.zone] = true
        end
        -- create zone if needed
        if pool.servers[server.zone] == nil then
            pool.zones_n = pool.zones_n + 1
            pool.servers[server.zone] = {
                id = pool.zones_n, n = 0, list = {}
            }
        end

        -- append server to connection pool
        pool:connect((shards_n + 1) * redundancy - id + 1, server)
        local zone = pool.servers[server.zone]
        zone.list[zone.n].zone_name = server.zone

        -- TODO: functionality with the arbiter should be deleted in a future
        if not server.arbiter then
            shards[shards_n + 1] = shards[shards_n + 1] or {}
            table.insert(shards[shards_n + 1], zone.list[zone.n])
        end
    end
    shards_n = shards_n + 1
    -- start_resharding must be called outside of an append context in order
    -- to mitigate errors occured during append procedure on remote replicas
    return true
end

--[[
Start resharding process on each master in shards table
* this function must be called after successfull append
]]--
local function start_resharding()
    for _, shard in ipairs(shards) do
        local conn = shard[#shard].conn
        if conn then
            conn.space._shard:replace{RSD_STATE, 1}
        else
            return false, ("%s is unavailable"):format(shard[#shard].uri)
        end
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

local function get_server_list()
    return configuration.servers
end

local function shard_connect(s_obj)
    -- check if connection exists
    if s_obj:is_ready() then
        s_obj.state = 'connected'
        log.info("Return '%s' from maintenance", s_obj.uri)
        return true
    end
    -- try to join replica
    local ok, conn = net_connect(s_obj.uri, {
        user = s_obj.login,
        password = s_obj.password,
        timeout = 10,
        reconnect_after = RECONNECT_AFTER
    })
    if not ok then
        local msg = string.format(
            "Server '%s' is unavailable or does not exist (%s)",
            s_obj.uri, conn
        )
        log.info(msg)
        return false, msg
    end
    s_obj.conn = conn
    s_obj.state = 'connected'
    log.info("Succesfully joined shard %d with url '%s'", s_obj.id, s_obj.uri)
    return true
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
    for j, shard_set in ipairs(shards) do
        for i, shard in ipairs(shard_set) do
            if shard.id == id then
                return shard_connect(shard)
            end
        end
    end
    return false, string.format("There is no shard with id %d", id)
end

local function unjoin_shard(id)
    -- In maintenance mode shard is available
    -- but client will recive erorrs using this shard
    for _, replica_set in ipairs(shards) do
        for _, srv in ipairs(replica_set) do
            if srv.id == id then
                srv.state = 'is_dead_connected'
            end
        end
    end
    return true
end

-- default on join/unjoin event
local function on_action(self, msg)
    log.info(msg)
end

local function cluster_operation(func_name, ...)
    local remote_log = {}
    local q_args = {...}
    local all_ok = true

    -- we need to check shard id for direct operations
    local id = nil
    if type(q_args[1]) == 'number' and func_name ~= 'rotate_shard' then
        id = q_args[1]
    end

    -- requested function is called on each node of all shards
    -- there are 2 types of operations possible:
    -- 1) does not depend on shard id (append)
    -- 2) depends on shard id (join, unjoin)
    -- the last one requires to skip node which is being joined/unjoined
    -- that's why we check for its id before proceeding with operation
    for _, shard_set in ipairs(shards) do
        for i, shard in ipairs(shard_set) do
            if shard.id ~= id then
                if shard.conn ~= nil then
                    log.info(
                        "Trying to '%s' to shard %d in node %s",
                        func_name, shard.id, shard.uri
                    )
                    local ok, err = shard.conn:call(func_name, q_args)
                    if not ok then
                        local msg = string.format('"%s" error: %s', func_name, err)
                        table.insert(remote_log, msg)
                        log.error(msg)
                        all_ok = false
                    else
                        local msg = string.format(
                            "Operaion '%s' for shard %d in node '%s' applied",
                            func_name, shard.id, shard.uri
                        )
                        table.insert(remote_log, msg)
                        shard_obj:on_action(msg)
                    end
                else
                    local msg = string.format("Server '%s' is offline", shard.uri)
                    table.insert(remote_log, msg)
                end
            end
        end
    end
    return all_ok, remote_log
end

--[[
Current master is placed last in shard's table. In order to tranfer roles
among replicas, we have to manipulate with the shard's table and assign
penultimate node as the new master.
Old master is returned to the head of the table.
]]--
local function rotate_shard(shard_id)
    local master = table.remove(shards[shard_id])
    table.insert(shards[shard_id], 1, master)
    return true
end

--[[
Sync current node's shard table with remote replica.
zones -- simplified shard table, returned by get_zones() function
]]--
local function synchronize_shards_object(zones)
    for i, zone in ipairs(zones) do
        for j, srv in ipairs(zone) do
            if shards[i][j].uri ~= srv then
                rotate_shard(i)
            end
        end
    end
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

-- call rotate_shard on each replica in shards table
local function remote_rotate(shard_id)
    return cluster_operation("rotate_shard", shard_id)
end

-- the function executes an function under the passed space on the remote server
-- @space_name - name of the space in which an operation will be executed
-- @server - server object, where server.conn is the connpool object
-- @fun - the function which will be executed
-- @... - this args will be passed to the function
-- @return a result of the function, be aware that this result may be a nil,
-- or nil, error_object
local function space_call(self, space_name, server, fun, ...)
    local result = nil
    if server == nil or server.conn == nil then
        return make_error(nil, 'Connection to server was lost')
    end

    if fun == nil or type(fun) ~= 'function' then
        return make_error(nil, 'Argument should be a function')
    end

    local status, reason = pcall(function(...)
        local conn = server.conn:timeout(5 * REMOTE_TIMEOUT)
        local space_obj = conn.space[space_name]
        if space_obj == nil then
            conn:reload_schema()
            space_obj = conn.space[space_name]
        end
        result = fun(space_obj, ...)
    end, ...)
    if not status then
        return make_error(reason.code, 'failed to execute operation on %s: %s',
                          server.uri, reason)
    end
    return result
end

-- the function executes a tarantool operation under the passed space
-- on the remote server
-- @space_name - name of the space in which an operation will be executed
-- @server - server object, where server.conn is the connpool object
-- @operation - name of a tarantool operation, it may be select, insert, delete, etc
-- @... - this args will be passed to the operation
-- @return a result of the function, be aware that this result may be a nil,
-- or nil, error_object
local function single_call(self, space_name, server, operation, ...)
    return self:space_call(space_name, server, function(space_obj, ...)
        return space_obj[operation](space_obj, ...)
    end, ...)
end

-- the function executes a tarantool operation under the passed space and the
-- index passed by an index number or a name on the remote server
-- @space_name - name of the space in which an operation will be executed
-- @server - server object, where server.conn is the connpool object
-- @operation - name of a tarantool operation, it may be select, insert, delete, etc
-- @index_no - name or number of the index in the passed space
-- @... - this args will be passed to the operation
-- @return a result of the function, be aware that this result may be a nil,
-- or nil, error_object
local function index_call(self, space_name, server, operation,
                          index_no, ...)
    return self:space_call(space_name, server, function(space_obj, ...)
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

local function mr_select(self, space_name, nodes, index_id, limits, key)
    local results = {}
    local sort_index_id = sort_index_id or index_id
    local merge_obj = nil
    limits       = limits       or {}
    limits.limit = limits.limit or 1000
    for _, node in pairs(nodes) do
        local j = #node
        local srd = node[j]
        if merge_obj == nil then
            merge_obj = get_merger(srd.conn.space[space_name], index_id)
        end
        local buf = buffer.ibuf()
        limits.buffer = buf
        local part, err = index_call(self, space_name, srd, 'select', index_id,
                                     key, limits)
        if err then
            return nil, err
        end
        while part == nil and j >= 0 do
            j = j - 1
            srd = node[j]
            part, err = index_call(self, space_name, srd, 'select', index_id,
                                   key, limits)
            if err then
                return nil, err
            end
        end
        table.insert(results, buf)
    end
    merge_obj.start(results, 1)
    local tuples = {}
    while #tuples < limits.limit do
        local tuple = merge_obj.next()
        if tuple == nil then
            break
        end
        table.insert(tuples, tuple)
    end
    return tuples
end

local function secondary_select(self, space_name, index_id, limits, key)
    return mr_select(self, space_name, shards, index_id, limits, key)
end

-- load new schema and invalidate mergers (they hold index parts)
local function reload_schema()
    for _, zone in ipairs(shards) do
        for _, node in ipairs(zone) do
            node.conn:reload_schema()
        end
    end

    merger = {}
end

local function direct_call(self, server, func_name, ...)
    local result = nil
    local status, reason = pcall(function(...)
        local conn = server.conn:timeout(REMOTE_TIMEOUT)
        result = conn[nb_call](conn, func_name, ...)
    end, ...)
    if not status then
        log.error('failed to call %s on %s: %s', func_name, server.uri, reason)
        if not server:is_connected() then
            log.error("server %s is unavailable", server.uri)
        end
    end
    return result
end

-- function is a similar as shard function, but
-- it should be used if a cluster in a resharding state.
-- This function serches a data with a new shard function at first, then
-- it searches with an old shard function.
-- @space - space, where data is
-- @tuple_id - shard key
-- @... - index key
-- @returns shard, where data is
local function lookup(self, space, tuple_id, ...)
    local q_args = {...}
    local key = {}
    if #q_args > 0 then
        key = q_args[1]
    else
        return make_error(nil, 'Missing params for lookup function')
    end

    -- try to find in new shard
    local nodes, err = shard(tuple_id)
    if not nodes then
        return nil, err
    end
    local new_data, err = single_call(self, space, nodes[1], 'select', key)
    if not new_data then
        return nil, err
    end
    if #new_data > 0 then
        -- tuple transfer complete, we can use new shard for this request
        return nodes
    end

    local old_nodes = shard(tuple_id, false, true)
    local old_data, err = single_call(self, space, old_nodes[1], 'select', key)
    if not old_data then
        return nil, err
    end
    if #old_data == 0 then
        -- tuple not found in old shard:
        -- 1. it does not exists
        -- 2. space transfer complete and in moved to new shard
        -- Conclusion: we need to execute request in new shard
        return nodes
    end

    -- tuple was found let's check transfer state
    local result, err = index_call(self, '_shard', old_nodes[1], 'select', 3, key)
    if err then
        return nil, err
    end
    if #result == 0 then
        -- we must use old shard
        -- FIXME: probably prev iteration is finished
        -- and we need to check CUR_SPACE or _shard:len()
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
    local mode = box.space._shard:get('CUR_SPACE')
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
            result = box.space._shard.index[3]:get(key)
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

-- function makes requests to a cluster
-- @space - space name where a data is
-- @operation - name of operation. It is the same as tarantool space operation names.
-- @tuple_id - shard key
-- @returns result of operation or nil, {error = error_test, errno = error_code}
local function request(self, space, operation, tuple_id, ...)
    local nodes = {}
    local err
    if operation == 'insert' or not reshard_works() then
        nodes, err = shard(tuple_id)
    else
        -- check where is the tuple
        nodes, err = lookup(self, space, tuple_id, ...)
    end
    if not nodes then
        return nil, err
    end

    return single_call(self, space, nodes[1], operation, ...)
end

local function broadcast_select(task)
    local conn = task.server.conn
    local index = conn:timeout(REMOTE_TIMEOUT).space[task.space].index[task.index]
    local key = task.args[1]
    local offset =  task.args[2]
    local limit = task.args[3]
    local tuples = index:select(key, { offset = offset, limit = limit, timeout = REMOTE_TIMEOUT})
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
        local i = srv.conn.space[space].index[index]
        log.info('%s.space.%s.index.%s:select{%s, {offset = %s, limit = %s}',
            srv.uri, space, index, json.encode(key), offset, limit)
        local tuples = i:select(key, { offset = offset, limit = limit, timeout = REMOTE_TIMEOUT })
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
    local conn = task.server.conn:timeout(REMOTE_TIMEOUT)
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
    local tuple = box.space._shard_operations:update(
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
    box.space._shard_operations:update(operation_id, {{'=', 2, STATE_HANDLED}})
    box.commit()
end

-- process operation in queue
local function push_operation(task)
    local server = task.server
    local tuple = task.tuple
    log.debug('PUSH_OP')
    server.conn.space._shard_operations:insert(tuple,{timeout = REMOTE_TIMEOUT})
end

local function ack_operation(task)
    log.debug('ACK_OP')
    local server = task.server
    local operation_id = task.id
    local conn = server.conn
    conn[nb_call](
        conn, 'execute_operation', operation_id, {timeout = REMOTE_TIMEOUT}
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
    return box.space._shard_operations:get(id)[2]
end

local function check_operation(self, space, operation_id, tuple_id)
    local delay = 0.001
    operation_id = tostring(operation_id)
    for i = 1, 100 do
        local failed = nil
        local task_status = nil
        for _, server in pairs(shard(tuple_id)) do
            -- check that transaction is queued to all hosts
            local status, reason = pcall(function()
                local conn = server.conn
                task_status = conn[nb_call](
                    conn, 'find_operation', operation_id, {timeout = REMOTE_TIMEOUT}
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

local function truncate_local_space(space)
    local ok, err = pcall(box.space[space].truncate, box.space[space])
    if not ok then
        log.error('Error occured during a truncate of the space: %s %s', space, err)
    end

    return ok, err
end

local function truncate(self, space)
    local synchronizer_enabled = is_synchronizer_enabled()
    if reshard_works(synchronizer_enabled) then
        for _, node_set in ipairs(shards) do
            local master = node_set[#node_set]
            local ok, rv = pcall(master.conn.space._shard.get,
                                 master.conn.space._shard,
                                 {RSD_HANDLED})
            if not ok then
                return make_error(rv.code, 'Request to server: %s failed (%s)',
                                  master.uri, rv)
            end
            local handled_spaces = rv[2]
            if not contains(handled_spaces, space) then
                table.insert(handled_spaces, space)
                local ok, err = pcall(master.conn.space._shard.replace,
                                        master.conn.space._shard,
                                        {RSD_HANDLED, handled_spaces})
                if not ok then
                    return make_error(err.code, 'Request to server: %s failed (%s)',
                                      master.uri, err)
                end
            end
        end
    end

    for _, node_set in ipairs(shards) do
        local master = node_set[#node_set]
        local execute_string = string.format("return require('shard').truncate_local_space('%s')", space)
        local pcall_ok, ok, res = pcall(master.conn.eval, master.conn, execute_string)
        if not pcall_ok or not ok then
            local err = ok
            if not ok then
                err = res
            end
            return make_error(err.code, 'Error occured during a truncate of ' ..
                                        'the space %s on the server: %s. (%s)',
                              space, master.uri, err)
        end
    end

    return true
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

local function shard_init_v01()
    if box.space.operations == nil then
        local operations = box.schema.create_space('operations')
        operations:create_index('primary', {
            type  = 'hash',
            parts = { 1, 'str' }
        })
        operations:create_index('queue', {
            type  = 'tree',
            parts = { 2, 'num', 1, 'str' }
        })
    end
    if box.space.cluster_manager == nil then
        local cluster_ops = box.schema.create_space('cluster_manager')
        cluster_ops:create_index('primary', {
            type  = 'tree',
            parts = { 1, 'num' }
        })
        cluster_ops:create_index('status', {
            type   = 'tree',
            parts  = { 2, 'num' },
            unique = false
        })
    end
    if box.space.sh_worker == nil then
        local sh_space = box.schema.create_space('sh_worker')
        sh_space:create_index('primary', {
            type  = 'tree',
            parts = { 1, 'num' }
        })
        sh_space:create_index('status', {
            type   = 'tree',
            parts  = { 2, 'num' },
            unique = false
        })
    end
    if box.space.sh_worker_vinyl == nil then
        local sh_space = box.schema.create_space('sh_worker_vinyl', {
            engine = 'vinyl'
        })
        sh_space:create_index('primary', {
            type  = 'tree',
            parts = { 1, 'num' }
        })
        sh_space:create_index('status', {
            type   = 'tree',
            parts  = {2, 'num'},
            unique = false
        })
    end
    -- sharding manager settings
    if box.space.sharding == nil then
        local sharding = box.schema.create_space('sharding')
        sharding:create_index('primary', {
            type  = 'tree',
            parts = { 1, 'str' }
        })
    end
end

local function shard_init_v02()
    box.space.cluster_manager:drop()
end

local function shard_init_v03()
    -- renaming memtx spaces
    box.space.sharding:rename('_shard')
    box.space.sh_worker:rename('_shard_worker')
    box.space.operations:rename('_shard_operations')

    -- it's better to create new vinyl space, move everything to it and
    -- drop the old space
    local new_sh_space = box.schema.create_space('_shard_worker_vinyl', { engine = 'vinyl' })
    new_sh_space:create_index('primary', { type  = 'tree', parts = { 1, 'num' } })
    new_sh_space:create_index('status', { type = 'tree', parts = {2, 'num'}, unique = false })
    for record in box.space.sh_worker_vinyl:pairs() do
        new_sh_space:replace(record)
    end
    box.space.sh_worker_vinyl:drop()
end

local function init_create_spaces(cfg)
    box.once('shard_init_v01', shard_init_v01)
    box.once('shard_init_v02', shard_init_v02)
    box.once('shard_init_v03', shard_init_v03)
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
    shard_obj.reload_schema = reload_schema
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
    shard_obj.truncate = truncate
    shard_obj.truncate_local_space = truncate_local_space

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
                truncate = function(this, ...)
                    return self.truncate(self, space, ...)
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

local function on_connected(cfg)
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

-- background fiber function that signals when all shards are connected
local function establish_connections_in_pool()
    pool:wait_connection()
    on_connected(configuration)
    if configuration.monitor == true then
        pool:start_monitoring()
    end
    connection_fiber.state = 'connected'
    return true
end

-- init shard, connect with servers
local function init(cfg, callback)
    init_create_spaces(cfg)
    log.info('Sharding initialization started...')

    pool.REMOTE_TIMEOUT = shard_obj.REMOTE_TIMEOUT
    pool.HEARTBEAT_TIMEOUT = shard_obj.HEARTBEAT_TIMEOUT
    pool.DEAD_TIMEOUT = shard_obj.DEAD_TIMEOUT
    pool.RECONNECT_AFTER = shard_obj.RECONNECT_AFTER

    pool:init(cfg)
    connection_fiber.fiber = fiber.create(establish_connections_in_pool)

    return true
end

local function len(self)
    return self.shards_n
end

local function has_unfinished_operations()
    local tuple = box.space._shard_operations.index.queue:min()
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
_G.join_shard        = join_shard
_G.unjoin_shard      = unjoin_shard
_G.rotate_shard      = rotate_shard
_G.resharding_status = resharding_status
_G.remote_append     = remote_append
_G.remote_join       = remote_join
_G.remote_unjoin     = remote_unjoin
_G.remote_rotate     = remote_rotate
_G.drop_space        = drop_space
_G.drop_index        = drop_index

_G.find_operation    = find_operation
_G.transfer_wait     = transfer_wait

_G.cluster_operation = cluster_operation
_G.execute_operation = execute_operation
_G.force_transfer    = force_transfer
_G.merge_sort        = merge_sort
_G.shard_status      = shard_status
_G.get_server_list   = get_server_list
_G.synchronize_shards_object = synchronize_shards_object

_G.start_resharding  = start_resharding

-- get fiber id function
local function get_fiber_id(fiber)
    local fid = 0
    if fiber ~= nil and fiber:status() ~= "dead" then
        fid = fiber:id()
    end
    return fid
end

--[[
Following functions are used during initialization process of a new replica.
If we tranfer replica roles between nodes we also have to invoke corresponding
operations on each node. Resharding workers must be created on new master and
cancelled on the previous one.
]]--
local function enable_resharding(self)
    self.resharding_worker_fiber = fiber.create(resharding_worker, self)
    self.transfer_worker_fiber = fiber.create(transfer_worker, self)
    log.info('Enabled resharding workers')
end

local function disable_resharding(self)
    if (get_fiber_id(self.resharding_worker_fiber) ~= 0) then
        self.resharding_worker_fiber:cancel()
        while self.resharding_worker_fiber:status() ~= 'dead' do
            fiber.sleep(0.01)
        end
        self.resharding_worker_fiber = msgpack.NULL
        log.info('Disabled resharding worker')
    end
    if (get_fiber_id(self.transfer_worker_fiber) ~= 0) then
        self.transfer_worker_fiber:cancel()
        while self.transfer_worker_fiber:status() ~= 'dead' do
            fiber.sleep(0.01)
        end
        self.transfer_worker_fiber = msgpack.NULL
        log.info('Disabled transfer worker')
    end
end

shard_obj = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
    DEAD_TIMEOUT = DEAD_TIMEOUT,
    RECONNECT_AFTER = RECONNECT_AFTER,

    shards = shards,
    shards_n = shards_n,
    len = len,
    get_zones = get_zones,
    redundancy = redundancy,
    is_connected = is_connected,
    wait_for_shards_to_go_online = wait_for_shards_to_go_online,
    wait_operations = wait_operations,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,
    queue = queue,
    init = init,
    shard = shard,
    pool = pool,
    init_synchronizer = init_synchronizer,
    check_shard = check_shard,
    enable_resharding = enable_resharding,
    disable_resharding = disable_resharding
}

return shard_obj
-- vim: ts=4:sw=4:sts=4:et
