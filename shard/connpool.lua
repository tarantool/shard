local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')

-- default values
local HEARTBEAT_TIMEOUT = 500
local DEAD_TIMEOUT = 5
local INFINITY_MIN = -1
local RECONNECT_AFTER = msgpack.NULL

local pool_table = {}

-- intentionally made global. this needs to be redone
-- heartbeat monitoring function
function heartbeat(pool_id)
    log.debug('ping to %s', pool_id)
    return pool_table[pool_id]
end

-- default callbacks

-- callbacks for usage in case of a failover
local function on_server_return(self, srv)
    log.warn("on_server_return is undefined")
    return nil
end
local function on_server_fail(self, srv)
    log.warn("on_server_fail is undefined")
    return nil
end

-- called when couldn't retrieve heartbeat
local function on_monitor_failure(self, srv)
    log.warn('- %s - connection failed', srv.uri)
end

-- called during first connection in case of an error
local function on_connection_failure(self, srv)
    srv.state = 'failed'
    log.warn('- %s - connection check failed', srv.uri)
end

-- on server connect
local function on_connected_one(self, srv)
    srv.state = 'connected'
    log.info(' - %s - connected', srv.uri)
end

-- called from wait_connection when all servers are connected
local function on_connected(self)
    log.info('connected to all servers')
end

-- on server disconnect
local function on_disconnect_one(self, srv)
    log.warn("kill %s by dead timeout", srv.uri)
end

-- called when the whole zone is disconnected
local function on_disconnect_zone(self, name)
    log.warn("zone %s has no active connections", name)
    log.error("Cluster is invalid. You must turn on read only mode")
end

-- called when each node in connection pool is disconnected
local function on_disconnect(self)
    log.error("there are no active connections")
end

-- actions to check for connpool's state on server disconnect
local function _on_disconnect(self, srv)
    self:on_disconnect_one(srv)

    -- check zone and pool
    local d = 0
    local all_zones = self:zone_list()
    for _, name in pairs(all_zones) do
        local alive = self:get_all_active_servers(name)
        if #alive == 0 then
            self:on_disconnect_zone(name)
            d = d + 1
        end
    end
    if d == #all_zones then
        self:on_disconnect()
    end
end

-- called when init is finished
-- connections may not be established at this point
local function on_init(self)
    self.init_complete = true
    log.info('started')
end

-- called when node is acknowleged dead by all other nodes in pool
local function on_dead_disconnected(self, srv)
    log.warn("Server %s is marked dead", srv.uri)
    srv.state = 'is_dead_disconnected'
    if self.on_server_fail then
        log.warn("Initializing failover procedure")
        self:on_server_fail(srv)
    end
    self.epoch_counter = self.epoch_counter + 1
    _on_disconnect(self, srv)
end

-- called when once proven dead node returns online
local function on_dead_connected(self, srv)
    srv.state = 'is_dead_connected'
    if self.on_server_return then
        log.warn("Initializing return procedure")
        self:on_server_return(srv)
    end
    log.warn("Server %s has returned and ready for maintenance", srv.uri)
end

local function server_is_ok(self, srv, include_dead)
    if include_dead then
        return true
    end
    return srv:is_ok()
end

local function merge_zones(self)
    local all_zones = {}
    local i = 1
    for _, zone in pairs(self.servers) do
        for _, server in pairs(zone.list) do
            all_zones[i] = server
            i = i + 1
        end
    end
    return all_zones
end

local function get_all_active_servers(self, zone_id, include_dead)
    local res = {}
    local k = 1
    local zone

    if zone_id ~= nil then
        zone = self.servers[zone_id]
    else
        zone = { list = self:merge_zones() }
    end

    for _, srv in pairs(zone.list) do
        if srv:is_connected() or include_dead then
            res[k] = srv
            k = k + 1
        end
    end
    return res
end

local function get_any_active_server(self, zone_id, include_dead)
    local active_list = self:get_all_active_servers(zone_id, include_dead)
    return active_list[math.random(#active_list)]
end

local function zone_list(self)
    local names = {}
    local i = 1
    for z_name, _ in pairs(self.servers) do
        names[i] = z_name
        i = i + 1
    end
    return names
end

local function monitor_fiber(self)
    fiber.name("_" .. self.configuration.pool_name .. "_monitor", { truncate = true })
    local i = 0
    while true do
        i = i + 1
        local server = self:get_any_active_server(nil, true)

        if not server:is_ok() and not server:is_dead() then
            local uri = server.uri
            log.debug("monitoring: %s", uri)
            local dead = true
            for k, v in pairs(self.heartbeat_state) do
                -- kill only if DEAD_TIMEOUT become in all servers
                if k ~= uri and v[uri].try < self.DEAD_TIMEOUT then
                    log.debug("%s is alive", uri)
                    dead = false
                    break
                end
            end

            if dead then
                self:on_dead_disconnected(server)
            end
        end
        fiber.sleep(math.random(1000)/1000)
    end
end

-- merge node response data with local table by fiber time
local function merge_tables(self, response)
    if response == nil then
        return
    end
    for seen_by_uri, old_beat in pairs(self.heartbeat_state) do
        local new_beat = response[seen_by_uri]
        if new_beat ~= nil then
            for uri, data in pairs(new_beat) do
                if data.ts > old_beat[uri].ts then
                    log.debug('merged heartbeat for uri %s from %s', uri, seen_by_uri)
                    old_beat[uri] = data
                end
            end
        end
    end
end

local function monitor_fail(self, uri)
    for _, zone in pairs(self.servers) do
        for _, server in pairs(zone.list) do
            if server.uri == uri then
                self:on_monitor_failure(server)
                break
            end
        end
    end
end

-- set or update opinions and timestamp in a heartbeat table
local function update_heartbeat(self, uri, response, status)
    -- local pool is represented via self_server
    -- for the remote pool (eg shards) each remote server represents itself
    local opinion = self.heartbeat_state[uri]
    if self.self_server then
        opinion = self.heartbeat_state[self.self_server.uri]
    end

    -- new nodes may be appended
    -- update table if we have recieved heartbeat from new node
    if opinion == nil then
        self.heartbeat_state[uri] = {}
        for srv, v in ipairs(self.heartbeat_state) do
            self.heartbeat_state[uri][srv] = {
                try = 0,
                ts  = INFINITY_MIN
            }
        end
        self.heartbeat_state[uri][uri] = {
            try = 0,
            ts  = INFINITY_MIN
        }
        opinion = self.heartbeat_state[uri]
    end

    if not status then
        -- register that an error occured during heartbeat
        opinion[uri].try = opinion[uri].try + 1
        self:monitor_fail(uri)
    else
        -- clear opinion since we have recieved heartbeat from server
        opinion[uri].try = 0
    end
    opinion[uri].ts = fiber.time()
    -- update local heartbeat table
    self:merge_tables(response)
end

-- heartbeat worker
local function heartbeat_fiber(self)
    fiber.name("_" .. self.configuration.pool_name .. "_heartbeat", { truncate = true })
    while true do
        -- random select node to check
        local server = self:get_any_active_server(nil, true)

        if server ~= nil then
            local uri = server.uri
            log.debug("checking %s", uri)

            if server:is_connected() then
                -- get heartbeat from node
                local response
                local status, err_state = pcall(function()
                    local expr = "return heartbeat('" .. self.configuration.pool_name .. "')"
                    response = server.conn:timeout(self.HEARTBEAT_TIMEOUT):eval(expr)
                end)
                -- update local heartbeat table
                self:update_heartbeat(uri, response, status)
                log.debug("%s", yaml.encode(self.heartbeat_state))
            else
                -- failed server's opinion marked useless
                for _, opinion in pairs(self.heartbeat_state[server.uri]) do
                    opinion.ts = fiber.time()
                    opinion.try = INFINITY_MIN
                end
                -- register failed attempt at heartbeat
                if self.self_server then
                    local opinion = self.heartbeat_state[self.self_server.uri][server.uri]
                    opinion.ts = fiber.time()
                    opinion.try = opinion.try + 1
                end
            end
        end
        -- randomized wait for next check
        fiber.sleep(math.random(1000)/1000)
    end
end

local function is_table_filled(self)
    local result = true
    for _, server in pairs(self.configuration.servers) do
        if self.heartbeat_state[server.uri] == nil then
            result = false
            break
        end
        for _, lserver in pairs(self.configuration.servers) do
            local srv = self.heartbeat_state[server.uri][lserver.uri]
            if srv == nil then
                result = false
                break
            end
        end
    end
    return result
end

local function wait_table_fill(self)
    while not self:is_table_filled() do
        fiber.sleep(0.01)
    end
end

local function fill_table(self)
    -- fill monitor table with start values
    for _, server in pairs(self.configuration.servers) do
        self.heartbeat_state[server.uri] = {}
        for _, lserver in pairs(self.configuration.servers) do
            self.heartbeat_state[server.uri][lserver.uri] = {
                try = 0,
                ts  = INFINITY_MIN,
            }
        end
    end
    pool_table[self.configuration.pool_name] = self.heartbeat_state
end

local function get_heartbeat(self)
    return self.heartbeat_state
end

local function enable_operations(self)
    -- set helpers
    self.get_heartbeat = self.get_heartbeat
end

local server_state_methods = {
    is_connected = function(self)
        return self.state == 'connected'
    end,
    is_dead = function(self)
        return self.state == 'is_dead_disconnected'
    end,
    is_ok = function(self)
        return self.conn ~= nil and self.conn:is_connected()
    end,
    is_ready = function(self)
        return self:is_ok() and self.state == 'is_dead_connected'
    end,
}

local function connect(self, id, server)
    -- filling server parameters
    local arbiter = server.arbiter or false
    local login = server.login or self.configuration.login
    local pass = server.password or self.configuration.password
    local uri = string.format("%s:%s@%s", login, pass, server.uri)

    -- server object user throughout connpool and shard
    local srv = {
        id       = id,
        uri      = server.uri,
        login    = login,
        arbiter  = arbiter,
        password = pass,
    }
    setmetatable(srv, { __index = server_state_methods })

    -- filling in zones (used to define different datacenters)
    local zone = self.servers[server.zone]
    zone.n = zone.n + 1
    zone.list[zone.n] = srv

    log.info(' - %s - connecting...', server.uri)
    while true do
        srv.state = 'connecting'
        local conn = remote:new(uri, { reconnect_after = self.RECONNECT_AFTER })
        if conn:ping() and conn.state == 'active' then
            srv.conn = conn
            if conn:eval("return box.info.server.uuid") == box.info.server.uuid then
                log.info("setting self_server to " .. server.uri)
                self.self_server = srv
            end
            break
        end
        conn:close()
        self:on_connection_failure(srv)
        fiber.sleep(math.random(1000)/1000)
    end
    self:on_connected_one(srv)
end

local function guardian_fiber(self)
    fiber.name("_" .. self.configuration.pool_name .. "_guardian", { truncate = true })
    while true do
        for _, zone in pairs(self.servers) do
            for _, server in pairs(zone.list) do
                if server:is_dead() then
                    local conn = remote:new(server.uri, {
                        user = server.login,
                        password = server.password,
                        reconnect_after = self.RECONNECT_AFTER
                    })
                    if conn:ping() and conn.state == 'active' then
                        server.conn = conn
                        server.conn_error = ""
                        self:on_dead_connected(server)
                    else
                        server.conn_error = conn.error
                    end
                end
            end
        end
        fiber.sleep(0.5)
    end
end

-- connect with servers
local function init(self, cfg)
    self.configuration = cfg
    -- check default pool name
    if self.configuration.pool_name == nil then
        self.configuration.pool_name = 'default'
    end

    self.servers_n = 0
    self.zones_n = 0

    log.info('establishing connection to cluster servers...')
    for id, server in pairs(cfg.servers) do
        self.servers_n = self.servers_n + 1
        local zone_name = server.zone or 'default'
        if self.servers[zone_name] == nil then
            self.zones_n = self.zones_n + 1
            self.servers[zone_name] = { id = self.zones_n, n = 0, list = {} }
        end
        fiber.create(self.connect, self, id, server)
    end

    self:fill_table()
    self:enable_operations()
    self:on_init()
    return true
end

local function start_monitoring(self)
    self.heartbeat_fiber = fiber.create(heartbeat_fiber, self)
    self.guardian_fiber = fiber.create(guardian_fiber, self)
    self.monitor_fiber = fiber.create(monitor_fiber, self)
end

local function len(self)
    return self.servers_n
end

local function is_connected(self)
    return self.init_complete
end

local function wait_connection(self)
    while true do
        local all_connected = true
        for _, zone in pairs(self.servers) do
            for _, srv in ipairs(zone.list) do
                if not srv:is_connected() then
                    all_connected = false
                end
            end
        end
        if all_connected then
            self:wait_table_fill()
            self:on_connected()
            return true
        end
        fiber.sleep(0.1)
        log.verbose("Retry checking connections")
    end
end

local function get_epoch(self)
    return self.epoch_counter
end

local function wait_epoch(self, epoch)
    while self:get_epoch() < epoch do
        fiber.sleep(0.01)
    end
end

local pool_object_methods = {
    server_is_ok = server_is_ok,
    merge_zones = merge_zones,
    merge_tables = merge_tables,
    monitor_fail = monitor_fail,
    update_heartbeat = update_heartbeat,
    connect = connect,
    fill_table = fill_table,
    enable_operations = enable_operations,

    len = len,
    is_connected = is_connected,
    wait_connection = wait_connection,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,

    -- public API
    init = init,
    get_any_active_server = get_any_active_server,
    get_all_active_servers = get_all_active_servers,
    zone_list = zone_list,
    get_heartbeat = get_heartbeat,
    -- background fibers
    start_monitoring = start_monitoring,
}

local function new()
    return setmetatable({
        servers = {},
        servers_n = 0,
        zones_n = 0,
        self_server = nil,
        heartbeat_state = {},
        init_complete = false,
        epoch_counter = 1,
        configuration = {},

        -- global constants
        HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT,
        DEAD_TIMEOUT = DEAD_TIMEOUT,
        RECONNECT_AFTER = RECONNECT_AFTER,

        -- callbacks available for set
        on_connected = on_connected,
        on_connected_one = on_connected_one,
        on_connection_failure = on_connection_failure,
        on_monitor_failure = on_monitor_failure,
        on_disconnect = on_disconnect,
        on_disconnect_one = on_disconnect_one,
        on_disconnect_zone = on_disconnect_zone,
        on_dead_disconnected = on_dead_disconnected,
        on_dead_connected = on_dead_connected,
        on_init = on_init,
        on_server_fail = on_server_fail,
        on_server_return = on_server_return
    }, {
        __index = pool_object_methods
    })
end

return {
    new = new
}
-- vim: ts=4:sw=4:sts=4:et
