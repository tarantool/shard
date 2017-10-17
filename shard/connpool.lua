local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')
local remote = require('net.box')
local yaml = require('yaml')

-- default values
local HEARTBEAT_TIMEOUT = 500
local DEAD_TIMEOUT = 10
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
local function on_connfail(self, srv)
    log.info('%s connection failed', srv.uri)
end

local function on_connected_one(self, srv)
    log.info(' - %s - connected', srv.uri)
end

local function on_connected(self)
    log.info('connected to all servers')
end

local function on_disconnect_one(self, srv)
    log.info("kill %s by dead timeout", srv.uri)
end

local function on_disconnect_zone(self, name)
    log.info("zone %s has no active connections", name)
end

local function on_disconnect(self)
    log.info("there is no active connections")
end

local function on_init(self)
    log.info('started')
end


local function server_is_ok(self, srv, dead)
    if srv.ignore then
        return false
    end

    if dead then
        return true
    end

    if srv.conn == nil then
        return false
    end

    return srv.conn:is_connected()
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

local function all(self, zone_id, include_dead)
    local res = {}
    local k = 1
    local zone

    if zone_id ~= nil then
        zone = self.servers[zone_id]
    else
        zone = { list=self:merge_zones() }
    end

    for _, srv in pairs(zone.list) do
        if self:server_is_ok(srv, include_dead) then
            res[k] = srv
            k = k + 1
        end
    end
    return res
end

local function one(self, zone_id, include_dead)
    local active_list = self:all(zone_id, include_dead)
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

local function _on_disconnect(self, srv)
    self:on_disconnect_one(srv)

    -- check zone and pool
    local d = 0
    local all_zones = self:zone_list()
    for _, name in pairs(all_zones) do
        local alive = self:all(name)
        if #alive == 0 then
            self:on_disconnect_zone(name)
            d = d + 1
        end
    end
    if d == #all_zones then
        self:on_disconnect()
    end
end

local function monitor_fiber(self)
    fiber.name("monitor")
    local i = 0
    while true do
        i = i + 1
        local server = self:one(nil, true)

        if server ~= nil then
            local uri = server.uri
            local dead = false
            for k, v in pairs(self.heartbeat_state) do
                -- true only if there is stuff in heartbeat_state
                if k ~= uri then
                    dead = true
                    log.debug("monitoring: %s", uri)
                    break
                end
            end
            for k, v in pairs(self.heartbeat_state) do
                -- kill only if DEAD_TIMEOUT become in all servers
                if k ~= uri and (v[uri] == nil or v[uri].try < self.DEAD_TIMEOUT) then
                    log.debug("%s is alive", uri)
                    dead = false
                    break
                end
            end
            if dead then
                server.conn:close()
                self.epoch_counter = self.epoch_counter + 1
                _on_disconnect(self, server)
            end
        end
        fiber.sleep(math.random(100)/1000)
    end
end

-- merge node response data with local table by fiber time
local function merge_tables(self, response)
    if response == nil then
        return
    end
    for seen_by_uri, node_data in pairs(self.heartbeat_state) do
        local node_table = response[seen_by_uri]
        if node_table ~= nil then
            for uri, data in pairs(node_table) do
                if data.ts > node_data[uri].ts then
                    log.debug('merged heartbeat from ' .. seen_by_uri .. ' with ' .. uri)
                    node_data[uri] = data
                end
            end
        end
    end
end

local function monitor_fail(self, uri)
    for _, zone in pairs(self.servers) do
        for _, server in pairs(zone.list) do
            if server.uri == uri then
                self:on_connfail(server)
                break
            end
        end
    end
end

-- heartbeat table and opinions management
local function update_heartbeat(self, uri, response, status)
    -- set or update opinions and timestamp
    if self.self_server == nil then
        return
    end

    local opinion = self.heartbeat_state[self.self_server.uri]
    if not status then
        opinion[uri].try = opinion[uri].try + 1
        self:monitor_fail(uri)
    else
        opinion[uri].try = 0
    end
    opinion[uri].ts = fiber.time()
    -- update local heartbeat table
    self:merge_tables(response)
end

-- heartbeat worker
local function heartbeat_fiber(self)
    fiber.name("heartbeat")
    local i = 0
    while true do
        i = i + 1
        -- random select node to check
        local server = self:one(nil, true)

        if server ~= nil then
            local uri = server.uri
            log.debug("checking %s", uri)

            if server.conn == nil then
                for _, opinion in pairs(self.heartbeat_state[server.uri]) do
                    opinion.ts = fiber.time()
                    opinion.try = INFINITY_MIN
                end

                if self.self_server then
                    self.heartbeat_state[self.self_server.uri][server.uri] = {
                        ts = fiber.time(), try = INFINITY_MIN}
                end
            else
                -- get heartbeat from node
                local response
                local status, err_state = pcall(function()
                        local expr = "return heartbeat('" .. self.configuration.pool_name .. "')"
                        response = server.conn:timeout(self.HEARTBEAT_TIMEOUT):eval(expr)
                end)
                -- update local heartbeat table
                self:update_heartbeat(uri, response, status)
                log.debug("%s", yaml.encode(self.heartbeat_state))
            end
        end
        -- randomized wait for next check
        fiber.sleep(math.random(1000)/1000)
    end
end

-- function to check a connection after it's established
local function check_connection(self, conn)
    return true
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

local function connect(self, id, server)
    local zone = self.servers[server.zone]
    log.info(' - %s - connecting...', server.uri)
    while true do
        local arbiter = server.arbiter or false
        local login = server.login
        local pass = server.password
        if login == nil or pass == nil then
            login = self.configuration.login
            pass = self.configuration.password
        end
        local uri = string.format("%s:%s@%s", login, pass, server.uri)
        local conn = remote:new(uri, { reconnect_after = self.RECONNECT_AFTER })
        if conn:ping() and self:check_connection(conn) then
            local srv = {
                uri = server.uri, conn = conn,
                login = login, password=pass,
                id = id, arbiter = arbiter
            }
            zone.n = zone.n + 1
            zone.list[zone.n] = srv
            self:on_connected_one(srv)
            if conn:eval("return box.info.server.uuid") == box.info.server.uuid then
                self.self_server = srv
            end
            break
        end
        conn:close()
        fiber.sleep(1)
    end
end

local function connection_fiber(self)
    while true do
        for _, zone in pairs(self.servers) do
            for _, server in pairs(zone.list) do

                if server.conn == nil or not server.conn:is_connected() then
                    local uri = ""

                    if server.password == "" then
                        uri = string.format("%s@%s", server.login, server.uri)
                    else
                        uri = string.format("%s:%s@%s", server.login, server.password, server.uri)
                    end

                    local conn = remote:new(uri, { reconnect_after = self.RECONNECT_AFTER })
                    if conn:ping() and self:check_connection(conn) then
                        server.conn = conn
                        server.conn_error = ""
                        log.debug("connected to: " .. server.uri)

                        if conn:eval("return box.info.server.uuid") == box.info.server.uuid then
                            log.info("setting self_server to " .. server.uri)
                            self.self_server = server
                        end
                    else
                        server.conn_error = conn.error
                    end
                end
            end
        end
        fiber.sleep(1)
    end
end

-- connect with servers
local function init(self, cfg)
    self.configuration = cfg
    -- check default pool name
    if self.configuration.pool_name == nil then
        self.configuration.pool_name = 'default'
    end
    log.info('establishing connection to cluster servers...')
    self.servers_n = 0
    self.zones_n = 0
    for id, server in pairs(cfg.servers) do
        self.servers_n = self.servers_n + 1
        local zone_name = server.zone
        if zone_name == nil then
            zone_name = 'default'
        end
        if self.servers[zone_name] == nil then
            self.zones_n = self.zones_n + 1
            self.servers[zone_name] = { id = self.zones_n, n = 0, list = {} }
        end
        local zone = self.servers[server.zone]

        local login = server.login
        local pass = server.password
        local arbiter = server.arbiter or false

        if login == nil or pass == nil then
            login = self.configuration.login
            pass = self.configuration.password
        end

        local srv = {
            uri = server.uri, conn = nil,
            login = login, password=pass,
            id = id, arbiter = arbiter
        }
        zone.n = zone.n + 1
        zone.list[zone.n] = srv
    end

    self:on_connected()
    self:fill_table()

    -- run monitoring and heartbeat fibers by default
    if cfg.monitor == nil or cfg.monitor then
        fiber.create(self.heartbeat_fiber, self)
        fiber.create(self.monitor_fiber, self)
    end
    fiber.create(self.connection_fiber, self)

    self:enable_operations()
    self.init_complete = true
    self:on_init()
    return true
end

local function len(self)
    return self.servers_n
end

local function is_connected(self)
    return self.init_complete
end

local function wait_connection(self)
    while not self:is_connected() do
        fiber.sleep(0.01)
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
    check_connection = check_connection,

    len = len,
    is_connected = is_connected,
    wait_connection = wait_connection,
    get_epoch = get_epoch,
    wait_epoch = wait_epoch,
    is_table_filled = is_table_filled,
    wait_table_fill = wait_table_fill,

    -- public API
    init = init,
    one = one,
    all = all,
    zone_list = zone_list,
    get_heartbeat = get_heartbeat,
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

        -- background fibers
        monitor_fiber = monitor_fiber,
        heartbeat_fiber = heartbeat_fiber,
        connection_fiber = connection_fiber,

        -- callbacks available for set
        on_connected = on_connected,
        on_connected_one = on_connected_one,
        on_disconnect = on_disconnect,
        on_disconnect_one = on_disconnect_one,
        on_disconnect_zone = on_disconnect_zone,
        on_init = on_init,
        on_connfail = on_connfail,
    }, {
        __index = pool_object_methods
    })
end

return {
    new = new
}
-- vim: ts=4:sw=4:sts=4:et
