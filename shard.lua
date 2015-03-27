#!/usr/bin/env tarantool

local fiber = require('fiber')
local log = require('log')
local digest = require('digest')
local msgpack = require('msgpack')
local remote = require('net.box')

local servers = {}
local servers_n
local redundancy = 3
local REMOTE_TIMEOUT = 500

-- errors handling
local function die(msg, ...)
    local err = string.format(msg, ...)
    log.error(err)
    error(err)
end

-- main shards search function
local function shard(key)
    local num = digest.crc32(key)
    local shards = {}
    local k = 1
    for i=1,redundancy do
        local zone = servers[tonumber(1 + (num + i) % servers_n)]
        local server = zone[1 + digest.guava(num, #zone)]
        -- ignore died servers
        if server.conn:is_connected() then
            shards[k] = server
            k = k + 1
        end
    end
    return shards
end

-- shards request function
local function request(space, operation, tuple_id, ...)
    result = nil
    local args = {...}
    for _, server in ipairs(shard(tuple_id)) do
        local status, reason = pcall(function()
            self = server.conn:timeout(5 * REMOTE_TIMEOUT).space[space]
			result = self[operation](self, unpack(args))
		end)
		if not status then
			log.error('failed to %s on %s: %s', operation, server.uri, reason)
			if not server.conn:is_connected() then
				log.error("server %s is offline", server.uri)
			end
		end
	end
	return result
end

-- default request wrappers for db operations
local function insert(space, data)
    tuple_id = data[1]
    return request(space, 'insert', tuple_id, data)
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

-- function for shard checking after init
local function check_shard(conn)
    return true
end

-- init shard, connect with servers
local function init(cfg, callback)
    log.info('establishing connection to cluster servers...')
    servers = {}
    local zones = {}
    for _, server in pairs(cfg.servers) do
        local conn
        log.info(' - %s - connecting...', server.uri)
        while true do
            conn = remote:new(cfg.login..':'..cfg.password..'@'..server.uri,
		{ reconnect_after = msgpack.NULL })
            conn:ping()
            if check_shard(conn) then
                local zone = zones[server.zone]
                if not zone then
                    zone = {}
                    zones[server.zone] = zone
                    table.insert(servers, zone)
                end
                if conn:eval('return box.info.server.uuid') == box.info.server.uuid then
                    -- detected self
                    log.info(" - self is %s:%s", conn.host, conn.port)
                    conn:close()
                    conn = remote.self
                    -- A workaround for #746
                    conn.is_connected = function() return true end
                end
                local srv = { uri = server.uri, conn = conn}
                -- there must be callback
                if callback ~= nil then
                    callback(srv)
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
    log.info('started')
    return true
end


local shard = {
    REMOTE_TIMEOUT = REMOTE_TIMEOUT,
    servers = servers, 
    servers_n = servers_n,
    redundancy = redundancy,
    
    request = request,
    init = init,
    check_shard = check_shard,
    insert = insert,
    select = select,
    replace = replace,
    update = update,
    delete = delete
}

setmetatable(shard, {
    __index = function(self, space)
        return {
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
           end
        }
    end
})

return shard
-- vim: ts=4:sw=4:sts=4:et
