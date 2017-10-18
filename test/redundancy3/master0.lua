#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')
util = require('util')

local cfg = {
    servers = {
        { uri = util.instance_uri(0), replica_set = '0' },
        { uri = util.instance_uri(1), replica_set = '0' },
        { uri = util.instance_uri(2), replica_set = '0' },
    },
    login = 'tester',
    password = 'pass',
    redundancy = 3,
    binary = util.instance_port(util.INSTANCE_ID),
}

require('console').listen(os.getenv('ADMIN'))

box.cfg{
    listen = cfg.binary,
    replication = {
        util.instance_uri(0), util.instance_uri(1), util.instance_uri(2),
    },
}
util.create_replica_user(cfg)
if util.INSTANCE_ID == 0 then
    local demo = box.schema.create_space('demo', {if_not_exists=true})
    demo:create_index('primary', {if_not_exists=true})
else
    require('log').warn('waiting ...')
    while box.space.demo == nil or box.space.demo.index[0] == nil do
        fiber.sleep(0.01)
    end
    require('log').warn('end wait')
end

fiber.create(function() shard.init(cfg) end)
