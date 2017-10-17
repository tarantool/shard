#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')
util = require('util')

local cfg = {
    servers = {
        { uri = util.instance_uri(0), zone = '0' },
        { uri = util.instance_uri(1), zone = '1' },
    },
    login = 'tester',
    password = 'pass',
    redundancy = 2,
    binary = util.instance_port(util.INSTANCE_ID),
}

require('console').listen(os.getenv('ADMIN'))

box.cfg {
    listen = cfg.binary,
    replication = {util.instance_uri(0), util.instance_uri(1)},
}
util.create_replica_user(cfg)

if util.INSTANCE_ID == 0 then
    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'tree', parts = {1, 'unsigned'}})
else
    while box.space.demo == nil or box.space.demo.index[0] == nil do
        fiber.sleep(0.01)
    end
end

fiber.create(function() shard.init(cfg) end)
