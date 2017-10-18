#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')
util = require('util')

local cfg = {
    servers = {
        { uri = util.instance_uri(0), replica_set = '0' },
        { uri = util.instance_uri(1), replica_set = '1' },
        { uri = util.instance_uri(2), replica_set = '2' },
        { uri = util.instance_uri(3), replica_set = '0' },
        { uri = util.instance_uri(4), replica_set = '1' },
        { uri = util.instance_uri(5), replica_set = '2' },
    },
    login = 'tester',
    password = 'pass',
    monitor = true,
    redundancy = 2,
    binary = util.instance_port(util.INSTANCE_ID),
}

require('console').listen(os.getenv('ADMIN'))

local replication = {}
if util.INSTANCE_ID <= 2 then
    replication = {
        util.instance_uri(util.INSTANCE_ID),
        util.instance_uri(util.INSTANCE_ID + 3)
    }
else
    replication = {
        util.instance_uri(util.INSTANCE_ID - 3),
        util.instance_uri(util.INSTANCE_ID)
    }
end

box.cfg {
    listen = cfg.binary,
    replication = replication,
}
util.create_replica_user(cfg)

if util.INSTANCE_ID <= 2 then
    local demo = box.schema.create_space('demo', {if_not_exists = true})
    demo:create_index('primary', {type = 'tree', parts = {1, 'unsigned'}, if_not_exists = true})
else
    while box.space.demo == nil or box.space.demo.index[0] == nil do
        fiber.sleep(0.01)
    end
end

fiber.create(function() shard.init(cfg) end)
