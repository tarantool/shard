#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')
util = require('util')

local cfg = {
    servers = {
        { uri = util.instance_uri(0), replica_set = '0' },
        { uri = util.instance_uri(1), replica_set = '1' },
    },
    login = 'tester',
    password = 'pass',
    redundancy = 10,
    binary = util.instance_port(util.INSTANCE_ID),
}

require('console').listen(os.getenv('ADMIN'))

box.cfg {
    listen = cfg.binary,
    replication = {util.instance_uri(0), util.instance_uri(1)},
}
util.create_replica_user(cfg)
shard_init_status = nil
shard_init_err = nil
fiber.create(function()
    shard_init_status, shard_init_err = pcall(shard.init, cfg)
end)
