#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')
util = require('util')

local cfg = {
    servers = {
        { uri = util.instance_uri(0), zone = '0' },
        { uri = util.instance_uri(1), zone = '1' },
        { uri = util.instance_uri(2), zone = '2' },
    },
    login = 'tester',
    password = 'pass',
    redundancy = 1,
    binary = util.instance_port(util.INSTANCE_ID),
}

require('console').listen(os.getenv('ADMIN'))

box.cfg{ listen = cfg.binary }
util.create_replica_user(cfg)
local demo = box.schema.create_space('demo', {if_not_exists=true})
demo:create_index('primary', {if_not_exists=true})
local multipart = box.schema.create_space('multipart', {if_not_exists=true})
multipart:create_index('primary', {type = 'tree', parts = {1, 'num', 2, 'str'},
                                   if_not_exists=true})

fiber.create(function() shard.init(cfg) end)
