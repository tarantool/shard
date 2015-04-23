#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')

local cfg = {
    servers = {
        { uri = 'localhost:33130', zone = '0' };
        { uri = 'localhost:33131', zone = '0' };
        { uri = 'localhost:33132', zone = '2' };
    };
    login = 'tester';
    password = 'pass';
    redundancy = 2;
    binary = 33132;
    my_uri = 'localhost:33132'
}

box.cfg {
    slab_alloc_arena = 0.1;
    wal_mode = 'none';
    listen = cfg.binary;
    pid_file  = "tarantool.pid";
    logger  = "tarantool.log";
    custom_proc_title  = "master2";
}

require('console').listen(os.getenv('ADMIN'))

if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')
	
    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'hash', parts = {1, 'num'}})

    local operations = box.schema.create_space('operations')
    operations:create_index('primary', {type = 'hash', parts = {1, 'str'}})
end

function print_shard_map()
    local result = {}
    for uri, hb_table in pairs(shard.get_heartbeat()) do
        table.insert(result, uri)
        for server, data in pairs(hb_table) do
            table.insert(result, server)
            table.insert(result, data.try)
        end
    end
    return result
end

function wait()
    fiber.sleep(5)
    return true
end


-- init shards
fiber.create(function()
    shard.init(cfg)
end)

