#!/usr/bin/env tarantool
shard = require('shard')
os = require('os')
fiber = require('fiber')

local cfg = {
    servers = {
        { uri = 'localhost:33130', zone = '0' };
        { uri = 'localhost:33131', zone = '0' };
        { uri = 'localhost:33132', zone = '0' };
        { uri = 'localhost:33133', zone = '1' };
        { uri = 'localhost:33134', zone = '1' };
        { uri = 'localhost:33135', zone = '1' };
    };
    login = 'tester';
    password = 'pass';
    monitor = false;
    redundancy = 2;
    replication = true;
    binary = 33130;
}

box.cfg {
    slab_alloc_arena = 0.1;
    listen = cfg.binary;
    custom_proc_title  = "master"
}

function cluster(operation)
    for i=1, #cfg.servers - 1 do
        operation(tostring(i))
    end
end

require('console').listen(os.getenv('ADMIN'))

if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
	
    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'tree', parts = {1, 'num'}})
end

-- init shards
fiber.create(function()
    shard.init(cfg)
end)

