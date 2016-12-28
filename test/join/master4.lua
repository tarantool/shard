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
    binary = 33134;
}

box.cfg {
    slab_alloc_arena = 0.1;
    listen = cfg.binary;
    custom_proc_title  = "replica";
    replication_source="localhost:33131";
}

require('console').listen(os.getenv('ADMIN'))

-- init shards
fiber.create(function()
    shard.init(cfg)
end)

