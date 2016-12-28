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
    redundancy = 2;
    monitor = false;
    replication = true;
    binary = 33135;
}

box.cfg {
    slab_alloc_arena = 0.1;
    listen = cfg.binary;
    custom_proc_title  = "replica";
    replication_source = "localhost:33132"
}

require('console').listen(os.getenv('ADMIN'))

-- init shards
fiber.create(function()
    shard.init(cfg)
end)

