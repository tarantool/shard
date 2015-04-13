#!/usr/bin/env tarantool

shard = require('shard')
log = require('log')
yaml = require('yaml')

-- check that demo space exists
shard.check_shard = function(conn)
    return conn.space.demo ~= nil
end

local cfg = {
    -- shards config
    servers = {
        { uri = [[my-server.com:33020]]; zone = [[zone1]]};
    };
    -- shard login/password
    login = 'tester';
    password = 'pass';
    monitor = false;
    redundancy = 1;
    binary = 33021
}

-- tarantool configuration
box.cfg {
    slab_alloc_arena = 1;
    slab_alloc_factor = 1.06;
    slab_alloc_minimal = 16;
    wal_mode = 'none';
    listen = cfg.binary
}

if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')
	
    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'hash', parts = {1, 'str'}})
end

-- run sharing
shard.init(cfg)

--test shard insert
print('insert')
shard.demo.insert({0, 'test'})

--test shard select
print('select')
data = shard.demo.select(0)
print(yaml.encode(data))

print('replace')
shard.demo.replace({0, 'test2'})
data = shard.demo.select()
print(yaml.encode(data))

print('update')
shard.demo.update(0, {{'=', 2, 'test3'}})
result = shard.demo.select()
print(yaml.encode(result))

shard.demo.delete(0)

-- vim: ts=4:sw=4:sts=4:et
