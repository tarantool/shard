Tarantool sharding module
=========================

Shading lua module for tarantool 1.6. API:
* init() - connect to all shards
* check_shard() - check that instanse is correct after init
* request() - base function to execute database operations with shard
* insert
* select
* replace
* update
* delete

Database operations by default has easy implementation. 

Install
-------
1. Add tarantool repository for yum or apt
2. (apt)
```bash
$sudo apt-get install tarantool-shard
```
2. yum
```bash
$sudo yum install tarantool-shard
```

Example
-------
```lua
shard = require('shard')
shard.check_shard = function(conn)
    return conn.space.demo ~= nil
end

--sharding configuration
local cfg = {
    -- servers table
    servers = {
        { uri = [[my-server.com:33020]]; zone = [[zone1]]};
    };
    -- shard login/password
    login = 'tester';
    password = 'pass';
    
    redundancy = 3;
    binary = 33021
}

-- tarantool configuration
box.cfg {
    wal_mode = 'none';
    listen = cfg.binary
}

--some init ...

-- run sharing
shard.init(cfg)
shard.demo.insert({0, 'test'})
shard.demo.select(0)
shard.demo.replace({0, 'test2'})
shard.demo.update(0, {{'=', 2, 'test3'}})
shard.demo.delete(0)
```
See demo.lua for full example


