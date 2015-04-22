Tarantool sharding module
=========================

Shading lua module for [tarantool 1.6](http://tarantool.org) with direct and queued operations. API:
* init - connect to all shards
* check_shard - check that instanse is correct after init
* shard - returns shards for given key
* get_heartbeat - returns last heartbeat table for all shards
* single_call - call opretarion for given space and server
* request - base function to execute database operations with shard

There are 2 execution protocols.
Direct operations:
* insert
* select
* replace
* update
* delete
* auto_increment

Queued operations:
* q_insert
* q_replace
* q_update
* q_delete
* q_auto_increment
* check_operation - return true if operation exists in needed shards

Database operations by default have an easy implementation.

Configuration
-------------
* REMOTE_TIMEOUT - timeout for shards call
* HEARTBEAT_TIMEOUT - timeout for heartbeat tick
* DEAD_TIMEOUT - number of falures before we close a connection with dead shard
* RECONNECT_AFTER - replace connect fiber with net.box reconnect_after (not recommended, by default=nil)
* WORKERS - number of queue workers

Install
-------
1. Add [tarantool repository](http://tarantool.org/download.html) for yum or apt
2. apt
```bash
$sudo apt-get install tarantool-shard
```
yum
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
See debug/ for full example


