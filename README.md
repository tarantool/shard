Tarantool sharding module
=========================
[![Tests status](https://travis-ci.org/tarantool/shard.svg?branch=master)](https://travis-ci.org/tarantool/shard)

An application-level library that provides sharding and client-side reliable replication for [tarantool 1.6] (http://tarantool.org). Implements a single-phase and two-phase protocol operations (with batching support), monitors availability of nodes and automatically expells failed nodes from the cluster.

To shard data across nodes, a variant of consistent hashing is used. The shard key is determined automatically based on sharded space description.

The following describes functions of the library with examples of use. 

Install
-------
1. Add [tarantool repository](http://tarantool.org/download.html) for yum or apt
2. Install
```bash
$sudo [yum|apt-get] install tarantool tarantool-shard tarantool-pool
```

API
---
Terminology
* redundancy - the redundancy factor. How many copies of each tuple to maintain in the cluster
* zone - a redundancy zone. May represent a single machine or a single data center. The number of zones must
  be greater or equal to the redundancy factor: duplicating data in the same zone doesn't increase availability
* binary - the listen port of this node

###`init(cfg)`  
* cfg - sharding configuration

initialize all nodes, connect and check. Example of use:
```lua
shard = require('shard')
local cfg = {
    servers = {
        { uri = 'localhost:33130', zone = '0' };
        { uri = 'localhost:33131', zone = '1' };
        { uri = 'localhost:33132', zone = '2' };
    };
    login = 'tester';
    password = 'pass';
    redundancy = 3;
    binary = 33131;
}
shard.init(cfg)
```

###`check_shard(conn)`
* conn - tarantool net.box connection

The function to check node after a successful connection, called in `shard.init`, returns true if successful. Default always returns true. Example override:
```lua
shard.check_shard = function(conn)
    return conn.space.demo ~= nil
end
shard.init(cfg)
```

### `get_heartbeat()`
Returns sharding table for given node

Single-phase operations
-------------------
Supported operations: [insert](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.insert),  [update](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.update), [replace](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.replace), [delete](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.delete), [select](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.select), auto_increment
```lua
shard.demo:insert{1, 'test'}
shard.demo:replace{1, 'test2'}
shard.demo:update(1, {{'=', 2, 'test3'}})
shard.demo:insert{2, 'test4'}
shard.demo:insert{3, 'test5'}
shard.demo:delete(3)
```
###`select(...)`
Execute [select](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.select) is all shards - returns `table` with the results from all nodes
```lua
shard.demo:select{} 
```
###`auto_increment()`
`insert` with automatic tuple_id increment
```lua
shard.demo:auto_increment{'test'}
-- returns tuple like: [<ID>, 'test']
```

Two-phase operations
-------------------

Two phase operations work, well, in two phases. The first phase pushes the operation into an auxiliary space "operations" on all the involved shards, according to the redundancy factor. As soon as the operation
is propagated to the shards, a separate call triggers execution of the operation on all shards. If the 
caller dies before invoking the second phase, the shards figure out by themselves that the operation has been propagated and execute it anyway (it only takes a while, since the check is done only once in a period of time).
The operation id is necessary to avoid double execution of the same operation (at most once execution semantics)
and most be provided by the user. The status of the operation can always be checked, given its operation id, and
provided that it has not been pruned from operations space.

###`q_*(operation_id, ...)`
* operation_id - operation identifier (set by the user)

Supported operations: `q_insert`, `q_replace`, `q_update`, `q_delete`, `q_auto_increment`. The operation is divided into two phases: distribution and execution. In the two-phase operation, we wait only the distribution of data across nodes, operations were executed in queue.
```lua
shard.demo:q_insert(1, {0, 'test'})
shard.demo:q_replace(2, {0, 'test2'})
shard.demo:q_update(3, 0, {{'=', 2, 'test3'}})
shard.demo:q_insert(4, {1, 'test4'})
shard.demo:q_insert(5, {2, 'test_to_delete'})
shard.demo:q_delete(6, 2)
```
###`check_operation(operation_id, tuple_id)`
* operation_id - operation identifier (set by the user)
* tuple_id - tuple identifier
Function checks operation on all nodes, if the operation is not finished - waiting for its execution
```lua
shard.demo:check_operation(1, 0)
```

###`q_begin()|q_end()`
Returns batch object, supported operations: `q_insert`, `q_replace`, `q_update`, `q_delete`, `q_auto_increment`, `q_end`. All operations will be executed in one batch.
```lua
batch_obj = shard.q_begin()
batch_obj.demo:q_insert(1, {0, 'test'})
batch_obj.demo:q_replace(2, {0, 'test2'})
batch_obj.demo:q_update(3, 0, {{'=', 2, 'test3'}})
batch_obj.demo:q_insert(4, {1, 'test4'})
batch_obj.demo:q_insert(5, {2, 'test_to_delete'})
batch_obj.demo:q_delete(6, 2)
batch_obj:q_end()
```
Misc functions
--------------
###`is_connected()`
Returns true if all shards are connected
###`is_table_filled()`
Returns true if sharding table is full
###`get_epoch()`
Returns epoch(version) of sharding table
###`wait_connection()`
Wait while all shards are connected
###`wait_operations()`
Wait for all operations in node
###`wait_epoch(epoch)`
* epoch - number of epoch(version)

Wait for epoch start

Configuration
-------------
* REMOTE_TIMEOUT - timeout for shards call
* HEARTBEAT_TIMEOUT - timeout for heartbeat tick
* DEAD_TIMEOUT - number of falures before we close a connection with dead shard
* RECONNECT_AFTER - replace connect fiber with net.box reconnect_after (not recommended, by default=nil)

Test
----
Sharding module can be tested with [tarantool functional testing framework](https://github.com/tarantool/test-run):
```bash
pip install -r test-run/requirement.txt
python test/test-run.py
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
shard.demo:insert{1, 'test'}
shard.demo:replace{1, 'test2'}
shard.demo:update(1, {{'=', 2, 'test3'}})
shard.demo:insert{2, 'test4'}
shard.demo:insert{3, 'test5'}
shard.demo:delete(3)
```
See debug/ and test/ for full examples


