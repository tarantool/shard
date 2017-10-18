# Tarantool sharding module
[![Tests status](https://travis-ci.org/tarantool/shard.svg?branch=master)](https://travis-ci.org/tarantool/shard)

An application-level library that provides sharding and client-side
reliable replication for [tarantool](http://tarantool.org). Implements
a single-phase and two-phase protocol operations (with batching
support), monitors availability of nodes and automatically expells
failed nodes from the cluster.

To shard data across nodes, a variant of consistent hashing is
used. The shard key is determined automatically based on sharded space
description.

## Installation
1. Add [tarantool repository](http://tarantool.org/download.html) for
   yum or apt
2. Install
```bash
$sudo [yum|apt-get] install tarantool tarantool-shard tarantool-pool
```

## Terminology

* redundancy - the redundancy factor. How many copies of each tuple to
  maintain in the cluster
* zone - a redundancy zone. May represent a single machine or a single
  data center. The number of zones must be greater or equal to the
  redundancy factor: duplicating data in the same zone doesn't
  increase availability

## Usage example

This example starts a tarantool instance that connects to itself,
creating a sharding configuration with a single zone and a single
server.

If you need more servers, add entries to the `servers` part of the
configuration. See "Configuration" below for details.

```lua
local shard = require('shard')
local json = require('json')

-- tarantool configuration
box.cfg {
    wal_mode = 'none',
    listen = 33021
}

box.schema.create_space('demo', {if_not_exists = true})
box.space.demo:create_index('primary',
                      {parts={1, 'unsigned'}, if_not_exists=true})

box.schema.user.grant('guest', 'read,write,execute',
                      'universe', nil, {if_not_exists = true})

box.schema.user.grant('guest', 'replication',
                      nil, nil, {if_not_exists = true})


-- sharding configuration
shard.init {
    servers = {
        { uri = 'localhost:33021', zone = '0' },
    },

    login = 'guest',
    password = '',
    redundancy = 1
}

shard.demo:insert({1, 'test'})
shard.demo:replace({1, 'test2'})
shard.demo:update({1}, {{'=', 2, 'test3'}})
shard.demo:insert({2, 'test4'})
shard.demo:insert({3, 'test5'})
shard.demo:delete({3})

print(json.encode(shard.demo:select({1})))
print(json.encode(shard.demo:select({2})))
```

## Testing

Sharding module can be tested with [tarantool functional testing framework](https://github.com/tarantool/test-run):
```bash
pip install -r test-run/requirements.txt
python test/test-run.py
```

## Configuration

```lua
cfg = {
    servers = {
        { uri = 'localhost:33130', zone = '0' },
        { uri = 'localhost:33131', zone = '1' },
        { uri = 'localhost:33132', zone = '2' }
    },
    login = 'tester',
    password = 'pass',
    monitor = true,
    pool_name = "default",
    redundancy = 3,
    rsd_max_rps = 1000,
    replication = true
}
```

Where:

* `servers`: a list of dictionaris {uri = '', zone = ''} that describe
  individual servers in the sharding configuration
* `login` and `password`: credentials that will be used to connect to
  `servers`
* `monitor`: whether to do active checks on the servers and remove
  them from sharding if they become unreachable (default `true`)
* `pool_name`: display name of the connection pool created for the
  group of `servers`. This only matters if you
  use [connpool](https://github.com/tarantool/connpool) module in
  parallel to the sharding module for other purposes. Otherwise you
  may skip this option. (default `'default'`)
* `redundancy`: How many copies of each tuple to maintain in the
  cluster. (defaults to number of zones)
* `replication`: Set to `true` if redundancy is handled by replication
  (default is `false`)

Timeout options are global, and can be set before calling the `init()`
funciton, like this:

```lua
shard = require 'shard'

local cfg = {...}

shard.REMOTE_TIMEOUT = 210
shard.HEARTBEAT_TIMEOUT = 500
shard.DEAD_TIMEOUT = 10
shard.RECONNECT_AFTER = 30

shard.init(cfg)
```

Where:

* `REMOTE_TIMEOUT` is a timeout in seconds for data access operations,
  like insert/update/delete. (default is `210`)
* `HEARTBEAT_TIMEOUT` is a timeout in seconds before a heartbeat call
  will fail. (default is `500`)
* `DEAD_TIMEOUT` is a timeout in seconds after which the
  non-responding node will be expelled from the cluster (default is
  10)
* `RECONNECT_AFTER` allows you to ignore transient failures in remote
  operations. Terminated connections will be re-established after a
  specified timeout in seconds. Under the hood, it uses the
  `reconnect_after` option for `net.box`. (disabled by default,
  i.e. `msgpack.NULL`)

## API

### Configuration and cluster management

#### `shard.init(cfg)`

Initialize sharding module, connect to all nodes and start monitoring them.

* cfg - sharding configuration (see Configuration above)

Note, that sharding configuration can be changed dynamically, and it
is your job to make sure that the changes get reflected in this
configuration. Because when you restart your cluster, the topology
will be read from whatever you pass to `init()`.

#### `shard.get_heartbeat()`

Returns status of the cluster from the point of view of each node.

Example output:

```yaml
---
- localhost:3302:
    localhost:3302: {'try': 0, 'ts': 1499270503.9233}
    localhost:3301: {'try': 0, 'ts': 1499270507.0284}
  localhost:3301:
    localhost:3302: {'try': 0, 'ts': 1499270504.9097}
    localhost:3301: {'try': 0, 'ts': 1499270506.8166}
...
```

#### `shard.is_table_filled()`

Returns `true` if the heartbeat table contains data about each node,
from the point of view of each other node. If the sharding module
hasn't yet filled in the heartbeats, or there are dead nodes, this
function will return `false`.

#### `shard.is_connected()`

Returns `true` if all shards are connected and operational.

#### `shard.wait_connection()`

Wait until all shards are connected and operational.

#### `shard_status()`

Returns the status of all shards: whether they are online, offline or
in maintenance.

Example output:

```yaml
---
- maintenance: []
  offline: []
  online:
  - uri: localhost:3301
    id: 1
  - uri: localhost:3302
    id: 2
  - uri: localhost:3303
    id: 3
...
```

#### `remote_append(servers)`

Appends a pair of redundant instances to the cluster, and initiates
resharding.

* `servers` - table of servers in the same format as in config

This function should be called on one node and will propagate changes
everywhere.

Example:

```lua
remote_append({{uri="localhost:3305", zone='2'},
               {uri="localhost:3306", zone='2'}})
```

Returns: `true` on success

#### `remote_join(id)`

If the node got expelled from the cluster, you may bring it back by
using `remote_join()`. It will reconnect to the node and allow write
access to it.

There are 2 reasons why it may happen: either the node has died, or
you've called `remote_unjoin()` on it.

Example:

```lua
remote_join(2)
```

Returns: `true` on success

#### `remote_unjoin(id)`

Put the node identified by `id` to maintenance mode. It will not
receive writes, and will not be returned by the `shard()` function.

### Operations

#### `shard.space.insert(tuple)`

Inserts `tuple` to the shard space.

`tuple[1]` is treated as shard key.

Returns: table with results of individual `insert()` calls on each
redundant node.

#### `shard.space.replace(tuple)`

Replaces `tuple` across the shard space.

`tuple[1]` is treated as shard key.

Returns: table with results of individual `replace()` calls on each
redundant node.

#### `shard.space.delete(key)`

Deletes tuples with primary key `key` across the shard space.

`key[1]` is treated as shard key.

Returns: table with results of individual `delete()` calls on each
redundant node.


#### `shard.space.update(key, {{operator, field_no, value}, ...})`

Update `tuple` across the shard space. Behaves the same way as Tarantool's [update()](http://tarantool.org/doc/book/box/box_space.html?highlight=insert#lua-function.space_object.update).

`key[1]` is treated as shard key.

Returns: table with results of individual `update()` calls on each
redundant node.


#### `shard.space.auto_increment(tuple)`

Inserts `tuple` to the shard space, automatically incrementing its primary key.

If primary key is numeric, `auto_increment()` will use the next integer number.
If primary key is string, `auto_increment()` will generate a new UUID.

Shard key is determined from the space schema, unlike the `insert()` operation.

Returns: table with results of individual `auto_increment()` calls on
each redundant node. Return value of each `auto_increment()` is the
same as in the `insert()` call.
