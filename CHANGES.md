# Changelog

## Version 2.1 (unstable)

Improved compatibility with shard-1.2 (consider PR #67 for details):

* Fixed undefined variables references to work under strict mode / debug
  tarantool build.
* Fixed a bug in connpool.wait_connection().
* Brought back shard.wait_connection() function.
* Brought back mr_select's sort_index_id parameter.

## Version 2.0 (unstable)

This version is not compatible with previous ones.

Requires `Tarantool 1.9` or higher.

### Breaking changes

* Return values for `*_call` functions, `secondary_select`, `shard()`, `lookup()`, `truncate()` has changed:
now they have mutlireturn `result`, `error`. If `result == nil` `error` will contain
either error object `{ errno, message }` or just `message`
* `append_shard` now does not require zones, they are retrieved from configs
* Resharding does not start on append. It is **required** to call `start_resharding` after successfull append
* Shard's `wait_connection()` is replaced by a new function `wait_for_shards_to_go_online(...)`
* Connection pool method `one` is renamed to `get_any_active_server`
* Connection pool method `all` is renamed to `get_all_active_servers`
* `check_connection` is removed

### API changes

#### New functions

* `start_resharding()` -- toggles flag that turns on resharding (global)
* `remote_rotate(shard_id)` -- calls `rotate_shard` on each cluster server one by one (global)
* `get_server_list()` -- returns list of servers from shard's configuration (global)

* `start_monitoring()` -- turns on fibers that monitor shards
* `init_synchronizer()` -- starts synchronizer fiber which checks resharding status
* `disable_resharding()` -- gracefully stops resharding fibers (pairs with `enable_resharding`)
* `rotate_shard(shard_id)` -- switch places of master and replica in the shard table
* `reload_schema()` -- load new schema and invalidate mergers
* `synchronize_shards_object(zones_object)` -- perform `rotate_shard` on particular shard if its entry in
shard table differs from zones_object
* `get_zones` -- return zones_object from server
* `truncate(self, space)` function allows to truncate space on the whole cluster
* `truncate_local_space(space)` -- perform truncate on a single server
* `wait_for_shards_to_go_online(timeout, delay)` -- blocks fiber until all shards are in `connected`
state. If `timeout` is reached, returns error.

New callbacks in connection pool:
* `on_connection_failure`
* `on_monitor_failure`
* `on_dead_disconnected`
* `on_dead_connected`

Following callbacks are used for failover:
* `on_server_fail`
* `on_server_return`

#### Reworked functions

* `append_shard(servers)` -- accepts only table of servers' uris

### New features

* Adds one more option: `rsd_max_tuple_transfer` which defines how many tuples
will be transfered in one operation (default: 1000)
* A lot of improvements for error handling
* `shard_status` gathers uuid of all servers in connection pool

* Implemented state-machine for server object (`server.state`). One of 5 states is possible:
    - connecting
    - connected
    - failed
    - is_dead_connected
    - is_dead_disconnected

### Bug Fixes

* Correct sorting order in `mr_select`

## Version 1.2 (stable)

Refer to `README.md` for details
