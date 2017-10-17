env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1', 'master2' }
test_run:create_cluster(servers, 'redundancy1')
test_run:cmd('switch master0')
shard.wait_connection()

-- monitoring test
shard.wait_table_fill()
shard.is_table_filled()

test_run:cmd("switch master1")
shard.wait_table_fill()
shard.is_table_filled()

test_run:cmd("switch master2")
shard.wait_table_fill()
shard.is_table_filled()

test_run:cmd('switch default')
test_run:drop_cluster(servers)
