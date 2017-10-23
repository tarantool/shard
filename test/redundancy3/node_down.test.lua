env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1', 'master2' }
test_run:create_cluster(servers, 'redundancy3')
test_run:wait_fullmesh(servers)
test_run:cmd('switch master0')
shard.wait_connection()

-- Kill server and wait for monitoring fibers kill
test_run:cmd("stop server master1")

-- Check that node is removed from shard
shard.wait_epoch(2)

test_run:cmd("switch master2")
shard.wait_epoch(2)

test_run:cmd('switch default')
test_run:drop_cluster({'master0', 'master2'})
