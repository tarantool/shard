env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1', 'master2' }
test_run:create_cluster(servers, 'redundancy3')
test_run:wait_fullmesh(servers)
test_run:cmd('switch master0')
shard.wait_connection()

-- num keys
#shard.shard(0)

-- str keys
#shard.shard('abc')

test_run:cmd('switch default')
test_run:drop_cluster(servers)
