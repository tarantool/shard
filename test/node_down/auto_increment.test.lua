env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1' }
test_run:create_cluster(servers, 'node_down')
test_run:wait_fullmesh(servers)
test_run:cmd('switch master0')
shard.wait_connection()

shard.demo:auto_increment{'test'}
shard.demo:auto_increment{'test2'}
shard.demo:auto_increment{'test3'}

_ = test_run:cmd("stop server master1")

box.space.demo:select()

test_run:cmd('switch default')
test_run:drop_cluster({'master0'})
