env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")
servers = { 'master0', 'master1', 'master2', 'master3', 'master4', 'master5' }
test_run:create_cluster(servers, 'join')
test_run:wait_fullmesh({'master0', 'master3'})
test_run:wait_fullmesh({'master1', 'master4'})
test_run:wait_fullmesh({'master2', 'master5'})
test_run:cmd('switch master0')
shard.wait_connection()

for i=1, 10 do shard.space.demo:insert{i, 'test'} end

-- check data
box.space.demo:select()
test_run:cmd("switch master3")
box.space.demo:select()

test_run:cmd("switch master1")
box.space.demo:select()
test_run:cmd("switch master4")
box.space.demo:select()

test_run:cmd("switch master2")
box.space.demo:select()
test_run:cmd("switch master5")
box.space.demo:select()
test_run:cmd("switch master0")

-- stop 2 and 3 pairs
test_run:cmd("stop server master1")
test_run:cmd("stop server master4")
test_run:cmd("stop server master2")
test_run:cmd("stop server master5")
status = shard.status()
_ = remote_unjoin(status.offline[1].id)
_ = remote_unjoin(status.offline[2].id)
_ = remote_unjoin(status.offline[3].id)
_ = remote_unjoin(status.offline[4].id)
status = shard.status()
status

-- add tuples
shard.space.demo:insert{12, 'test_pair'}
shard.space.demo:insert{19, 'test_pair'}

-- start servers
test_run:cmd("switch master0")
test_run:cmd("start server master1 with wait_load=False, wait=False")
test_run:cmd("start server master4 with wait_load=False, wait=False")
test_run:cmd("start server master2 with wait_load=False, wait=False")
test_run:cmd("start server master5 with wait_load=False, wait=False")
test_run:wait_fullmesh({'master1', 'master4'})
test_run:wait_fullmesh({'master2', 'master5'})
_ = remote_join(status.offline[2].id)
_ = remote_join(status.offline[1].id)
_ = remote_join(status.offline[4].id)
_ = remote_join(status.offline[3].id)
shard.status()
shard.space.demo:insert{12, 'test_pair'}
shard.space.demo:insert{19, 'test_pair'}

-- check joined replica
box.space.demo:select()
test_run:cmd("switch master3")
box.space.demo:select()

test_run:cmd("switch master1")
box.space.demo:select()
test_run:cmd("switch master4")
box.space.demo:select()

test_run:cmd("switch master2")
box.space.demo:select()
test_run:cmd("switch master5")
box.space.demo:select()
test_run:cmd("switch default")
test_run:drop_cluster(servers)
test_run:cmd('clear filter')
