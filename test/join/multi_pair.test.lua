env = require('test_run')
test_run = env.new()

test_run:cmd("setopt delimiter ';'")
-- start shards
cluster(function(id)
    test_run:cmd("create server master"..id.." with script='join/master"..id..".lua'")
    test_run:cmd("start server master"..id)
end);
test_run:cmd("setopt delimiter ''");
shard.wait_connection()


for i=1, 10 do shard.demo:insert{i, 'test'} end

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
test_run:cmd("switch default")

-- stop 2 and 3 pairs
test_run:cmd("stop server master1")
test_run:cmd("stop server master4")
test_run:cmd("stop server master2")
test_run:cmd("stop server master5")
status = shard_status()
_ = remote_unjoin(status.offline[1].id)
_ = remote_unjoin(status.offline[2].id)
_ = remote_unjoin(status.offline[3].id)
_ = remote_unjoin(status.offline[4].id)
status = shard_status()
status

-- add tuples
result = shard.demo:insert{12, 'test_pair'}
result[1].status
result = shard.demo:insert{19, 'test_pair'}
result[1].status

-- start servers
test_run:cmd("start server master1")
test_run:cmd("start server master4")
test_run:cmd("start server master2")
test_run:cmd("start server master5")

_ = remote_join(status.offline[2].id)
_ = remote_join(status.offline[1].id)
_ = remote_join(status.offline[4].id)
_ = remote_join(status.offline[3].id)
shard_status()
shard.demo:insert{12, 'test_pair'}
shard.demo:insert{19, 'test_pair'}

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
-- cleanup
test_run:cmd("setopt delimiter ';'")
cluster(function(id)
    _ = test_run:cmd("stop server master"..id)
    test_run:cmd("cleanup server master"..id)
end);

test_run:cmd("setopt delimiter ''");
test_run:cmd("restart server default with cleanup=1")
