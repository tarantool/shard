env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy1/master1.lua'")
test_run:cmd("create server master2 with script='redundancy1/master2.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
shard.wait_connection()

shard.demo:insert{1, 'test'}
shard.demo:replace{1, 'test2'}
shard.demo:update({1}, {{'=', 2, 'test3'}})
shard.demo:insert{2, 'test4'}
shard.demo:insert{3, 'test5'}
shard.demo:delete({3})

shard.demo2:replace{1, 2, 10}
shard.demo2:replace{2, 2, 20}
shard.demo2:replace{3, 2, 30}
shard.demo2:replace{4, 2, 40}
shard.demo2:replace{5, 2, 50}
shard.demo2:replace{6, 2, 60}
shard.demo2:replace{7, 2, 70}
shard.demo2:replace{8, 2, 80}
shard.demo2:replace{9, 2, 90}
shard.demo2:replace{10, 2, 100}

box.space.demo:select()
box.space.demo2:select()
test_run:cmd("switch master1")
box.space.demo:select()
box.space.demo2:select()
test_run:cmd("switch master2")
box.space.demo:select()
box.space.demo2:select()
test_run:cmd("switch default")

shard.demo2:secondary_select(1, {}, {2})
shard.demo2:secondary_select(1, {}, {2, 10})
shard.demo2:secondary_select(1, {}, {2, 200})
shard.demo2:secondary_select(1, {limit = 3}, {2, 80})

_ = test_run:cmd("stop server master1")
_ = test_run:cmd("stop server master2")
test_run:cmd("cleanup server master1")
test_run:cmd("cleanup server master2")
test_run:cmd("restart server default with cleanup=1")
