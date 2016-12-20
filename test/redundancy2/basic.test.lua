env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy2/master1.lua', lua_libs='redundancy2/lua/shard.lua'")
test_run:cmd("create server master2 with script='redundancy2/master2.lua', lua_libs='redundancy2/lua/shard.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
shard.wait_connection()

shard.demo:insert{1, 'test'}
shard.demo:replace{1, 'test2'}
shard.demo:update({1}, {{'=', 2, 'test3'}})
shard.demo:insert{2, 'test4'}
shard.demo:insert{3, 'test5'}
shard.demo:delete({3})

box.space.demo:select()
test_run:cmd("switch master1")
box.space.demo:select()
test_run:cmd("switch master2")
box.space.demo:select()
test_run:cmd("switch default")

_ = test_run:cmd("stop server master1")
_ = test_run:cmd("stop server master2")
test_run:cmd("cleanup server master1")
test_run:cmd("cleanup server master2")
test_run:cmd("restart server default with cleanup=1")
