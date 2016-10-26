env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy1/master1.lua', lua_libs='redundancy1/lua/shard.lua'")
test_run:cmd("create server master2 with script='redundancy1/master2.lua', lua_libs='redundancy1/lua/shard.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
shard.wait_connection()

-- num keys
#shard.shard(0)

-- str keys
#shard.shard('abc')

_ = test_run:cmd("stop server master1")
_ = test_run:cmd("stop server master2")
test_run:cmd("cleanup server master1")
test_run:cmd("cleanup server master2")
test_run:cmd("restart server default with cleanup=1")
