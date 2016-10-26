env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'")
test_run:cmd("start server master1")
shard.wait_connection()
shard.wait_table_fill()
shard.is_table_filled()

-- Kill server and wait for monitoring fibers kill
_ = test_run:cmd("stop server master1")
shard.wait_epoch(2)
shard.is_table_filled()

test_run:cmd("cleanup server master1")
test_run:cmd("restart server default with cleanup=1")
