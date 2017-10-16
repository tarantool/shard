env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy2/master1.lua'")
test_run:cmd("create server master2 with script='redundancy2/master2.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
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

test_run:cmd("switch default")

_ = test_run:cmd("stop server master1")
_ = test_run:cmd("stop server master2")
test_run:cmd("cleanup server master1")
test_run:cmd("cleanup server master2")
test_run:cmd("restart server default with cleanup=1")
