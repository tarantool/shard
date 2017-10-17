env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy3/master1.lua'")
test_run:cmd("create server master2 with script='redundancy3/master2.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
shard.wait_connection()

shard.demo:auto_increment{'test'}
shard.demo:auto_increment{'test2'}
shard.demo:auto_increment{'test3'}

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
