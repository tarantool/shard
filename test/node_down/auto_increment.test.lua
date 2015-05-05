--# create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'
--# start server master1
--# set connection default
shard.wait_connection()

shard.demo:auto_increment{'test'}
shard.demo:auto_increment{'test2'}
shard.demo:auto_increment{'test3'}

--# stop server master1

shard.demo:q_auto_increment(1, {'test4'})
batch = shard.q_begin()
batch.demo:q_auto_increment(2, {'test5'})
batch.demo:q_auto_increment(3, {'test6'})
batch:q_end()

shard.wait_operations()
box.space.demo:select()
box.space.operations:select()

--# cleanup server master1
--# stop server default
--# start server default
--# set connection default
