--# create server master1 with script='redundancy1/master1.lua', lua_libs='redundancy1/lua/shard.lua'
--# create server master2 with script='redundancy1/master2.lua', lua_libs='redundancy1/lua/shard.lua'
--# start server master1
--# start server master2
--# set connection default
shard.wait_connection()

shard.demo.auto_increment{'test'}
shard.demo.auto_increment{'test2'}
shard.demo.auto_increment{'test3'}

shard.demo.q_auto_increment(1, {'test4'})
q = shard.q_begin()
shard.demo.q_auto_increment(2, {'test5'})
shard.demo.q_auto_increment(3, {'test6'})
shard.q_end(q)

--# set connection default
shard.wait_operations()
box.space.demo:select()
--# set connection master1
shard.wait_operations()
box.space.demo:select()
--# set connection master2
shard.wait_operations()
box.space.demo:select()
--# set connection default

box.space.operations:select()

--# stop server master1
--# stop server master2
--# cleanup server master1
--# cleanup server master2
--# stop server default
--# start server default
--# set connection default
