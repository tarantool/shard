--# create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'
--# start server master1
--# set connection default
shard.wait_connection()

shard.demo.insert{1, 'test'}
shard.demo.replace{1, 'test2'}
shard.demo.update(1, {{'=', 2, 'test3'}})

--# stop server master1

shard.demo.insert{2, 'test4'}
shard.demo.insert{3, 'test5'};
shard.demo.delete(3)

box.space.demo:select()

--# cleanup server master1
--# stop server default
--# start server default
--# set connection default
