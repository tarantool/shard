--# create server master1 with script='redundancy1/master1.lua', lua_libs='redundancy1/lua/shard.lua'
--# create server master2 with script='redundancy1/master2.lua', lua_libs='redundancy1/lua/shard.lua'
--# start server master1
--# start server master2
--# set connection default
shard.wait_connection()

-- num keys
#shard.shard(0)

-- str keys
#shard.shard('abc')

--# stop server master1
--# stop server master2
--# cleanup server master1
--# cleanup server master2
--# stop server default
--# start server default
--# set connection default
