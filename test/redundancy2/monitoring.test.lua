--# create server master1 with script='redundancy2/master1.lua', lua_libs='redundancy2/lua/shard.lua'
--# create server master2 with script='redundancy2/master2.lua', lua_libs='redundancy2/lua/shard.lua'
--# start server master1
--# start server master2
--# set connection default
shard.wait_connection()

-- monitoring test
shard.wait_epoch(3)
shard.is_table_filled()

--# set connection master1
shard.wait_epoch(3)
shard.is_table_filled()

--# set connection master2
shard.wait_epoch(3)
shard.is_table_filled()

--# set connection default

--# stop server master1
--# stop server master2
--# cleanup server master1
--# cleanup server master2
--# stop server default
--# start server default
--# set connection default
