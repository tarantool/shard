--# create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'
--# start server master1
--# set connection default
shard.wait_connection()
shard.wait_epoch(2)
shard.is_table_filled()

-- Kill server and wait for monitoring fibers kill
--# stop server master1
shard.wait_epoch(3)

--# cleanup server master1
--# stop server default
--# start server default
--# set connection default
