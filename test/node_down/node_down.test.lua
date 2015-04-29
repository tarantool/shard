--# create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'
--# start server master1
--# set connection default
wait()

-- monitoring test
print_shard_map()

--# set connection master1
print_shard_map()

--# set connection default

-- Kill server and wait for monitoring fibers kill
--# stop server master1
wait()

print_shard_map()

--# cleanup server master1
--# stop server default
--# start server default
--# set connection default
