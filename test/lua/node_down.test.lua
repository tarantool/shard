--# create server master1 with script='lua/master1.lua', lua_libs='lua/lua/shard.lua'
--# create server master2 with script='lua/master2.lua', lua_libs='lua/lua/shard.lua'
--# start server master1
--# start server master2
--# set connection default
wait()

-- monitoring test
print_shard_map()

--# set connection master1
print_shard_map()

--# set connection master2
print_shard_map()

--# set connection default

-- Kill server and wait for monitoring fibers kill
--# stop server master1
wait()

print_shard_map()
--# set connection master2
print_shard_map()
--# set connection default


--# stop server master2
--# cleanup server master1
--# cleanup server master2
--# stop server default
--# start server default
--# set connection default
