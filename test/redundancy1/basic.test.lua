env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1', 'master2' }
test_run:create_cluster(servers, 'redundancy1')
test_run:cmd('switch master0')
shard.wait_connection()

shard.space.demo:insert{1, 'test'}
shard.space.demo:replace{1, 'test2'}
shard.space.demo:update({1}, {{'=', 2, 'test3'}})
shard.space.demo:insert{2, 'test4'}
shard.space.demo:insert{3, 'test5'}
shard.space.demo:delete({3})


shard.space.multipart:insert{1, 'a', 4}
shard.space.multipart:insert{1, 'b', 5}
shard.space.multipart:insert{1, 'c', 6}
shard.space.multipart:insert{1, 'd', 7}
shard.space.multipart:insert{2, 'b', 1}
shard.space.multipart:insert{2, 'a', 2}
shard.space.multipart:replace{2, 'a', 3}
shard.space.multipart:replace{1, 'c', 8}
shard.space.multipart:delete{1, 'd'}
shard.space.multipart:update({1, 'c'}, {{'+', 3, 1}})

box.space.demo:select()
box.space.multipart:select()
test_run:cmd("switch master1")
box.space.demo:select()
box.space.multipart:select()
test_run:cmd("switch master2")
box.space.demo:select()
box.space.multipart:select()

--
-- Test shard call. It calls a function on a shard by a given
-- key.
--
shard.call(1, 'shard_call', {'argument1', 'argument2'}, {})
shard.call(4, 'shard_call', {'argument1', 'argument2'}, {})

test_run:cmd('switch default')
test_run:drop_cluster(servers)
