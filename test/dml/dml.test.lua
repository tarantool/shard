env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")
test_run:create_shard_cluster(test_cluster, 'dml')
test_run:wait_shard_fullmesh(test_cluster)
shard.cfg(shard_cfg)

-- Index only existing spaces.
shard.space.demo ~= nil
shard.space.demo2 == nil

-- Replica set 0.
shard.space.demo:insert{1, 1, 1}
shard.space.demo:insert{2, 2, 1}
shard.space.demo:insert{3, 3, 1}
-- Replica set 1.
shard.space.demo:replace{4, 4, 2}
shard.space.demo:replace{5, 5, 2}
shard.space.demo:replace{6, 6, 2}
-- Insert in random order.
shard.space.demo:replace{100, 200, 300}
shard.space.demo:replace{50, 300, 400}
shard.space.demo:replace{150, 100, 500}
shard.space.demo:replace{25, 400, 600}

test_run:cmd('switch master0')
box.space.demo:select{}
test_run:cmd('switch master1')
box.space.demo:select{}
test_run:cmd('switch master2')
box.space.demo:select{}
test_run:cmd('switch master3')
box.space.demo:select{}
test_run:cmd('switch default')

-- DML on the same replica set.
shard.space.demo:select{4}
-- Map-reduce selects.
shard.space.demo:select({4}, {iterator = 'GE'})
shard.space.demo:select{}
shard.space.demo:select()
shard.space.demo:select({4}, {limit = 3, iterator='LE'})
-- Select multiple tuples from not-unique indexes.
shard.space.demo.index.third:select{1}
shard.space.demo.index.third:select({2}, {iterator='GE'})
shard.space.demo.index.third:select({2}, {iterator='LT'})

shard.space.demo:get{5}
shard.space.demo:delete{6}
shard.space.demo:get{6}

-- DML to another replica set.
shard.space.demo:select{1}
shard.space.demo:get{2}
shard.space.demo:delete{3}
shard.space.demo:get{3}

test_run:cmd('switch master0')
box.space.demo:select{}
test_run:cmd('switch default')

-- Duplicate key.
shard.space.demo:insert{5, 5, 5}

-- Replace exising tuple.
shard.space.demo:replace{5, 5, 5, 'replaced'}
shard.space.demo:select{5}

-- Update.
shard.space.demo:update({5}, {{'=', 4, 'updated'}})
shard.space.demo:update({1}, {{'=', 4, 'updated'}})
shard.space.demo:select{1}
shard.space.demo:select{5}

-- Upsert.
shard.space.demo:upsert({6, 6, 6}, {})
shard.space.demo:upsert({2, 2, 2, 2}, {{'=', 4, 'upserted'}})
shard.space.demo:select{6}
shard.space.demo:select{2}

--
-- Test indexes.
--

shard.space.demo.index.primary ~= nil
shard.space.demo.index.secondary ~= nil

shard.space.demo.index.secondary:select({1}, {limit = 100})
shard.space.demo.index.secondary:select({5}, {limit = 100})

--
-- Test multipart keys.
--
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

shard.space.multipart:select{}
shard.space.multipart:select{1}
shard.space.multipart:select{1, 'a'}
shard.space.multipart:select({1, 'c'}, {iterator = 'GE'})
shard.space.multipart:select{2, 'b'}
-- Test too big key.
shard.space.multipart:select{2, 'b', 'c'}

test_run:cmd('switch master0')
box.space.multipart:select()
test_run:cmd("switch master1")
box.space.multipart:select()
test_run:cmd("switch master2")
box.space.multipart:select()
test_run:cmd('switch default')

-- Test call be key.
shard.call({1}, 'local_function', {'a', 'b', 'c'})
shard.call({5}, 'local_function', {'a', 'b', 'c'})

--
-- Test access unavailable replica set.
--
fiber = require('fiber')

test_run:cmd('stop server master0')
test_run:cmd('stop server master1')
shard.space.demo:select{2}
test_run:start_cluster(test_cluster[1])
shard.wait_connection()
shard.space.demo:select{2}

test_run:cmd('clear filter')
test_run:drop_shard_cluster(test_cluster)
