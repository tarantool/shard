env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")

servers = { 'master0', 'master1', 'master2', 'master3' }
test_run:create_cluster(servers, 'dml')
test_run:wait_fullmesh({'master0', 'master1'})
test_run:wait_fullmesh({'master2', 'master3'})
test_run:cmd('switch master0')
shard.wait_connection()

-- Index only existing spaces.
shard.space.demo ~= nil
shard.space.demo2 == nil

-- Replica set 0.
shard.space.demo:insert{1, 1, 1}
shard.space.demo:insert{2, 2, 2}
shard.space.demo:insert{3, 3, 3}
-- Replica set 1.
shard.space.demo:replace{4, 4, 4}
shard.space.demo:replace{5, 5, 5}
shard.space.demo:replace{6, 6, 6}

box.space.demo:select{}
test_run:cmd('switch master1')
box.space.demo:select{}
test_run:cmd('switch master2')
box.space.demo:select{}
test_run:cmd('switch master3')
box.space.demo:select{}

-- DML on the same replica set.
shard.space.demo:select{4}
shard.space.demo:select({4}, {iterator = 'GE'})
shard.space.demo:select{} -- Unsupported.
shard.space.demo:select() -- Unsupported.
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

test_run:cmd("switch default")
test_run:cmd('clear filter')
test_run:drop_cluster(servers)
