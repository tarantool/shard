fiber = require('fiber')
env = require('test_run')
test_run = env.new()
test_run:create_shard_cluster(test_cluster, 'node_down')
test_run:wait_shard_fullmesh(test_cluster)

-- Ensure shard.cfg can not finish until all replica sets are
-- available.
-- Master 2 and master 3 must be started and stopped before
-- shard.cfg, because in only such way after shard.cfg is called
-- they can bootstrap with already correct schema.

test_run:cmd('stop server master2')
test_run:cmd('stop server master3')
f = fiber.create(function() shard.cfg(shard_cfg) end)
shard.status == nil
test_run:start_cluster(test_cluster[2])
test_run:wait_fullmesh(test_cluster[2])
while f:status() ~= 'dead' do fiber.sleep(0.1) end
-- Replica set 1 is available and shard.cfg is finished ok.
shard.replica_sets[1].master ~= nil

test_run:drop_shard_cluster(test_cluster)
