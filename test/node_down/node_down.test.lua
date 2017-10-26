env = require('test_run')
test_run = env.new()
test_run:create_shard_cluster(test_cluster, 'node_down')
test_run:wait_shard_fullmesh(test_cluster)
shard.cfg(shard_cfg)
shard.status()

-- Kill master to raise a new master election.
_ = test_run:cmd("stop server master0")
shard.wait_epoch(2)
-- Wait connect to a new master.
shard.wait_connection()
shard.status()
_ = test_run:cmd("start server master0")
-- Noting changed. The master is still 'master1'.
shard.status()

-- Kill entire replica set.
_ = test_run:cmd("stop server master2")
shard.wait_epoch(3)
_ = test_run:cmd("stop server master3")
shard.wait_epoch(4)
shard.status()
shard.replica_sets[2].master == nil

-- Ensure the replica set is resurrected, when the replica set it
-- active again.
test_run:start_cluster(test_cluster[2])
test_run:wait_fullmesh(test_cluster[2])
shard.wait_connection()
shard.replica_sets[1].master ~= nil
shard.replica_sets[2].master ~= nil

test_run:drop_shard_cluster(test_cluster)
