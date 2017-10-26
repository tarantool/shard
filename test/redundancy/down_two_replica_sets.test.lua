env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")
test_run:create_shard_cluster(test_cluster, 'redundancy')
test_run:wait_shard_fullmesh(test_cluster)

shard.cfg(shard_cfg)
shard.status()

-- Down two replica sets at once.
shard.errinj.delay_replica_set_resurrection = true

test_run:drop_cluster(test_cluster[1])
test_run:drop_cluster(test_cluster[3])
shard.status()

test_run:start_cluster(test_cluster[1])
test_run:start_cluster(test_cluster[3])
test_run:wait_shard_fullmesh(test_cluster)

-- Replica set 1.
shard.space.demo:replace{1, 1}
shard.space.demo:replace{2, 2}
-- Replica set 3.
shard.space.demo:replace{3, 3}
-- Replica set 2. Only these tuples are ok. Other replica sets
-- are down.
shard.space.demo:replace{4, 4}
shard.space.demo:replace{5, 5}

shard.errinj.delay_replica_set_resurrection = false
shard.wait_connection()
shard.status()

shard.space.demo:select{}

test_run:drop_shard_cluster(test_cluster)
test_run:cmd('clear filter')
