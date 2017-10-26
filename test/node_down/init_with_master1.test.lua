env = require('test_run')
test_run = env.new()
test_run:create_shard_cluster(test_cluster, 'node_down')
test_run:wait_shard_fullmesh(test_cluster)

-- Fail one of expected master nodes and start sharding. It must
-- start ok and elect another master.

test_run:cmd('stop server master0')
shard.cfg(shard_cfg)
shard.status()

test_run:drop_cluster({'master1', 'master2', 'master3'})
