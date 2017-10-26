fiber = require('fiber')
env = require('test_run')
test_run = env.new()
test_run:create_shard_cluster(test_cluster, 'node_down')
test_run:wait_shard_fullmesh(test_cluster)

-- Ensure elections to be restarted on fail during initial
-- connection.

shard.errinj.delay_after_initial_master_search = true
f = fiber.create(function() shard.cfg(shard_cfg) end)
test_run:cmd('stop server master0')
shard.errinj.delay_after_initial_master_search = false
while f:status() ~= 'dead' do fiber.sleep(0.1) end
-- Master 0 failed during elections. Master 1 is the new master.
shard.wait_connection()
shard.status()

test_run:drop_cluster({'master1', 'master2', 'master3'})
