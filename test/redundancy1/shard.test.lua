env = require('test_run')
test_run = env.new()
servers = { 'master0', 'master1', 'master2' }
test_run:create_cluster(servers, 'redundancy1')
test_run:cmd('switch master0')
shard.wait_connection()

-- num keys
shard.server_by_key(0).uri

-- str keys
shard.server_by_key('abc').uri

test_run:cmd('switch default')
test_run:drop_cluster(servers)
