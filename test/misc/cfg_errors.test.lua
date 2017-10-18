env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/init.lua.*:[0-9]+: ' to 'init.lua:<line>: '")

-- Try bad redundancy.
servers = { 'master0', 'master1' }
test_run:create_cluster(servers, 'misc')
test_run:wait_fullmesh(servers)
test_run:cmd('switch master0')
shard_init_status
shard_init_err
test_run:cmd('switch master1')
shard_init_status
shard_init_err

-- Try bad config.
test_run:cmd('switch default')
cfg = {login = 'kek', password = 'kek', binary = 12345, servers = { {unknown = 100} }}
shard = require('shard')
shard.init(cfg)

test_run:cmd('clear filter')
test_run:drop_cluster(servers)
