shard = require('shard')
fiber = require('fiber')
require('console').listen(require('os').getenv('ADMIN'))

test_cluster = {{'master0'}, {'master1'}}

shard_cfg = {
    sharding = {
        { uri = 'tester:pass@localhost:33130', replica_set = '0' },
        { 'tester:pass@localhost:33131', 1 },
    },
    sharding_redundancy = 10,
}
box.cfg{}
