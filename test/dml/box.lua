shard = require('shard')
fiber = require('fiber')
require('console').listen(require('os').getenv('ADMIN'))

test_cluster = {
	{'master0', 'master1'}, {'master2', 'master3'}
}

shard_cfg = {
    sharding = {
        { uri = 'tester:pass@localhost:33130', replica_set = '0' },
        { uri = 'tester:pass@localhost:33131', replica_set = '0' },
        { uri = 'tester:pass@localhost:33132', replica_set = '1' },
        { uri = 'tester:pass@localhost:33133', replica_set = '1' },
    },
    sharding_redundancy = 2,
}
box.cfg{}
