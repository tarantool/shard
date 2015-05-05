shard = require('shard')
log = require('log')
yaml = require('yaml')

local cfg = {
    servers = {
        { uri = 'localhost:3313', zone = '0' };
        { uri = 'localhost:3314', zone = '0' };
    };
    http = 8080;
    login = 'tester';
    password = 'pass';
    redundancy = 1;
    binary = 3314;
    my_uri = 'localhost:3314'
}

box.cfg {
    slab_alloc_arena = 1.0;
    slab_alloc_factor = 1.06;
    slab_alloc_minimal = 16;
    wal_mode = 'none';
    logger = 'm2.log';
    log_level = 5;
    work_dir='work';
    listen = cfg.binary;
}
if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')

    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'hash', parts = {1, 'num'}})
end

-- init shards
shard.init(cfg)

-- do inser, replace, update operations
shard.demo:q_auto_increment(1, {'second', 'third'})
shard.demo:q_auto_increment(2, {'second'})
test_id = shard.demo:q_auto_increment(3, {'test'})[1]
shard.demo:q_replace(4, {test_id, 'test2'})
shard.demo:q_update(5, test_id, {{'=', 2, 'test3'}})
shard.demo:q_auto_increment(6, {'test_incr'})

--batching
batch = shard.q_begin()
batch.demo:q_auto_increment(7, {'batch1'})
batch.demo:q_auto_increment(8, {'batch2'})
batch.demo:q_auto_increment(9, {'batch3'})
batch:q_end()

-- wait and show results
require('fiber').sleep(3)
log.info(yaml.encode(box.space.demo:select{}))
log.info(yaml.encode(shard.demo.check_operation(4, test_id)))
