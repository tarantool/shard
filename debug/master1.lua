shard = require('shard')
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
shard.init(cfg)

--shard.queue_request('demo', 1, 'insert', {1, 'second', 'third'})
--shard.queue_request('demo', 2, 'insert', {2, 'second'})
--shard.queue_request('demo', 3, 'insert', {3, 'test'})
shard.demo.insert({1, 'second', 'third'})
shard.demo.insert({2, 'second'})
shard.demo.insert({3, 'test'})
shard.demo.replace({3, 'test2'})
shard.demo.update(3, {{'=', 2, 'test3'}})
require('fiber').sleep(3)
require('log').info(require('yaml').encode(box.space.demo:select{}))
