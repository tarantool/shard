shard = require('shard')
log = require('log')
local cfg = {
    servers = {
        { uri = 'localhost:3313', zone = '0' };
--        { uri = 'build.tarantool.org:33020', zone = '1' };
        { uri = 'localhost:3314', zone = '1' };
    };
    http = 8080;
    login = 'tester';
    password = 'pass';
    redundancy = 2;
    binary = 3313;
    my_uri = 'localhost:3313'
}

box.cfg {
    slab_alloc_arena = 1.0;
    slab_alloc_factor = 1.06;
    slab_alloc_minimal = 16;
    wal_mode = 'none';
    logger = 'm1.log';
    log_level = 5;
    listen = cfg.binary;
}
if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')
	
    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'hash', parts = {1, 'str'}})
end
shard.init(cfg)
--shard.demo.insert({0, 'test'})
--shard.demo.delete(0)
