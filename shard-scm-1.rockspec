package = 'shard'
version = 'scm-1'
source  = {
    url    = 'git://github.com/tarantool/shard.git',
    branch = 'master',
}
description = {
    summary  = "Lua sharding for Tarantool",
    homepage = 'https://github.com/tarantool/shard.git',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1',
    'pool'
}
build = {
    type = 'builtin',

    modules = {
        ['shard'] = 'shard.lua'
    }
}

-- vim: syntax=lua
