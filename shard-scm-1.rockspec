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
    'lua >= 5.1';
    'connpool >= 1.1';
}
build = {
    type = 'cmake'
}

-- vim: syntax=lua
