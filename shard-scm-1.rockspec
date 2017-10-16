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
}
build = {
    type = 'cmake';
    variables = {
        CMAKE_BUILD_TYPE="RelWithDebInfo";
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)";
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)";
    };
}

-- vim: syntax=lua
