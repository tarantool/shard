#!/usr/bin/env tarantool
util = require('util')

require('console').listen(require('os').getenv('ADMIN'))

box.cfg {
    listen = util.instance_port(util.INSTANCE_ID),
    replication = {util.instance_uri(0), util.instance_uri(1)},
}
util.create_replica_user('tester', 'pass')
