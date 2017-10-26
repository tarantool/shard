#!/usr/bin/env tarantool
util = require('util')

require('console').listen(require('os').getenv('ADMIN'))
require('log').warn(util.INSTANCE_ID)

local replication
if util.INSTANCE_ID == 0 then
	replication = nil
elseif util.INSTANCE_ID <= 2 then
	replication = {util.instance_uri(1), util.instance_uri(2)}
else
	replication = {util.instance_uri(3), util.instance_uri(4), util.instance_uri(5)}
end

box.cfg {
    listen = util.instance_port(util.INSTANCE_ID),
    replication = replication,
}
util.create_replica_user('tester', 'pass')

local demo = box.schema.create_space('demo', {if_not_exists = true})
demo:create_index('primary', {if_not_exists = true})
