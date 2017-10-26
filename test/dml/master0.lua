#!/usr/bin/env tarantool
util = require('util')

require('console').listen(require('os').getenv('ADMIN'))

local replication
if util.INSTANCE_ID == 0 or util.INSTANCE_ID == 1 then
    replication = {util.instance_uri(0), util.instance_uri(1)}
else
    replication = {util.instance_uri(2), util.instance_uri(3)}
end

box.cfg {
    listen = util.instance_port(util.INSTANCE_ID),
    replication = replication,
}
util.create_replica_user('tester', 'pass')

function local_function(args)
	return {args = args, instance = util.INSTANCE_ID}
end

local demo = box.schema.create_space('demo', {if_not_exists = true})
demo:create_index('primary', {if_not_exists = true})
demo:create_index('secondary', {parts = {2, 'unsigned'}, if_not_exists = true})
demo:create_index('third', {parts = {3, 'unsigned'}, unique = false, if_not_exists = true})

local multipart = box.schema.create_space('multipart', {if_not_exists=true})
multipart:create_index('primary', {parts = {1, 'unsigned', 2, 'string'}, if_not_exists=true})
