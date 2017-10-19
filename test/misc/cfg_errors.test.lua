env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")

-- Try bad redundancy.
servers = { 'master0', 'master1' }
test_run:create_cluster(servers, 'misc')
test_run:wait_fullmesh(servers)
test_run:cmd('switch master0')
shard_init_status
shard_init_err
test_run:cmd('switch master1')
shard_init_status
shard_init_err

-- Try bad config.
test_run:cmd('switch default')
cfg = {login = 'kek', password = 'kek', binary = 12345, servers = { {unknown = 100} }}
shard = require('shard')
shard.init(cfg)

--
-- Check inconsistent schema.
--
replica_sets = {}
replica_sets[1] = {}
test_run:cmd("setopt delimiter ';'")
replica_sets[1][1] = {
	uri = 'localhost3313',
	conn = {
		space = {
			space1 = {
				name = 'space1', id = 500, engine = 'memtx',
				field_count = 5, temporary = false,
				_format = {
					{type = 'string', name = 'field1'},
					{type = 'unsigned', name = 'field2'},
				},
				index = {
					pk = {
						id = 0, name = 'pk', type = 'tree',
						parts = { {type = 'string', fieldno = 1} }
					},
					sk = {
						id = 1, name = 'sk', type = 'tree',
						parts = { {type = 'unsigned', fieldno = 2} }
					}
				}
			},
		}
	},
};
test_run:cmd("setopt delimiter ''");
replica_sets[2] = table.deepcopy(replica_sets[1])
replica_sets[2][1].uri = 'localhost3314'
check_schema = require('shard.config_util').check_schema

-- Ok, schemas are equal.
check_schema(replica_sets)

-- Different space count.
replica_sets[2][1].conn.space.space2 = {'smoke weed every day'}
check_schema(replica_sets)

-- Space not found.
replica_sets[2][1].conn.space.space2 = nil
replica_sets[1][1].conn.space.space2 = table.deepcopy(replica_sets[1][1].conn.space.space1)
replica_sets[1][1].conn.space.space2.name = 'space2'
check_schema(replica_sets)

-- Different space attributes.
replica_sets[1][1].conn.space.space2 = nil
space1 = replica_sets[1][1].conn.space.space1
space2 = replica_sets[2][1].conn.space.space1
space2.id = 600
check_schema(replica_sets)
space2.id = space1.id
space2.engine = 'vinyl'
check_schema(replica_sets)
space2.engine = space1.engine
space2.field_count = 10
check_schema(replica_sets)
space2.field_count = space1.field_count
space2.temporary = true
check_schema(replica_sets)
space2.temporary = space1.temporary

-- Different space formats.
space2._format[2] = nil
check_schema(replica_sets)
space2._format[2] = table.deepcopy(space1._format[2])
space2._format[2].name = 'new_name'
check_schema(replica_sets)
space2._format[2].name = space1._format[2].name
space2._format[2].type = 'scalar'
check_schema(replica_sets)
space2._format[2].type = space1._format[2].type

-- Different index count.
space2.index.sk2 = {'sk2'}
check_schema(replica_sets)
space2.index.sk2 = nil

-- Index not found.
space1.index.sk2 = {'sk2'}
check_schema(replica_sets)
space1.index.sk2 = nil

-- Different index attributes.
space2.index.sk.id = 100
check_schema(replica_sets)
space2.index.sk.id = space1.index.sk.id
space2.index.sk.type = 'hash'
check_schema(replica_sets)
space2.index.sk.type = space1.index.sk.type
space2.index.sk.unique = false
check_schema(replica_sets)
space2.index.sk.unique = space1.index.sk.unique
space2.index.sk.parts[2] = {'part2'}
check_schema(replica_sets)
space2.index.sk.parts[2] = nil
space2.index.sk.parts[1].type = 'scalar'
check_schema(replica_sets)
space2.index.sk.parts[1].type = space1.index.sk.parts[1].type
space2.index.sk.parts[1].fieldno = 100
check_schema(replica_sets)
space2.index.sk.parts[1].fieldno = space1.index.sk.parts[1].fieldno

test_run:cmd('clear filter')
test_run:drop_cluster(servers)
