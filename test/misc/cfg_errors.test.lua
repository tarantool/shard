env = require('test_run')
test_run = env.new()
test_run:cmd("push filter '.*/*.lua.*:[0-9]+: ' to 'file.lua:<line>: '")
test_run:create_shard_cluster(test_cluster, 'misc')
test_run:wait_shard_fullmesh(test_cluster)

-- Too big redundancy. Real redundancy < specified.
shard.cfg(shard_cfg)

-- Try bad config.
cfg = {sharding = { {unknown = 100} }}
shard.cfg(cfg)

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
			[512] = {
				name = 'space1', id = 500, engine = 'memtx',
				field_count = 5, temporary = false,
				_format = {
					{type = 'string', name = 'field1'},
					{type = 'unsigned', name = 'field2'},
				},
				index = {
					[1] = {
						id = 0, name = 'pk', type = 'tree',
						parts = { {type = 'string', fieldno = 1} }
					},
					[2] = {
						id = 1, name = 'sk', type = 'tree',
						parts = { {type = 'unsigned', fieldno = 2} }
					}
				}
			},
		}
	},
};
test_run:cmd("setopt delimiter ''");
replica_sets[1].master = replica_sets[1][1]
replica_sets[2] = table.deepcopy(replica_sets[1])
replica_sets[2].master = replica_sets[2][1]
replica_sets[2][1].uri = 'localhost3314'
util = require('shard.config_util')
check_schema = util.check_schema
build_schema = util.build_schema
build_config = util.build_config
cfg = build_config({})

-- Ok, schemas are equal.
check_schema(replica_sets, cfg)
s = build_schema(replica_sets, cfg)
s.space1.index.pk.name
s.space1.index.sk.name

-- Different space count.
replica_sets[2][1].conn.space[513] = {'smoke weed every day'}
check_schema(replica_sets, cfg)

-- Space not found.
replica_sets[2][1].conn.space[513] = nil
replica_sets[1][1].conn.space[513] = table.deepcopy(replica_sets[1][1].conn.space[512])
replica_sets[1][1].conn.space[513].name = 'space2'
check_schema(replica_sets, cfg)

-- Different space attributes.
replica_sets[1][1].conn.space[513] = nil
space1 = replica_sets[1][1].conn.space[512]
space2 = replica_sets[2][1].conn.space[512]
space2.id = 600
check_schema(replica_sets, cfg)
space2.id = space1.id
space2.engine = 'vinyl'
check_schema(replica_sets, cfg)
space2.engine = space1.engine
space2.field_count = 10
check_schema(replica_sets, cfg)
space2.field_count = space1.field_count
space2.temporary = true
check_schema(replica_sets, cfg)
space2.temporary = space1.temporary

-- Different space formats.
space2._format[2] = nil
check_schema(replica_sets, cfg)
space2._format[2] = table.deepcopy(space1._format[2])
space2._format[2].name = 'new_name'
check_schema(replica_sets, cfg)
space2._format[2].name = space1._format[2].name
space2._format[2].type = 'scalar'
check_schema(replica_sets, cfg)
space2._format[2].type = space1._format[2].type

-- Different index count.
space2.index[3] = { name = 'sk2' }
check_schema(replica_sets, cfg)
space2.index[3] = nil

-- Index not found.
space1.index[3] = { name = 'sk2' }
check_schema(replica_sets, cfg)
space1.index[3] = nil

-- Different index attributes.
space2.index[2].id = 100
check_schema(replica_sets, cfg)
space2.index[2].id = space1.index[2].id
space2.index[2].type = 'hash'
check_schema(replica_sets, cfg)
space2.index[2].type = space1.index[2].type
space2.index[2].unique = false
check_schema(replica_sets, cfg)
space2.index[2].unique = space1.index[2].unique
space2.index[2].parts[2] = {'part2'}
check_schema(replica_sets, cfg)
space2.index[2].parts[2] = nil
space2.index[2].parts[1].type = 'scalar'
check_schema(replica_sets, cfg)
space2.index[2].parts[1].type = space1.index[2].parts[1].type
space2.index[2].parts[1].fieldno = 100
check_schema(replica_sets, cfg)
space2.index[2].parts[1].fieldno = space1.index[2].parts[1].fieldno

test_run:cmd('clear filter')
test_run:drop_shard_cluster(test_cluster)
