import os
import time
from lib.tarantool_server import TarantoolServer

# Deploy shard1
server1 = TarantoolServer(server.ini)
server1.script = 'multi1/master1.lua'
server1.vardir = os.path.join(server.vardir, 'master1')
server1.deploy()

# Deploy shard2
server2 = TarantoolServer(server.ini)
server2.script = 'multi1/master2.lua'
server2.vardir = os.path.join(server.vardir, 'master2')
server2.deploy()

# wait for shard cluster up and heartbeat table fill
for shard in [server, server1, server2]:
    shard.admin('wait()')

test_id = 10

#check shard function
server.admin('shard.shard(%d)[1].uri' % test_id)
server.admin('shard.shard("%d")[1].id' % test_id)

# heartbeat table for from first shard
server.admin('print_shard_map()')

# single phase operations
server.admin("shard.demo.insert({0, 'test'})")
server.admin("shard.demo.replace({0, 'test2'})")
server.admin("shard.demo.update(0, {{'=', 2, 'test3'}})")
server.admin("shard.demo.auto_increment{'test3'}")
server.admin("shard.demo.auto_increment{'test4'}")
server.admin("box.space.demo:select()")
server.admin("shard.demo.delete(0)")
server.admin("box.space.demo:select()")


# bipahse operations
server.admin("shard.demo.q_insert(1, {0, 'test'})")
server.admin("shard.demo.q_replace(2, {0, 'test2'})")
server.admin("shard.demo.q_update(3, 0, {{'=', 2, 'test3'}})")
server.admin("shard.demo.q_auto_increment(4, {'test3'})")

# check transactions with STR operation id
server.admin("shard.demo.q_auto_increment('5', {'test4'})")

# wait for queue and check results
for shard in [server, server1, server2]:
    shard.admin("fiber.sleep(1)")
    shard.admin("box.space.demo:select()")

# check for operation q_insert is in shard
server.admin("shard.demo.check_operation(1, 0)")

# check for not exists operations
server.admin("shard.demo.check_operation('12345', 0)")
