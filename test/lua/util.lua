local INSTANCE_ID = tonumber(string.match(arg[0], "%d"))

local function instance_port(instance_id)
	return 33130 + instance_id
end

local function instance_uri(instance_id)
    return "localhost:"..instance_port(instance_id)
end

local function create_replica_user(login, password)
	box.schema.user.create(login, { password = password, if_not_exists = true })
	box.schema.user.grant(login, 'read,write,execute', 'universe', nil, {if_not_exists=true})
	box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {if_not_exists=true})
end

return {INSTANCE_ID = INSTANCE_ID,
	instance_port = instance_port,
	instance_uri = instance_uri,
	create_replica_user = create_replica_user}
