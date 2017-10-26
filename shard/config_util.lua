log = require('log')
local urilib = require('uri')

--
-- Check sharding configuration on basic options.
--
local function check_cfg(template, default, cfg)
    -- Set default options.
    for k, v in pairs(default) do
        if cfg[k] == nil then
            cfg[k] = default[k]
        end
    end
    -- Check specified options.
    for k, value in pairs(cfg) do
        if template[k] == nil then
            error("Unknown cfg option "..k)
        end
        if type(template[k]) == 'function' then
            template[k](value)
        elseif template[k] ~= type(value) then
            error("Incorrect type of cfg option "..k..": expected "..
                  template[k])
        end
    end
end

--
-- Template of each server in servers list.
--
local cfg_server_template = {
    uri = 'string',
    replica_set = function(value)
        return type(value) == 'string' or type(value) == 'number'
    end,
}

--
-- Template of a sharding configuration.
--
local cfg_template = {
    sharding = function(value)
        if type(value) ~= 'table' then
            error('Option "servers" must be table')
        end
        for _, server in ipairs(value) do
            if type(server) ~= 'table' then
                error('Each server must be table')
            end
            if server.uri == nil then
                server.uri = server[1]
                server[1] = nil
            end
            if server.replica_set == nil then
                server.replica_set = server[2]
                server[2] = nil
            end
            check_cfg(cfg_server_template, {}, server)
        end
    end,
    sharding_redundancy = 'number',
    sharding_timeout = 'number',
}

--
-- Default values for several sharding options.
--
local cfg_default = {
    sharding = {},
    sharding_redundancy = 2,
    sharding_timeout = 0.3,
}

--
-- Error messages for schema checking.
--
local err_field_count = 'Schema is inconsistent: format of a space %s has '..
                        'different field count on %s and %s'
local err_field_attr = 'Schema is inconsistent: field %d of a space %s has '..
                       'different "%s" on %s and %s'
local err_space_attr = 'Schema is inconsistent: space %s has different "%s" '..
                       'on %s and %s'
local err_index_attr = 'Schema is inconsistent: space %s has index %s with '..
                       'different "%s" on %s and %s'
local err_part_count = 'Schema is inconsistent: space %s has index %s with '..
                       'different part count on %s and %s'
local err_part_attr = 'Schema is inconsistent: space %s has index %s with '..
                      'different part %d - "%s" are different on %s and %s'
local err_index_count = 'Schema is inconsistent: space %s has different index'..
                        ' count on %s and %s'
local err_no_index = 'Schema is inconsistent: space %s has index %s on %s '..
                     'but has no on %s'
local err_no_space = 'Schema is inconsistent: %s has space %s, but %s does not'
local err_space_count = 'Schema is inconsistent: %s has different space count'..
                        ' with %s'

local function check_space_formats_are_equal(format1, uri1, format2, uri2, name)
    if #format1 ~= #format2 then
        error(string.format(err_field_count, name, uri1, uri2))
    end
    local function check_field(field1, field2, attr, fieldno)
        if field1[attr] ~= field2[attr] then
            error(string.format(err_field_attr, fieldno, name, attr,
                                uri1, uri2))
        end
    end
    for i, field1 in ipairs(format1) do
        local field2 = format2[i]
        check_field(field1, field2, 'name', i)
        check_field(field1, field2, 'type', i)
        check_field(field1, field2, 'collation', i)
        check_field(field1, field2, 'is_nullable', i)
    end
end

local function check_indexes_are_equal(index1, uri1, index2, uri2, name)
    -- Indexes are selected by name.
    assert(index1.name == index2.name)
    local function check_index_attr(attr)
        if index1[attr] ~= index2[attr] then
            error(string.format(err_index_attr, name, index1.name, attr,
                                uri1, uri2))
        end
    end
    local function check_part_attr(part1, part2, attr, partno)
        if part1[attr] ~= part2[attr] then
            error(string.format(err_part_attr, name, index1.name, partno, attr,
                                uri1, uri2))
        end
    end
    check_index_attr('id')
    check_index_attr('type')
    check_index_attr('unique')
    if #index1.parts ~= #index2.parts then
        error(string.format(err_part_count, name, index1.name, uri1, uri2))
    end
    for i, part1 in ipairs(index1.parts) do
        local part2 = index2.parts[i]
        check_part_attr(part1, part2, 'type', i)
        check_part_attr(part1, part2, 'fieldno', i)
        check_part_attr(part1, part2, 'collation', i)
        check_part_attr(part1, part2, 'is_nullable', i)
    end
end

local function build_indexes(space)
    local indexes = {}
    -- Iterate over identifiers.
    for id, index in pairs(space.index) do
        if type(id) == 'number' then
            local sharded_index = { name = index.name, id = id,
                                    space_id = space.id, parts = index.parts }
            indexes[index.name] = sharded_index
            indexes[id] = sharded_index
        end
    end
    return indexes
end

local function check_spaces_are_equal(space1, uri1, space2, uri2)
    -- Spaces are selected by the same name.
    assert(space1.name == space2.name)
    local function check_space_attr(attr)
        if space1[attr] ~= space2[attr] then
            error(string.format(err_space_attr, space1.name, attr, uri1, uri2))
        end
    end
    check_space_attr('id')
    check_space_attr('engine')
    check_space_attr('field_count')
    check_space_attr('temporary')
    check_space_formats_are_equal(space1._format, uri1, space2._format, uri2,
                                  space1.name)
    local space_index_count1 = 0
    -- Iterate over identifiers.
    for id, index1 in pairs(space1.index) do
        if type(id) == 'number' then
            space_index_count1 = space_index_count1 + 1
            local index2 = space2.index[id]
            if index2 == nil then
                error(string.format(err_no_index, space1.name, index1.name,
                                    uri1, uri2))
            end
            check_indexes_are_equal(index1, uri1, index2, uri1, space1.name)
        end
    end
    local space_index_count2 = 0
    for id, index2 in pairs(space2.index) do
        if type(id) == 'number' then
            space_index_count2 = space_index_count2 + 1
        end
    end
    if space_index_count1 ~= space_index_count2 then
        error(string.format(err_index_count, space1.name, uri1, uri2))
    end
end

local function check_schema(replica_sets, cfg)
    -- The algorithm is to get a one server from each replica set
    -- because on other servers the schema is the same.
    local server1 = replica_sets[1].master
    for i = 2, #replica_sets do
        local server2 = replica_sets[i].master
        assert(server2 ~= nil)
        local space_count1 = 0
        -- Iterate over identifiers.
        for id, space1 in pairs(server1.conn.space) do
            if type(id) == 'number' then
                space_count1 = space_count1 + 1
                local space2 = server2.conn.space[id]
                if space2 == nil then
                    error(string.format(err_no_space, server1.uri, space1.name,
                                        server2.uri))
                else
                    check_spaces_are_equal(space1, server1.uri, space2,
                                           server2.uri)
                end
            end
        end
        local space_count2 = 0
        for id, space2 in pairs(server2.conn.space) do
            if type(id) == 'number' then
                space_count2 = space_count2 + 1
            end
        end
        if space_count1 ~= space_count2 then
            error(string.format(err_space_count, server1.uri, server2.uri))
        end
    end
end

local function build_schema(replica_sets, cfg)
    local spaces = {}
    -- The algorithm is to get a one server from each replica set
    -- because on other servers the schema is the same.
    for _, replica_set in ipairs(replica_sets) do
        local server = replica_set.master
        assert(server ~= nil)
        -- Iterate over identifiers.
        for id, space in pairs(server.conn.space) do
            if type(id) == 'number' then
                local sharded_space = { name = space.name, id = id,
                                        index = build_indexes(space) }
                spaces[space.name] = sharded_space
                spaces[id] = sharded_space
            end
        end
    end
    return spaces
end

local function map_replica_sets(servers)
    local replica_sets_n = 0
    local replica_set_to_i = {}
    local replica_sets = {}
    for _, server in ipairs(servers) do
        local replica_set_i = replica_set_to_i[server.replica_set]
        if replica_set_i == nil then
            replica_sets_n = replica_sets_n + 1
            replica_set_i = replica_sets_n
            replica_set_to_i[server.replica_set] = replica_set_i
            replica_sets[replica_set_i] = { id = replica_set_i }
        end
        log.info('Adding %s to replica set %d', server.uri, replica_set_i)
        table.insert(replica_sets[replica_set_i], server)
    end
    log.info("Replica sets count = %d", replica_sets_n)
    return replica_sets
end

local function purge_password_from_uri(uri)
    local parsed = urilib.parse(uri)
    if parsed ~= nil and parsed.password ~= nil then
        return urilib.format(parsed, false)
    end
    return uri
end

local function serialize_server(server)
    -- Do not print access_uri - it contains password.
    return {
        uri = server.uri,
        replica_set = server.replica_set
    }
end

local function build_replica_sets(cfg)
    local replica_sets = map_replica_sets(cfg.sharding)

    -- Check that a requested redundancy is <= than real one.
    local min_redundancy = 999999999
    for _, replica_set in ipairs(replica_sets) do
        if min_redundancy > #replica_set then
            min_redundancy = #replica_set
        end
    end
    if min_redundancy < cfg.sharding_redundancy then
        local msg = string.format('Minimal redundancy found %s, but specified %s',
                                  min_redundancy, cfg.sharding_redundancy)
        log.error(msg)
        error(msg)
    end
    log.info("Redundancy = %d", min_redundancy)

    for _, replica_set in ipairs(replica_sets) do
        for _, server in ipairs(replica_set) do
            server.access_uri = server.uri
            -- Create an uri with no password to print it in
            -- statistics.
            server.uri = purge_password_from_uri(server.uri)
            setmetatable(server, { __serialize = serialize_server })
        end
    end

    return replica_sets
end

return {
    build_config = function(cfg)
        cfg = table.deepcopy(cfg)
        check_cfg(cfg_template, cfg_default, cfg)
        return cfg
    end,
    check_schema = check_schema,
    build_schema = build_schema,
    build_replica_sets = build_replica_sets,
}
