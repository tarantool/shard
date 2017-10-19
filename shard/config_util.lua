--
-- Check sharding configuration on basic options.
--
local function check_cfg(template, default, cfg)
    cfg = table.deepcopy(cfg)
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
    return cfg
end

--
-- Template of each server in servers list.
--
local cfg_server_template = {
    uri = 'string',
    replica_set = 'string',
}

--
-- Template of a sharding configuration.
--
local cfg_template = {
    servers = function(value)
        if type(value) ~= 'table' then
            error('Option "servers" must be table')
        end
        for _, server in ipairs(value) do
            if type(server) ~= 'table' then
                error('Each server must be table')
            end
            check_cfg(cfg_server_template, {}, server)
        end
    end,
    login = 'string',
    password = 'string',
    monitor = 'boolean',
    pool_name = 'string',
    redundancy = 'number',
    rsd_max_rps = 'number',
    binary = 'number'
}

--
-- Default values for several sharding options.
--
local cfg_default = {
    servers = {},
    monitor = true,
    pool_name = 'sharding_pool',
    redundancy = 2,
    rsd_max_rps = 1000,
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
    for name, index1 in pairs(space1.index) do
        space_index_count1 = space_index_count1 + 1
        local index2 = space2.index[name]
        if index2 == nil then
            error(string.format(err_no_index, space1.name, name, uri1, uri2))
        end
        check_indexes_are_equal(index1, uri1, index2, uri1, space1.name)
    end
    local space_index_count2 = 0
    for name, index2 in pairs(space2.index) do
        space_index_count2 = space_index_count2 + 1
    end
    if space_index_count1 ~= space_index_count2 then
        error(string.format(err_index_count, space1.name, uri1, uri2))
    end
end

local function check_schema(replica_sets)
    -- The algorithm is to get a one server from each replica set
    -- because on other servers the schema is the same.
    local server1 = replica_sets[1][1]
    for i = 2, #replica_sets do
        local server2 = replica_sets[i][1]
        local space_count1 = 0
        for name, space1 in pairs(server1.conn.space) do
            space_count1 = space_count1 + 1
            local space2 = server2.conn.space[name]
            if space2 == nil then
                error(string.format(err_no_space, server1.uri, name,
                                    server2.uri))
            end
            check_spaces_are_equal(space1, server1.uri, space2, server2.uri)
        end
        local space_count2 = 0
        for name, space2 in pairs(server2.conn.space) do
            space_count2 = space_count2 + 1
        end
        if space_count1 ~= space_count2 then
            error(string.format(err_space_count, server1.uri, server2.uri))
        end
    end
end

return {
    check_schema = check_schema,
    check_cfg = function(cfg) return check_cfg(cfg_template, cfg_default, cfg) end,
}
