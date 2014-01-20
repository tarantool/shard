-- shard.lua

if box.shard ~= nil then
    return box.shard
end

box.shard =
(function()
    local list = {}
    local me = 0
    local curr  = function(space, key)
        error("function 'curr' is not defined")
    end
    local prev
    local numbers = false
    local lock = {}

    -- connections
    local c = {}
    local cw = {}
    local mode = 'ro'

    local MAX_INDEX_SIZE = 10

    local function TRUE()
        if numbers then return 1 else return '1' end
    end
    local function FALSE()
        if numbers then return 0 else return '0' end
    end

    -- throw formatted error message
    local function errorf(msg, ...)
        local str = string.format(msg, ...)
        error(str)
    end

    -- print formatted message
    local function printf(msg, ...)
        print(string.format(msg, ...))
    end


    -- return true if shard no is valid
    local function is_valid_shardno(shardno)
        if shardno < 1 then
            return false
        end
        if shardno > #list then
            return false
        end
        return true
    end

    -- return valid shardno by key (or raise exception)
    local function curr_valid(space, ...)
        local shardno = curr(space, ...)
        if not is_valid_shardno(shardno) then
            errorf("'curr' returned invalid shardno: %s", shardno)
        end
        return shardno
    end

    -- return valid shardno by key (or raise exception)
    local function prev_valid(space, ...)
        local shardno = prev(space, ...)
        if not is_valid_shardno(shardno) then
            errorf("'prev' returned invalid shardno: %s", shardno)
        end
        return shardno
    end


    -- extract key from tuple of space
    local function extract_key(space, ...)
        local tuple = {...}
        local key = {}

        -- there is no way to detect index keylen
        for i = 0, MAX_INDEX_SIZE - 1 do
            if box.space[space].index[0].key_field[ i ] == nil then
                break
            end
            local fno = box.space[space].index[0].key_field[i].fieldno + 1
            if tuple[fno] == nil then
                error("incomplete primary key in tuple")
            end
            table.insert(key, tuple[fno])
        end
        return key
    end

    -- unpack number
    local function unpack_number(no)
        if type(no) == 'number' then
            return no
        end
        if numbers then
            return box.unpack('i', no)
        end
        return tonumber(no)
    end


    -- check shard list
    local function check_shardlist(list)
        for i, sh in pairs(list) do
            if type(sh) ~= 'table' then
                error("Each shard must be described by 'table of table'")
            end

            if #sh > 0 and type(sh[1]) ~= 'table' then
                error("Each host must be described by 'table'")
            end

            table.sort(sh,
                function(a, b)
                    if a[1] == 'rw' then
                        return true
                    else
                        return false
                    end
                end
            )

            if #sh > 1 and sh[1][1] == 'rw' then
                error("Only one shard host can have 'rw' mode")
            end
        end
    end


    -- greatest common divisor
    local function gcd(a, b)
        while true do
            if a < b then
                a, b = b, a
            end

            local nb = math.fmod(a, b)
            if nb == 0 then
                return b
            end
            a = nb
        end
    end

    -- connect to all shards
    local function connect_all()
        c, cw = {}, {}
        for i, shl in pairs(list) do
            c[i] = {}
            cw[i] = {}

            local div = nil
            for j, sh in pairs(shl) do
                if sh[2] > 0 then
                    sh[2] = math.ceil(sh[2])
                    if div == nil then
                        div = sh[2]
                    else
                        div = gcd(div, sh[2])
                    end
                else
                    sh[2] = 0
                end

                c[i][j] = box.net.box.new(sh[3], sh[4])
            end
            if div == nil then
                div = 1
            end

            for j, sh in pairs(shl) do
                if sh[2] > 0 then
                    for k = 1, sh[2] / div do
                        table.insert(cw[i], j)
                    end
                end
            end
        end
    end

    -- return connection by
    local function connection(shardno, mode)
        if shardno > #list or shardno < 1 then
            error("Wrong shard number")
        end
        if mode == 'rw' then
            if list[shardno][1][1] == 'rw' then
                return c[shardno][1]
            end
            errorf("There is no shard configured as 'rw' for shard %d", shardno)
        end

        local tnt = c[shardno][ math.random(1, #cw[shardno]) ]
        if tnt == nil then
            errorf("Can't find connection for shard %d (%s)", shardno, mode)
        end
        return tnt
    end

    local self

    self = {
        schema = {
            -- shard.schema.config()
            config = function(cfg)
                if type(cfg) ~= 'table' then
                    error("Usage: shard.schema.config({name = value, ...})")
                end

                if cfg.numbers ~= nil then
                    if cfg.numbers then
                        numbers = true
                    else
                        numbers = false
                    end
                end


                local new_list = list
                if cfg.list ~= nil then
                    new_list = cfg.list
                end

                local new_me = me
                if cfg.me ~= nil then
                    new_me = cfg.me
                end

                if type(new_list) ~= 'table' then
                    error("shard list must be table")
                end

                if type(new_me) ~= 'number' then
                    error("'me' must be number")
                end

                if new_me < 0 or new_me > #new_list then
                    error("'me is out of range")
                end

                if cfg.me ~= nil or cfg.list ~= nil then
                    check_shardlist(new_list)
                    me = new_me
                    list = new_list
                    connect_all()
                end

                if cfg.curr ~= nil then
                    if type(cfg.curr) ~= 'function' then
                        error("'curr' must be a function")
                    end
                    curr = cfg.curr
                end

                if cfg.prev ~= nil then
                    if type(cfg.prev) == 'boolean' then
                        if cfg.prev then
                            error("'prev' must be a function or false")
                        else
                            prev = nil
                        end
                    else
                        if type(cfg.prev) ~= 'function' then
                            error("'prev' must be a function or false")
                        end
                        prev = cfg.prev
                    end
                end

                if cfg.mode ~= nil then
                    if cfg.mode ~= 'ro' and cfg.mode ~= 'rw' then
                        errorf("wrong 'mode' option: %s", mode)
                    end

                    mode = cfg.mode
                end

                if numbers then
                    return 1
                else
                    return '1'
                end
            end,

            -- shard.schema.is_valid()
            is_valid = function()
                if #list == 0 then
                    return FALSE()
                end
                return TRUE()
            end
        },

        internal = {

            -- shard.replace(space, ...)
            replace = function(ttl, space, ...)
                ttl = tonumber(ttl)
                space = unpack_number(space)

                local key = extract_key(space, ...)
                local shardno = curr_valid(space, unpack(key))
                local tnt = connection(shardno, 'rw')

                if shardno ~= me or mode ~= 'rw' then
                    if not numbers then
                        space = tostring(space)
                    end
                    if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d (ttl=0)",
                            shardno
                        )
                    end
                    ttl = tostring(ttl - 1)
                    return
                        tnt:call('box.shard.internal.replace', ttl, space, ...)
                end

                return box.replace(space, ...)
            end,

            -- shard.insert(space, ...)
            insert = function(ttl, space, ...)
                ttl = tonumber(ttl)
                space = unpack_number(space)
                printf("box.shard.insert(%s, ...)", space)
                local key = extract_key(space, ...)
                local shardno = curr_valid(space, unpack(key))

                if shardno ~= me or mode ~= 'rw' then
                    if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d (ttl=0)",
                            shardno
                        )
                    end
                    local tnt = connection(shardno, 'rw')
                    printf("redirect insert to shard %d", shardno)
                    ttl = tostring(ttl - 1)
                    if not numbers then
                        space = tostring(space)
                    end
                    return
                        tnt:call('box.shard.internal.insert', ttl, space, ...)
                end

                if prev == nil then
                    return box.insert(space, ...)
                end

                local tuple = box.select(space, 0, unpack(key))
                if tuple ~= nil then
                    error("Duplicate key exists in unique index 0")
                end

                local pshardno = prev_valid(space, unpack(key))
                if pshardno == shardno then
                    return box.insert(space, ...)
                end

                local tnt = connection(pshardno, 'rw')
                local ptuple = tnt:select(space, 0, unpack(key))
                if ptuple ~= nil then
                    error("Duplicate key exists in unique index 0")
                end
                return box.insert(space, ...)
            end,

            delete = function(ttl, space, ...)
                ttl = tonumber(ttl)
                space = unpack_number(space)
                local shardno = curr_valid(space, ...)

                if shardno ~= me or mode ~= 'rw' then
                    if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d (ttl=0)",
                            shardno
                        )
                    end
                    ttl = tostring(ttl - 1)
                    if not numbers then
                        space = tostring(space)
                    end
                    local tnt = connection(shardno, 'rw')
                    return
                        tnt:call('box.shard.internal.delete', ttl, space, ...)
                end


                local ptuple
                if prev ~= nil then
                    local pshardno = prev_valid(space, ...)
                    if pshardno ~= shardno then
                        local tnt = connection(pshardno, 'rw')
                        ptuple = tnt:delete(space, ...)
                    end
                end

                if ptuple == nil then
                    return box.delete(space, ...)
                end

                local tuple = box.delete(space, ...)

                if tuple ~= nil then
                    return tuple
                end
                return ptuple
            end,


            -- shard.call(mode, proc_name, ...)
            call = function(mode, proc_name, ...)
                if mode ~= 'ro' and mode ~= 'rw' then
                    errorf("Wrong mode: %s", mode)
                end
                local shardno = math.random(1, #list)
                local tnt = connection(shardno, mode)
                return box.net.self:call(proc_name, ...)
            end,

            -- box.shard.update(space, key[, subkey, ...], format, ...)
            update = function(ttl, space, ...)
                ttl = tonumber(ttl)
                space = unpack_number(space)

                local format
                local oplist
                local key = {}

                local l = {...}

                for i = 1, MAX_INDEX_SIZE do
                    if box.space[space].index[0].key_field[i - 1] == nil then
                        format = l[ i ]
                        oplist = { select(i + 1, ...) }
                        break
                    end
                    table.insert(key, l[i])
                end

                if #key < 1 then
                    errorf("Wrong key")
                end


                if format == nil then
                    error("Wrong format")
                end

                if oplist == nil or math.fmod(#oplist, 2) ~= 0 then
                    errorf("Wrong oplist (contains %d elements)", #oplist)
                end

                local shardno = curr_valid(space, unpack(key))
                
                if shardno ~= me or mode ~= 'rw' then
                    local tnt = connection(shardno, 'rw')
                    if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d (ttl=0)",
                            shardno
                        )
                    end
                    ttl = tostring(ttl - 1)

                    if not numbers then
                        space = tostring(space)
                    end
                    return tnt:call('box.shard.internal.update',
                        ttl, space, ...)
                end


                local tuple = box.select(space, 0, unpack(key))
                if tuple ~= nil or prev == nil then
                    return box.update(space, key, format, unpack(oplist))
                end

                local pshardno = prev_valid(space, unpack(key))
                if pshardno == shardno then
                    return box.update(space, key, format, unpack(oplist))
                end

                local ptnt = connection(pshardno, 'rw')
                local ptuple = ptnt:select(space, 0, unpack(key))
                if ptuple == nil then
                    return box.update(space, key, format, unpack(oplist))
                end

                tuple = box.select(space, 0, unpack(key))
                if tuple ~= nil then
                    return box.update(space, key, format, unpack(oplist))
                end
                box.insert(space, ptuple:unpack())
                return box.update(space, key, format, unpack(oplist))
            end,
        },

        -- box.shard.insert(space, ...)
        insert = function(space, ...)
            return box.shard.internal.insert(1, space, ...)
        end,

        -- box.shard.replace(space, ...)
        replace = function(space, ...)
            return box.shard.internal.replace(1, space, ...)
        end,

        -- box.shard.select(space, ...)
        select = function(space, ...)
            return box.shard.eselect('ro', space, ...)
        end,

        -- box.shard.eselect(mode, space, ...)
        eselect = function(smode, space, ...)
            if smode ~= 'ro' and smode ~= 'rw' then
                errorf('wrong mode: %s', smode)
            end
            space = unpack_number(space)

            local shardno = curr_valid(space, ...)

            local tnt

            if shardno == me then
                if smode == mode or mode == 'rw' then
                    local tuple = box.select(space, 0, ...)
                    if tuple ~= nil then
                        return tuple
                    end
                else
                    tnt = connection(shardno, mode)
                end
            else
                tnt = connection(shardno, mode)
            end

            if tnt ~= nil then
                local tuple = tnt:select(space, 0, ...)
                if tuple ~= nil then
                    return tuple
                end
            end

            if prev == nil then
                return
            end

            local pshardno = prev_valid(space, ...)
            if pshardno == shardno then
                return
            end
            tnt = connection(pshardno, mode)
            return tnt:select(space, 0, ...)
        end,

        -- box.shard.delete(space, ...)
        delete = function(space, ...)
            return box.shard.internal.delete(1, space, ...)
        end,

        -- box.shard.update(space, key[,...], format, ...)
        update = function(space, ...)
            return box.shard.internal.update(1, space, ...)
        end,

    }

    return self

end)()

return box.shard
