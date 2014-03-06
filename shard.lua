-- shard.lua

if box.shard ~= nil then
    return box.shard
end

local ffi = require("ffi")
ffi.cdef[[
struct __timeval {
    uint64_t      tv_sec;
    uint64_t      tv_usec;
};
uint64_t time(uint64_t *t);
int gettimeofday(struct __timeval *tv, struct timezone *tz);
]]
timeval = ffi.typeof("struct __timeval");

local hitime = function()
    local tv = timeval();
    ffi.C.gettimeofday(tv,nil);
    return tonumber(tv.tv_sec) + tonumber(tv.tv_usec)/1e6;
end

box.shard =
(function()
    -- throw formatted error message
    local function errorf(msg, ...)
        error(debug.traceback(string.format(msg, ...),2))
    end

    -- print formatted message
    local function printf(msg, ...)
        print(string.format(msg, ...))
    end
    
    local _id = box.uuid_hex()
    printf("Configuring shard with id = %s",_id)
    
    local debugging = true
    local list = {}
    local me = 0
    local nodeno = -1
    local remote_timeout = 0.05
    local curr  = function(space, key)
        error("function 'curr' is not defined")
    end
    local prev
    local numbers = false
    local lock = {}
    
    local callbacks = {}

    local internal_spaces = {}

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


    local function cleanup_space(space)
        local count = 0
        while true do
            local to_rm = {}
            for tuple in box.space[space].index[0]:iterator(box.index.ALL) do
                local key = extract_key(space, tuple:unpack())
                local shardno = curr(space, unpack(key))
                if shardno ~= me then
                    table.insert(to_rm, key)
                    if #to_rm > 1000 then
                        break
                    end
                end
            end
            if #to_rm == 0 then
                break
            end
            for i, key in pairs(to_rm) do
                box.delete(space, unpack(key))
                count = count + 1    
            end
        end

        if numbers then
            return box.tuple.new({space, count})
        else
            return box.tuple.new({tostring(space), tostring(count)})
        end
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

            if #sh > 1 and sh[1][2] == 'rw' then
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
    --[[
        c[shard]:
            all: [ ... ]
            rw: [ ... ]
    ]]--
    local connections
    local connected = {}
    local function connect_all()
        if connections ~= nil then
            error("TODO: cancel fibers")
        end
        connections = {}
        local i,j,shl,sh
        local xx = 0
        for i, shl in pairs(list) do
            connected[i] = { rw = nil; all = {} }
            for j, sh in pairs(shl) do
                (function (shno, nno, shcf)
                    local isrw = shcf[1] == 'rw'
                    local ckey = shcf[3] .. ':' .. shcf[4]
                    local shtp = i..'/'..shcf[1]
                    if debugging then printf("connecting for %s=%s ...",ckey, shtp) end
                    if shno == me and nno == nodeno then
                        printf("skip %d/%d", me,nodeno)
                        return
                    end
                    local con = box.net.box.new(shcf[3], shcf[4], remote_timeout, xx)
                    xx = xx + 1
                    -- TODO: connect+ping behaves wrong
                    local fib = box.fiber.create(function()
                        box.fiber.name("shardc."..ckey);
                        box.fiber.detach()
                        while true do
                            local r,e = pcall(function()
                                local t = con:timeout(0.05):call('box.shard.id')
                                if t then return t[0] else return nil end
                            end)
                            if r and e then
                                printf('shard id for %s=%s = %s',ckey,shtp,e)
                                if e == _id then
                                    print("It's me / "..shno, " rw:",isrw)
                                    con:close()
                                    con = box.net.self
                                    if isrw then
                                        print("set connected ",con)
                                        connected[ shno ]['rw'] = box.net.self
                                    end
                                    table.insert(connected[ shno ]['all'],box.net.self)
                                    connections[ckey]['status'] = true
                                    return
                                end
                                break
                            else
                            	-- print(r,e)
                                box.fiber.sleep(1)
                            end
                        end
                        while true do
                            -- printf("ping %s:%s...", con.host, con.port)
                            local time1 = hitime();
                            local r,e = pcall(function()
                                return con:timeout(remote_timeout):ping()
                                --return con:ping()
                            end)
                            time1 = hitime()-time1
                            -- printf("pong %s:%s -> %s %s %f", con.host, con.port, r,e, time1)
                            if r and e then
                                -- connection good
                                if not connections[ckey]['status'] then
                                    if debugging then printf("connection to %s=%s became ok: %s / %f",ckey, shtp, e, time1) end
                                    if isrw then
                                        connected[ shno ]['rw'] = con
                                    end
                                    table.insert(connected[ shno ]['all'],con)
                                    connections[ckey]['status'] = true
                                    if callbacks.connected then callbacks.connected( shno, con, isrw ) end
                                end
                                box.fiber.sleep(1)
                            else
                                -- connection bad
                                if not r then printf("connection to %s=%s failed: %s / %f",ckey,shtp,e, time1) end
                                if connections[ckey]['status'] then
                                    if debugging then printf("connection to %s=%s became bad: %s / %f",ckey, shtp, e, time1) end
                                    if callbacks.disconnected then callbacks.disconnected( shno, con, isrw ) end
                                    if isrw then
                                        connected[ shno ]['rw'] = nil
                                    end
                                    for k,v in pairs(connected[ shno ]['all']) do
                                        if con == v then
                                            printf("removing %d from all %s:%s", k,v.host,v.port)
                                            table.remove(connected[ shno ]['all'], k )
                                            break;
                                        else
                                            -- print("skip ",k," in all")
                                        end
                                    end
                                    connections[ckey]['status'] = false
                                end
                                box.fiber.sleep(1)
                            end
                        end
                    end)
                    print("store fiber ".. box.fiber.id(fib) .." in " ..ckey.. " > ", connections[ckey])
                    connections[ckey] = {
                        c = con,
                        f = fib
                    }
                    box.fiber.resume(fib)
                end)(i,j,sh)
            end
        end
    end

    -- connect to all shards
    local function connect_all_1()
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
            --[[
            for j, sh in pairs(shl) do
                if sh[2] > 0 then
                    for k = 1, sh[2] / div do
                        table.insert(cw[i], j)
                    end
                end
            end
            ]]--
        end
    end
    local function connections_debug()
        for shardno,v in pairs(connected) do
            print(shardno,":")
            for ix,lst in pairs(v) do
                if ix == 'rw' then
                    print("\t",ix," ", lst and "ok" or "none")
                else
                    print("\t",ix," ", #lst)
                end
            end
        end
    end

    -- return connection by
    local function connection(shardno, mode)
        if shardno > #list or shardno < 1 then
            error("Wrong shard number")
        end
        -- connections_debug()
        if mode == 'rw' then
            if list[shardno][1][1] == 'rw' then
                --print(table.concat(connected,', '))
                if connected[shardno]['rw'] then
                    return connected[shardno]['rw']
                end
                errorf("Not conneted to rw node for shard %d", shardno)
            end
            errorf("There is no shard configured as 'rw' for shard %d", shardno)
        end
        --connections_debug()
        
        if #connected[shardno]['all'] > 0 then
            return connected[shardno]['all'][ math.random(1, #connected[shardno]['all']) ]
        end
        errorf("Not connected to any node for shard %d", shardno)
    end
    
    local function copy_space(space)
        local count = 0
        while true do
            local done = 0
            for tuple in box.space[space].index[0]:iterator(box.index.ALL) do
                local key = extract_key(space, tuple:unpack())
                local pshardno = prev(space, unpack(key))
                local shardno = curr(space, unpack(key))
                if pshardno == me and shardno ~= me then
                    local tnt = connection(shardno, 'rw')
                    local stuple =
                        tnt:call('box.shard.internal.copy_tuple',
                            tostring(space),
                            tuple:unpack())
                    if stuple ~= nil then
                        done = done + 1
                        count = count + 1
                    end
                end
            end

            if done == 0 then
                break
            end
        end
        if numbers then
            return box.tuple.new({ space, count })
        else
            return box.tuple.new({ tostring(space), tostring(count) })
        end
    end

    local self

    self = {
        id = function () return _id end,
        schema = {
            -- shard.schema.config()
            info = function ()
                connections_debug()
                -- print('XXX')
            end,
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

                if cfg.internal_spaces ~= nil then
                    if type(cfg.internal_spaces) ~= 'table' then
                        errorf("wrong 'internal_spaces' option: %s",
                            type(cfg.internal_spaces))
                    end

                    internal_spaces = {}
                    for i, v in pairs(cfg.internal_spaces) do
                        internal_spaces[ i ] = true
                    end
                end
                
                if cfg.on_connected then
                    callbacks["connected"] = cfg.on_connected
                end
                if cfg.on_disconnected then
                    callbacks["disconnected"] = cfg.on_disconnected
                end
                
                if numbers then
                    return 1
                else
                    return '1'
                end
            end,
            
            autome = function()
                if #list == 0 then error("list must be configured") end
                local ip = box.cfg.bind_ipaddr;
                me = 0
                for k,v in pairs(list) do
                    for j,sh in pairs(v) do
                        if ip == sh[3] and box.cfg.primary_port == sh[4] then
                            print("autome: ", k, ": ", table.concat(sh, ", "))
                            me = k
                            nodeno = j
                            mode = sh[1]
                            return
                        end
                    end
                end
                print("i am (",ip,":",box.cfg.primary_port, ") have no shardno");
            end,

            -- shard.schema.is_valid()
            is_valid = function()
                if #list == 0 then
                    return FALSE()
                end
                return TRUE()
            end,
            
            curr = curr_valid,
            prev = prev_valid,
            connection = connection,
        },

        internal = {

            -- copy tuple
            copy_tuple = function (space, ...)
                space = tonumber(space)
                local key = extract_key(space, ...)
                local exists = box.select(space, 0, unpack(key))
                if exists then
                    return
                end
                local shardno = curr(space, unpack(key))
                if shardno ~= me then
                    errorf("Can't copy tuple: curr points the other shard: %d",
                        shardno)
                end
                if mode ~= 'rw' then
                    errorf("Can't copy tuple: shard %d isn't in 'rw' mode",
                        me)
                end
                return box.insert(space, ...)
            end,

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
                if (debugging) then print(".internal.insert(",table.concat({ttl,space,...},","),")") end
                ttl = tonumber(ttl)
                space = unpack_number(space)
                local key = extract_key(space, ...)
                local shardno = curr_valid(space, unpack(key))
                if (debugging) then print(".internal.insert fwd to ",shardno) end

                if shardno ~= me or mode ~= 'rw' then
                    if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d:%s from %d:%s (ttl=0)",
                            shardno, "rw", me, mode
                        )
                    end
                    local tnt = connection(shardno, 'rw')
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
            if (debugging) then print(".insert(",table.concat({space,...},","),")") end
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

        -- box.shard.cleanup()
        cleanup = function()
            if mode ~= 'rw' then
                return
            end
            if prev ~= nil then
                errorf("Reshading isn't done yet")
            end
            result = {}
            for sno, space in pairs(box.space) do
                if not internal_spaces[ sno ] then
                    table.insert(result, cleanup_space(sno))
                end
            end
            return unpack(result)
        end,

        -- box.shard.copy()
        copy = function()
            if mode ~= 'rw' then
                return
            end
            if prev == nil then
                errorf("Resharding mode is already done")
            end
            result = {}
            for sno, space in pairs(box.space) do
                if not internal_spaces[ sno ] then
                    table.insert(result, copy_space(sno))
                end
            end
            return unpack(result)
        end

    }

    return self

end)()

return box.shard
