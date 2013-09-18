-- box.shard
(function(box)

    local function errorf(fmt, ...)
        error( string.format(fmt, ...) )
    end

    local function printf(fmt, ...)
        print( string.format(fmt, ...) )
    end
        


    local self = {
        tx = {}
    }

    box.shard = self

    local function extract_key_from_tuple(space, tuple)
        
        local key = {}

        for i = 0, #space.index[0].key_field do
            local subkey = tuple[ space.index[0].key_field[i].fieldno ]
            if subkey == nil then
                errorf("Can't extract %s subkey from tuple", i)
            end
            table.insert(key, subkey)
        end
        return key
    end


    local function move_tuples(space, tuples)
            printf('Preempt %s tuples', #tuples)
        for _, tuple in pairs(tuples) do

            local key = extract_key_from_tuple(space, tuple)
            local sno = self.schema.key(space.n, unpack(key))
            if sno ~= self.schema.this then
                -- try move record
                self.schema.box[sno]:call('box.shard.select',
                    space.name, unpack(key))
                local t = space:select(0, unpack(key))

                if t ~= nil then
                    local ckey = extract_key_from_tuple(space, t)
                    local csno = self.schema.key(space.n, unpack(ckey))

                    if csno == sno then
                        errorf("Can't preemt tuple of %s shard", csno)
                    end
                end
            end
        end
    end


    local function space_is_system(space)
        if space.n > box.schema.SYSTEM_ID_MAX then
            return false
        end
        if space.n < box.schema.SYSTEM_ID_MIN then
            return false
        end
        return true
    end


    local function check_space(space)
        if space_is_system(space) then
            return
        end
        
        printf('Trying preempt tuples from space[%s] (%s)',
            space.n, space.name)

        while true do

            local list = {}
            local cnt = 0
            local found = 0
            for tuple in space.index[0]:iterator(box.index.ALL) do
                local key = extract_key_from_tuple(space, tuple)
                local sno = self.schema.key(space.n, unpack(key))
                if sno ~= self.schema.this then
                    table.insert(list, tuple)
                    found = found + 1
                end
                cnt = cnt + 1

                if #list > 100 then
                    move_tuples(space, list)
                    list = {}
                    cnt = 0
                end


                if cnt > 1000 then
                    box.fiber.sleep(0)
                end
            end

            if #list > 0 then
                move_tuples(space, list)
                list = {}
            end


            box.fiber.sleep(0)
            if found == 0 then
                printf('Finished moving tuples of space[%s] (%s)',
                    space.n, space.name)
                break
            end
            printf('Moved %s tuples, repeat daemon cycle', found)
        end
    end

    local function daemon_process()
        local done = {}
        for _, s in pairs(box.space) do
            if not done[ s.n ] then
                check_space(s)
            end
            done[ s.n ] = true
        end
    end

    local function daemon()
        printf('Staring preempt daemon')
        if self.schema.old_key == nil then
            self.daemon = nil
            return
        end
        if self.schema.daemon_off_period ~= nil then
            box.fiber.sleep(self.schema.daemon_off_period)
        end
        
        if self.schema.old_key == nil then
            self.daemon = nil
            return
        end

        local res, err = pcall(daemon_process)
        self.daemon = nil
        if not res then
            errorf('box.shard: daemon was crashed: %s', err)
        end
    end


    -- Shard schema
    self.schema = {
        list                = {  },
        this                = 1,
        daemon              = false,
        daemon_off_period   = 30,

        key                 = function() return self.schema.this end,
        old_key             = nil,

        -- Return true if shard schema is valid
        valid   = function(sch)
            if #sch.list > 0 then
                if sch.this > 0 and sch.this <= #sch.list then
                    return 1
                end
            end
            return 0
        end,

        -- configure the shard
        config = function(cfg)
            local sch = self.schema
            local may = {
                list                = 'table',
                this                = 'number',
                key                 = 'function',
                old_key             = 'function',
                daemon              = 'boolean',
                daemon_off_period   = 'number'
            }
            
            local key_changed = false
            
            for k, t in pairs(may) do
                -- disconnect all shards if shardlist is changed
                if k == 'list' or k == 'this' then
                    for i, _ in pairs(sch.box) do
                        rawset(sch.box, i, nil)
                    end
                end
                if type(cfg[ k ]) == t then
                    rawset(sch, k, cfg[ k ])
                    if k == 'key' or k == 'old_key' then
                        key_changed = true
                    end
                end
            end
            
            if key_changed and sch.old_key ~= nil then
                if sch.daemon then
                    if self.daemon == nil then
                        self.daemon = box.fiber.wrap(daemon)
                    end
                end
            end

            return sch:valid()
        end,

        box = {  }
    }

    setmetatable(self.schema.box, {
        __index = function(bx, no)
            if no == self.schema.this then
                rawset(bx, no, box.net.self)
                return bx[ no ]
            end
            if type(no) ~= 'number' then
                errorf('Wrong shard number: %s', no)
            end
            if no < 1 or no > #self.schema.list then
                errorf('Shard %s is not found in shardlist', no)
            end
            rawset(bx, no, box.net.box.new( unpack( self.schema.list[ no ] ) ))
--             bx[ no ].debug = true
            return bx[ no ]
        end
    })

    local function extract_key(space, ...)
        
        local s = box.space[ space ]
        if s == nil then
            errorf('Space "%s" is not exists', space)
        end
        
        local tuple = { ... }
        local key = {}

        for i = 0, #s.index[0].key_field do
            local subkey = tuple[ s.index[0].key_field[i].fieldno + 1 ]
            if subkey == nil then
                errorf("Can't extract %s subkey from tuple", i)
            end
            table.insert(key, subkey)
        end
        return key
    end

    local function rettuple(tuple)
        if tuple == nil then
            return
        end
        return tuple
    end

    -- move record to the shard
    local function move_record(sno, space, ...)

        assert(sno ~= self.schema.this)

        local klen = select('#', ...)
        local key = tostring(space)
        for i = 1, klen do
            key = key .. '-' .. select(i, ...)
        end
        
        if self.tx[ klen ] == nil then
            self.tx[ klen ] = {}
        end
        if self.tx[ klen ][ key ] ~= nil then
            local ch = box.ipc.channel()
            table.insert(self.tx[ klen ][ key ], ch)
            return rettuple( ch:get() )
        end

        local ttuple = self.schema.box[self.schema.this]
                                    :select(box.space[space].n, 0, ...)

        -- the other fibers will wait for move record
        self.tx[ klen ][ key ] = {}

        if ttuple ~= nil then
            self.schema.box[sno]:delete(box.space[space].n, ...)
        else

            local t = self.schema.box[sno]:select(box.space[space].n, 0, ...)
            if t ~= nil then
                ttuple = self.schema.box[self.schema.this]
                    :select(box.space[space].n, 0, ...)
                if ttuple == nil then
                    ttuple = self.schema.box[self.schema.this]
                        :insert(box.space[space].n, t:unpack())
                end
                self.schema.box[sno]:delete(box.space[space].n, ...)
            end
        end

        for _, ch in pairs(self.tx[ klen ][ key ]) do
            ch:put(ttuple)
        end
        self.tx[ klen ][ key ] = nil

        return rettuple(ttuple)
    end


    local function remote_update(sno, space, key, ...)
        if type(key) ~= 'table' then
            key = { key }
        end
        local request = { tostring(#key), unpack(key) }
        for i = 1, select('#', ...) do
            local v = select(i, ...)
            if i % 2 == 0 then
                v = tostring( v )
            end
            table.insert(request, v)
        end
        return self.schema.box[sno]:call('box.shard._update',
            space, unpack(request) )
    end

    --
    --
    -- API
    --
    --

    -- box.shard.select(space, key)
    --   select tuple by key
    self.select = function(space, ...)
        local sno = self.schema.key(space, ...)

        if sno ~= self.schema.this then
            -- don't lua call if resharding is done
            if self.schema.old_key == nil then
                return self.schema.box[sno]:select(box.space[space].n, 0, ...)
            end
            return self.schema.box[sno]:call('box.shard.select', space, ...)
        end

        
        local tuple = self.schema.box[sno]:select(box.space[space].n, 0, ...)
        if tuple ~= nil then
            return tuple
        end
        if self.schema.old_key == nil then
            return
        end
        local old_sno = self.schema.old_key(space, ...)
        if old_sno == sno then
            return
        end

        return move_record(old_sno, space, ...)
    end


    -- box.shard.insert(space, ...)
    -- insert tuple
    self.insert = function(space, ...)
        local key = extract_key(space, ...)
        local sno = self.schema.key(space, unpack(key))

        if sno ~= self.schema.this then
            if self.schema.old_key ~= nil then
                return self.schema.box[sno]:call('box.shard.insert', space, ...)
            end
        end
        return self.schema.box[sno]:insert(box.space[space].n, ...)
    end
    
    -- box.shard.replace(space, ...)
    -- replace tuple
    self.replace = function(space, ...)
        local key = extract_key(space, ...)
        local sno = self.schema.key(space, unpack(key))
        if sno ~= self.schema.this then
            if self.schema.old_key ~= nil then
                return self.schema.box[sno]:call('box.shard.insert', space, ...)
            end
        end
        return self.schema.box[sno]:replace(box.space[space].n, ...)
    end

    -- box.shard.update(space, key, fmt, ...)
    -- update tuple
    self.update = function(space, key, ...)
        if type(key) ~= 'table' then
            key = { key }
        end
        local sno = self.schema.key(space, unpack(key) )

        if sno ~= self.schema.this then
            if self.schema.old_key ~= nil then
                return remote_update(sno, space, key, ...)
            end
            return self.schema.box[sno]:update(box.space[space].n, key, ...)
        end

        if self.schema.old_key ~= nil then
            local old_sno = self.schema.old_key( space, unpack(key) )
            if old_sno ~= sno then
                move_record(old_sno, space, unpack(key))
            end
        end

        return self.schema.box[sno]:update(box.space[space].n, key, ...)
    end


    -- internal method (uses to translate multikey through socket)
    self._update = function(space, klen, ...)
        klen = tonumber(klen)
        local al = select('#', ...)
        local key = {}
        for i = 1, klen do
            local sk = select(i, ...)
            table.insert(key, sk)
        end
        local tail = { select(klen + 1, ...) }
        for i = 1, #tail do
            if i % 2 == 0 then
                tail[ i ] = tonumber(tail[ i ])
            end
        end
        return self.update( space, key,  unpack(tail) )
    end

    -- box.shard.delete(space, key)
    -- delete tuple
    self.delete = function(space, ...)
        local sno = self.schema.key(space, ...)
        if sno ~= self.schema.this then
            if self.schema.old_key == nil then
                return self.schema.box[sno]:delete(box.space[space].n, ...)
            else
                return self.schema.box[sno]:call('box.shard.delete', space, ...)
            end
        end

        if self.schema.old_key ~= nil then
            local old_sno = self.schema.old_key(space, ...)
            if old_sno == sno then
                return self.schema.box[sno]:delete(box.space[space].n, ...)
            end

            local tuple = move_record(old_sno, space, ...)
            if tuple == nil then
                return
            end
        end
            
        return self.schema.box[sno]:delete(box.space[space].n, ...)
    end



end)(box)

