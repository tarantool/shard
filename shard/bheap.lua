-- Lua heap implementation
--
-- API:
-- bheap:new(cmp) - create new bheap with compare function cmp
-- bheap:push(item) - add new item to bheap
-- bheap:pop() - take top item from bheap
-- bheap:top() - get top item from bheap
-- bheap:insert(item) - add item to bheap without ordering
-- bheap:start() - sort bheap
-- bheap:replace(item) - take top item and add new
local function up(heap, index)
    if index == 1 then
        return
    end
    local item = heap.data[index]
    while index > 1 do
        local parent_index = math.floor(index / 2)
        if not heap.cmp(heap.data[parent_index], item) then
            heap.data[index] = heap.data[parent_index]
            heap.data[parent_index] = item
            index = parent_index
        else
            break
        end
    end
    return index
end

local function down(heap, index)
    if index > heap.data_len / 2 then
        return
    end
    local item = heap.data[index]
    while index * 2 <= heap.data_len do
        local child_index = index * 2
        if index * 2 + 1 <= heap.data_len and
           not heap.cmp(heap.data[index * 2], heap.data[index * 2 + 1]) then
            child_index = index * 2 + 1
        end
        if not heap.cmp(item, heap.data[child_index]) then
            heap.data[index] = heap.data[child_index]
            heap.data[child_index] = item
            index = child_index
        else
            break
        end
    end
    return index
end

local function push(heap, item)
    local index = heap.data_len + 1
    heap.data[index] = item
    heap.data_len = heap.data_len + 1
    up(heap, index)
end

local function pop(heap, item)
    local res = heap.data[1]
    if heap.data_len > 1 then
        heap.data[1] = heap.data[heap.data_len]
        heap.data[heap.data_len] = nil
	heap.data_len = heap.data_len - 1
        down(heap, 1)
    else
        heap.data[1] = nil
	heap.data_len = 0
    end
    return res
end

local function top(heap)
    if heap.data_len == 0 then
        return nil
    end
    return heap.data[1]
end

local function insert(heap, item)
    table.insert(heap.data, item)
    heap.data_len = heap.data_len + 1
end

local function start(heap)
    for i = math.floor(heap.data_len / 2), 1, -1 do
    print(i)
        down(heap, i)
    end
end

local function replace(heap, item)
    if heap.data_len == 0 then
        return push(heap, item)
    end
    heap.data[1] = item
    down(heap, 1)
end

local function new(compare)
    local res = {
        data = {},
	data_len = 0,
        cmp = compare,
        push = push,
        pop = pop,
	top = top,
	insert = insert,
	start = start,
	replace = replace}
    return res
end

return {
    new = new}
