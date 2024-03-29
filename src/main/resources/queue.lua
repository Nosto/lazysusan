local public = {}
local private = {}

function public.enqueue(slot, queue, time, nexttime, tenant, key, payload)
    local invisible_key = private.invisible_key(slot, queue, tenant)
    if redis.call("zrank", invisible_key, key) then
        return 0
    end

    local visible_key = private.visible_key(slot, queue, tenant)
    redis.call("hset", private.period(slot, queue), tenant, nexttime - time)
    redis.call("hset", private.payload_key(slot, queue, tenant), key, payload)

    if redis.call("zrank", visible_key, key) == false then
        local schedule_key = private.schedule_key(slot, queue)

        if not redis.call("zrank", schedule_key, tenant) then
            redis.call("zadd", schedule_key, nexttime, tenant)
        end
        redis.call("zadd", visible_key, time, key)
        return 1
    else
        return 2
    end
end

function public.dequeue(slot, queue, time, message_nexttime, maxkeys, multiple_per_tenant)
    -- Command-line arguments (maxkeys, multiple_per_tenant) are strings
    -- so we need to convert them integer and boolean
    local max_count = tonumber(maxkeys)
    local only_one_per_tenant = multiple_per_tenant == "0" -- 0=false, 1=true

    local schedule_key = private.schedule_key(slot, queue)
    local tenants = redis.call("zrangebyscore", schedule_key, "-inf", time, "LIMIT", 0, maxkeys)
    local result = {}
    local result_count = 0
    local result_count_before_current_round = 0

    repeat
        result_count_before_current_round = result_count
        for _, tenant in ipairs(tenants) do
            -- Dequeue new messages only if max_count has not been reached yet
            if result_count < max_count then
                local invisible_key = private.invisible_key(slot, queue, tenant)
                local tenant_nexttime = time + redis.call("hget", private.period(slot, queue), tenant)

                local invisible = redis.call("zrangebyscore", invisible_key, "-inf", time, "LIMIT", 0, 1)
                if next(invisible) == nil then
                    local visible = redis.call("zrangebyscore", private.visible_key(slot, queue, tenant), "-inf", "+inf", "LIMIT", 0, 1)

                    if next(visible) then
                        local _, key = next(visible)
                        local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)
                        redis.call("zrem", private.visible_key(slot, queue, tenant), key)

                        redis.call("zadd", invisible_key, message_nexttime, key)

                        result[#result + 1] = tenant
                        result[#result + 1] = key
                        result[#result + 1] = payload
                        result_count = result_count + 1
                        redis.call("zadd", schedule_key, tenant_nexttime, tenant)
                    end
                else
                    local _, key = next(invisible)
                    local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)
                    redis.call("zadd", schedule_key, tenant_nexttime, tenant)

                    redis.call("zrem", invisible_key, key)
                    redis.call("zadd", invisible_key, message_nexttime, key)

                    result[#result + 1] = tenant
                    result[#result + 1] = key
                    result[#result + 1] = payload
                    result_count = result_count + 1
                end
            end
        end
    -- Repeat dequeing
    -- - only if only_one_per_tenant is false (multiple messages per tenant was requested)
    -- - until result_count
    --   - has reached requested max_count or
    --   - is same as in the beginning of current round in repeat..until
    --     meaning that this round did not dequeue any messages so either queue is empty or
    --     there are only invisible messages to which invisibility time has not passed yet.
    until only_one_per_tenant or result_count == max_count or result_count == result_count_before_current_round

    return result
end

function public.ack(slot, queue, tenant, key)
    local invisible_key = private.invisible_key(slot, queue, tenant)
    local payload_key = private.payload_key(slot, queue, tenant)
    redis.call("zrem", private.visible_key(slot, queue, tenant), key)
    local deleted_messages = tonumber(redis.call("hdel", payload_key, key))
    redis.call("zrem", invisible_key, key)
    if redis.call("hlen", payload_key) == 0 then
        redis.call("zrem", private.schedule_key(slot, queue), tenant)
    end
    return deleted_messages == 1
end

function public.queuestats(slot, queue)
    local tenants = redis.call("zrangebyscore", private.schedule_key(slot, queue), "-inf", "+inf")
    local result = {}
    for _, tenant in ipairs(tenants) do
        result[#result + 1] = tenant
        result[#result + 1] = redis.call("zcard", private.invisible_key(slot, queue, tenant))
        result[#result + 1] = redis.call("zcard", private.visible_key(slot, queue, tenant))
    end
    return result
end

function public.peek(slot, queue, tenant, time)
    local result = {}
    local invisible_key = private.invisible_key(slot, queue, tenant)
    local invisible = redis.call("zrangebyscore", invisible_key, "-inf", time, "LIMIT", 0, 1)
    if next(invisible) == nil then
        local visible = redis.call("zrangebyscore", private.visible_key(slot, queue, tenant), "-inf", "+inf", "LIMIT", 0, 1)

        if next(visible) then
            local _, key = next(visible)
            local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)

            result[#result + 1] = tenant
            result[#result + 1] = key
            result[#result + 1] = payload
        end
    else
        local _, key = next(invisible)
        local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)
        result[#result + 1] = tenant
        result[#result + 1] = key
        result[#result + 1] = payload
    end
    return result
end

function public.purge(slot, queue, tenant)
    local schedule_key = private.schedule_key(slot, queue)
    redis.call("zrem", schedule_key, tenant)

    local payload_key = private.payload_key(slot, queue, tenant)
    redis.call("del", payload_key)

    local total_removed = 0
    local invisible_key = private.invisible_key(slot, queue, tenant)
    total_removed = total_removed + redis.call("zcard", invisible_key)
    redis.call("del", invisible_key)

    local visible_key = private.visible_key(slot, queue, tenant)
    total_removed = total_removed + redis.call("zcard", visible_key)
    redis.call("del", visible_key)

    return total_removed
end

function private.schedule_key(slot, queue)
    return "mq:{" .. slot .. "}:" .. queue .. ":schedule"
end

function private.visible_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:" .. queue .. ":visible:" .. tenant
end

function private.invisible_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:invisible:" .. queue .. ":" .. tenant
end

function private.payload_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:payload:" .. queue .. ":" .. tenant
end

function private.period(slot, queue)
    return "mq:{" .. slot .. "}:period:" .. queue
end

return public[ARGV[1]](unpack(ARGV, 2))