local public = {}
local private = {}

function public.enqueue(slot, queue, time, nexttime, tenant, key, payload)
    local visible_key = private.visible_key(slot, queue, tenant)
    redis.call("hset", private.period(slot, queue), tenant, nexttime - time)
    redis.call("hset", private.payload_key(slot, queue, tenant), key, payload)

    if redis.call("zrank", visible_key, key) == false then
        local schedule_key = private.schedule_key(slot, queue)

        if not redis.call("zrank", schedule_key, tenant) then
            redis.call("zadd", schedule_key, nexttime, tenant)
        end
        redis.call("zadd", visible_key, time, key)
        return true
    else
        return false
    end
end

function public.dequeue(slot, queue, time, maxkeys, nexttime_no_ack)
    local schedule_key = private.schedule_key(slot, queue)
    local tenants = redis.call("zrangebyscore", schedule_key, "-inf", time, "LIMIT", 0, maxkeys) -- todo maxkeys
    local result = {}
    for _, tenant in ipairs(tenants) do
        local invisible_key = private.invisible_key(slot, queue, tenant)
        local nexttime = time + redis.call("hget", private.period(slot, queue), tenant)

        local invisible = redis.call("zrangebyscore", invisible_key, "-inf", time, "LIMIT", 0, 1)
        if next(invisible) == nil then
            local visible = redis.call("zrangebyscore", private.visible_key(slot, queue, tenant), "-inf", "+inf", "LIMIT", 0, 1)

            if next(visible) then
                local _, key = next(visible)
                local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)
                redis.call("zrem", private.visible_key(slot, queue, tenant), key)

                redis.call("zadd", invisible_key, nexttime, key)

                result[#result + 1] = tenant
                result[#result + 1] = key
                result[#result + 1] = payload
                result[#result + 1] = nexttime - time
                redis.call("zadd", schedule_key, nexttime_no_ack, tenant)
            end
        else
            local _, key = next(invisible)
            local payload = redis.call("hget", private.payload_key(slot, queue, tenant), key)
            redis.call("zadd", schedule_key, nexttime, tenant)
            result[#result + 1] = tenant
            result[#result + 1] = key
            result[#result + 1] = payload
            result[#result + 1] = nexttime - time
        end
    end
    return result
end

function public.ack(slot, queue, tenant, key)
    local invisible_key = private.invisible_key(slot, queue, tenant)
    local payload_key = private.payload_key(slot, queue, tenant)
    redis.call("zrem", private.visible_key(slot, queue, tenant), key)
    redis.call("hdel", payload_key, key)
    redis.call("zrem", invisible_key, key)
    if redis.call("hlen", payload_key) == 0 then
        redis.call("zrem", private.schedule_key(slot, queue), tenant)
    end
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