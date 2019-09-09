local public = {}
local private = {}
local keyseparator = "KEY_PAYLOAD_SEPARATOR"

function public.enqueue(slot, queue, time, nexttime, tenant, key, payload)
    local deduplicator_key = private.deduplicator_key(slot, queue)
    local deduplicator_member = private.deduplication_member(tenant, key)

    redis.call("hset", private.period(slot, queue), tenant, nexttime - time)

    if redis.call("sismember", deduplicator_key, deduplicator_member) == 0 then
        local schedule_key = private.schedule_key(slot, queue)

        if not redis.call("zrank", schedule_key, tenant) then
            redis.call("zadd", schedule_key, nexttime, tenant)
        end
        redis.call("rpush", private.visible_key(slot, queue, tenant), key .. keyseparator .. payload)
        redis.call("sadd", deduplicator_key, deduplicator_member)
        return true
    else
        return false
    end
end

function public.dequeue(slot, queue, time, maxkeys)
    local schedule_key = private.schedule_key(slot, queue)
    local tenants = redis.call("zrangebyscore", schedule_key, "-inf", time, "LIMIT", 0, maxkeys) -- todo maxkeys
    local result = {}
    for _, tenant in ipairs(tenants) do
        local invisible_key = private.invisible_key(slot, queue, tenant)
        local nexttime = time + redis.call("hget", private.period(slot, queue), tenant)

        local invisible = redis.call("zrangebyscore", invisible_key, "-inf", time, "LIMIT", 0, 1)
        if next(invisible) == nil then
            local packed = redis.call("lpop", private.visible_key(slot, queue, tenant))
            if packed then
                local key, payload = private.split(packed, keyseparator)

                redis.call("zadd", invisible_key, nexttime, key)
                redis.call("hset", private.invisible_payload_key(slot, queue, tenant), key, payload)

                result[#result + 1] = tenant
                result[#result + 1] = key
                result[#result + 1] = payload
                redis.call("zadd", schedule_key, nexttime, tenant)
            end
        else
            local _, key = next(invisible)
            local payload = redis.call("hget", private.invisible_payload_key(slot, queue, tenant), key)
            redis.call("zadd", schedule_key, nexttime, tenant)
            result[#result + 1] = tenant
            result[#result + 1] = key
            result[#result + 1] = payload
        end
    end
    return result
end

function public.ack(slot, queue, tenant, key)
    local invisible_key = private.invisible_key(slot, queue, tenant)
    redis.call("srem", private.deduplicator_key(slot, queue), private.deduplication_member(tenant, key))
    redis.call("hdel", private.invisible_payload_key(slot, queue, tenant), key)
    redis.call("zrem", invisible_key, key)
    if redis.call("llen", private.visible_key(slot, queue, tenant)) == 0 and redis.call("zcard", invisible_key) == 0 then
        redis.call("zrem", private.schedule_key(slot, queue), tenant)
    end
end

function public.queuestats(slot, queue)
    local tenants = redis.call("zrangebyscore", private.schedule_key(slot, queue), "-inf", "+inf")
    local result = {}
    for _, tenant in ipairs(tenants) do
        result[#result + 1] = tenant
        result[#result + 1] = redis.call("hlen", private.invisible_payload_key(slot, queue, tenant))
        result[#result + 1] = redis.call("llen", private.visible_key(slot, queue, tenant))
    end
    return result
end

function private.schedule_key(slot, queue)
    return "mq:{" .. slot .. "}:" .. queue .. ":schedule"
end

function private.deduplicator_key(slot, queue)
    return "mq:{" .. slot .. "}:" .. queue .. ":deduplicator"
end

function private.deduplication_member(tenant, key)
    return tenant .. ":" .. key
end

function private.visible_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:visible:" .. queue .. ":" .. tenant
end

function private.invisible_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:invisible:" .. queue .. ":" .. tenant
end

function private.invisible_payload_key(slot, queue, tenant)
    return "mq:{" .. slot .. "}:invisible:payload:" .. queue .. ":" .. tenant
end

function private.period(slot, queue)
    return "mq:{" .. slot .. "}:period:" .. queue
end

-------------------------------------------------------------------------------
-- splits given string by a separator
-------------------------------------------------------------------------------
function private.split(str, separator)
    return str:match("([^" .. separator .. "]+)" .. separator .. "(.+)")
end

return public[ARGV[1]](unpack(ARGV, 2))