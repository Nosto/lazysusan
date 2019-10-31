/*******************************************************************************
 * Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

class SingleNodeScript extends AbstractScript {
    private static final byte[] SLOT = new byte[] {};

    private final JedisPool jedisPool;
    private final int dbIndex;
    private final byte[] sha;

    SingleNodeScript(JedisPool jedisPool, int dbIndex) {
        this.jedisPool = jedisPool;
        this.dbIndex = dbIndex;
        byte[] loadedScript = loadScript();
        sha = executeJedis(jedis -> jedis.scriptLoad(loadedScript));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TenantMessage> dequeue(Instant now, Duration invisiblePeriod, String queue, int maxKeys) {
        return unpackTenantMessage((List<byte[]>) call(Function.DEQUEUE,
                SLOT,
                bytes(queue),
                bytes(now.toEpochMilli()),
                bytes(now.plus(invisiblePeriod).toEpochMilli()),
                bytes(maxKeys)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public QueueStatistics getQueueStatistics(String queue) {
        return unpackQueueStatistics((List<Object>) call(Function.GET_QUEUE_STATS, SLOT, bytes(queue)));
    }

    @Override
    Object evalsha(List<byte[]> keys, List<byte[]> args) {
        return executeJedis(jedis -> jedis.evalsha(sha, Collections.emptyList(), args));
    }

    @Override
    byte[] slot(String tenant) {
        return SLOT;
    }

    private <R> R executeJedis(java.util.function.Function<Jedis, R> jedisFunction) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(dbIndex);
            return jedisFunction.apply(jedis);
        }
    }
}
