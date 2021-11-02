/*
 *  Copyright (c) 2020 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

class SingleNodeScript extends AbstractScript {
    private static final byte[] SLOT = new byte[]{};

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
    List<TenantMessage> dequeue(Instant now, Duration invisiblePeriod, String queue, int maxKeys) {
        return unpackTenantMessage((List<byte[]>) call(Function.DEQUEUE,
                SLOT,
                bytes(queue),
                bytes(now.toEpochMilli()),
                bytes(now.plus(invisiblePeriod).toEpochMilli()),
                bytes(maxKeys)));
    }

    @SuppressWarnings("unchecked")
    @Override
    Optional<TenantMessage> peek(Instant now, String queue, String tenant) {
        List<TenantMessage> messages = unpackTenantMessage((List<byte[]>) call(Function.PEEK,
                SLOT,
                bytes(queue),
                bytes(tenant),
                bytes(now.toEpochMilli())));
        return messages.stream().findFirst();
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

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private <R> R executeJedis(java.util.function.Function<Jedis, R> jedisFunction) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(dbIndex);
            return jedisFunction.apply(jedis);
        }
    }
}
