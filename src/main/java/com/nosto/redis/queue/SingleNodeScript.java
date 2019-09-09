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

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import redis.clients.jedis.BinaryScriptingCommands;

class SingleNodeScript extends AbstractScript {
    private static final byte[] SLOT = new byte[] {};
    private final BinaryScriptingCommands jedis;
    private final byte[] sha;

    SingleNodeScript(BinaryScriptingCommands jedis) throws IOException {
        this.jedis = jedis;
        sha = jedis.scriptLoad(loadScript());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TenantMessage> dequeue(Instant now, String queue, int maxKeys) {
        return unpackTenantMessage((List<byte[]>) call(Function.DEQUEUE,
                SLOT,
                bytes(queue),
                bytes(now.toEpochMilli()),
                bytes(maxKeys)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public QueueStatistics getQueueStatistics(String queue) {
        return unpackQueueStatistics((List<Object>) call(Function.GET_QUEUE_STATS, SLOT, bytes(queue)));
    }

    @Override
    Object evalsha(List<byte[]> keys, List<byte[]> args) {
        return jedis.evalsha(sha, Collections.emptyList(), args);
    }

    @Override
    byte[] slot(String tenant) {
        return SLOT;
    }
}
