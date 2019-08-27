/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.BinaryScriptingCommands;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class SingleNodeScript extends AbstractScript {
    private static final byte[] SLOT = new byte[] {};
    private final BinaryScriptingCommands jedis;
    private final byte[] sha;

    public SingleNodeScript(BinaryScriptingCommands jedis) throws IOException {
        this.jedis = jedis;
        sha = jedis.scriptLoad(IOUtils.toByteArray(getClass().getResourceAsStream("/queue.lua")));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TenantMessage> dequeue(Instant now, String queue, int maxKeys) {
        return unpack((List<byte[]>) call(Function.DEQUEUE, SLOT, bytes(queue), bytes(now.toEpochMilli()), bytes(maxKeys)));
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
