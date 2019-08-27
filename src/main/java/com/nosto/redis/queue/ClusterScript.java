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
import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ClusterScript extends AbstractScript {
    private final BinaryJedisCluster jedis;
    private final byte[] source;
    private final byte[] sha;
    /**
     * Provides a random permutation over the shards to avoid thundering herd issues
     * without sacrificing evenly balanced load.
     */
    private final IntSupplier nextSlot;
    private final int numSlots;

    public ClusterScript(BinaryJedisCluster jedis, int numSlots) throws IOException {
        this.jedis = jedis;
        this.numSlots = numSlots;

        source = IOUtils.toByteArray(getClass().getResourceAsStream("/queue.lua"));
        sha = jedis.scriptLoad(source, new byte[]{0}); // load it on a random host

        nextSlot = new IntSupplier() {
            final List<Integer> permutation = IntStream.range(0, numSlots)
                    .boxed()
                    .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                        Collections.shuffle(list);
                        return list;
                    }));
            final Iterator<Integer> it = Stream.generate(() -> permutation).flatMap(List::stream).iterator();

            @Override
            public int getAsInt() {
                return it.next();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TenantMessage> dequeue(Instant now, String queue, int maxKeys) {
        return unpack(IntStream.range(0, numSlots).map(x -> nextSlot.getAsInt()).mapToObj(ClusterScript::bytes).flatMap(key ->
                ((List<byte[]>) call(Function.DEQUEUE, key, bytes(queue), bytes(now.toEpochMilli()), bytes(maxKeys))).stream()).collect(Collectors.toList()));

    }

    /**
     * Evaluates the script on a host. The script loading is lazy,
     * in case the script is not loaded load it
     */
    @Override
    Object evalsha(final List<byte[]> keys, final List<byte[]> args) {
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException e) {
            keys.forEach(key -> jedis.scriptLoad(source, key));
            return jedis.evalsha(sha, keys, args);
        }
    }

    byte[] slot(String tenant) {
        return bytes(Math.floorMod(tenant.hashCode(), numSlots));
    }
}
