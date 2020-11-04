/******************************************************************************
 Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 <p>
 This software is the confidential and proprietary information of
 Nosto Solutions Ltd ("Confidential Information"). You shall not
 disclose such Confidential Information and shall use it only in
 accordance with the terms of the agreement you entered into with
 Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.exceptions.JedisNoScriptException;

class ClusterScript extends AbstractScript {
    private final BinaryJedisCluster jedis;
    private final byte[] source;
    private final byte[] sha;
    /**
     * Provides a random permutation over the shards to avoid thundering herd issues
     * without sacrificing evenly balanced load.
     */
    private final IntSupplier nextSlot;
    private final int numSlots;

    ClusterScript(BinaryJedisCluster jedis, int numSlots) {
        this.jedis = jedis;
        this.numSlots = numSlots;

        source = loadScript();
        sha = jedis.scriptLoad(source, new byte[]{0}); // load it on a random host

        nextSlot = new IntSupplier() {
            private final List<Integer> permutation = IntStream.range(0, numSlots)
                    .boxed()
                    .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                        Collections.shuffle(list);
                        return list;
                    }));
            private final Iterator<Integer> it = Stream.generate(() -> permutation).flatMap(List::stream).iterator();

            @Override
            public int getAsInt() {
                return it.next();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    List<TenantMessage> dequeue(Instant now, Duration invisiblePeriod, String queue, int maxKeys) {
        return unpackTenantMessage(IntStream.range(0, numSlots)
                .map(x -> nextSlot.getAsInt())
                .mapToObj(ClusterScript::bytes)
                .flatMap(key ->
                    ((List<byte[]>) call(Function.DEQUEUE,
                            key,
                            bytes(queue),
                            bytes(now.toEpochMilli()),
                            bytes(now.plus(invisiblePeriod).toEpochMilli()),
                            bytes(maxKeys))).stream())
                .collect(Collectors.toList()));

    }

    @Override
    Optional<TenantMessage> peek(Instant now, String queue, String tenant) {
        @SuppressWarnings("unchecked") List<TenantMessage> messages = unpackTenantMessage(IntStream.range(0, numSlots)
                .map(x -> nextSlot.getAsInt())
                .mapToObj(ClusterScript::bytes)
                .flatMap(key ->
                        ((List<byte[]>) call(Function.PEEK,
                                key,
                                bytes(queue),
                                bytes(tenant),
                                bytes(now.toEpochMilli()))).stream())
                .collect(Collectors.toList()));
        return messages.stream().findFirst();
    }

    @Override
    @SuppressWarnings("unchecked")
    QueueStatistics getQueueStatistics(String queue) {
        return unpackQueueStatistics(IntStream.range(0, numSlots)
                    .map(x -> nextSlot.getAsInt())
                    .mapToObj(ClusterScript::bytes)
                    .flatMap(key ->
                        ((List<byte[]>) call(Function.GET_QUEUE_STATS, key, bytes(queue))).stream())
                .collect(Collectors.toList()));
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
