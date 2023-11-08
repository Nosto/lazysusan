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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;

/**
 * Java wrapper for executing a Redis script.
 */
abstract class AbstractScript {
    private final Long TRUE = 1L;

    static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] bytes(long l) {
        return Long.toString(l).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] bytes(boolean bool) {
        return bytes(bool ? 1 : 0);
    }

    static List<TenantMessage> unpackTenantMessage(List<byte[]> response) {
        ArrayList<TenantMessage> result = new ArrayList<>(response.size() >> 1);
        Iterator<byte[]> it = response.iterator();
        while (it.hasNext()) {
            result.add(new TenantMessage(new String(it.next(), StandardCharsets.UTF_8),
                    new String(it.next(), StandardCharsets.UTF_8), it.next()));
        }
        return result;
    }

    static QueueStatistics unpackQueueStatistics(List<Object> response) {
        Map<String, TenantStatistics> tenantStatisticsMap = new HashMap<>();
        Iterator<Object> it = response.iterator();
        while (it.hasNext()) {
            TenantStatistics tenantStatistics = new TenantStatistics(
                    new String((byte[]) it.next(), StandardCharsets.UTF_8),
                    (Long) it.next(),
                    (Long) it.next());
            tenantStatisticsMap.put(tenantStatistics.getTenant(), tenantStatistics);
        }
        return new QueueStatistics(tenantStatisticsMap);
    }

    /**
     * Adds a message to a queue.
     *
     * @param now             Time now
     * @param invisiblePeriod When the message becomes visible if the tenant does not have earlier messages in queue.
     * @param queue           The name of the queue.
     * @param tenantMessage   The message to be added.
     * @return The {@link EnqueueResult} which describes how the message was enqueued.
     */
    EnqueueResult enqueue(Instant now, Duration invisiblePeriod, String queue, TenantMessage tenantMessage) {
        Long result = (Long) call(Function.ENQUEUE,
                slot(tenantMessage.getTenant()),
                bytes(queue),
                bytes(now.toEpochMilli()),
                bytes(now.plus(invisiblePeriod).toEpochMilli()),
                bytes(tenantMessage.getTenant()),
                bytes(tenantMessage.getKey()),
                tenantMessage.getPayload());

        switch (result.intValue()) {
            case 0:
                return EnqueueResult.DUPLICATE_INVISIBLE;
            case 1:
                return EnqueueResult.SUCCESS;
            case 2:
                return EnqueueResult.DUPLICATE_OVERWRITTEN;
            default:
                throw new IllegalStateException("Unrecognized result: " + result);
        }
    }

    /**
     * Removes messages from a queue.
     * <p>
     * Removes N messages but guarantees only one message per tenant is removed.
     * This allows easy rate-limit implementation while reducing Redis round-trips.
     * <p>
     * Note that removed messages will become invisible but stay in Redis until
     * {@link #ack} is called.
     *
     * @param now             Time now
     * @param invisiblePeriod When the message becomes visible again if it hasn't been acked in time.
     * @param queue           The name of the queue.
     * @param maxKeys         Maximum number of keys to remove.
     * @return A list of removed messages
     */
    abstract List<TenantMessage> dequeue(Instant now, Duration invisiblePeriod, String queue, int maxKeys);

    /**
     * Returns a message in the queue for the given tenant. The message does not become invisible.
     *
     * @param now    Time now
     * @param queue  The name of the queue.
     * @param tenant The name of the tenant.
     * @return A message for the tenant.
     */
    abstract Optional<TenantMessage> peek(Instant now, String queue, String tenant);

    /**
     * Acks that a message was processed and it can be permanently removed.
     *
     * @param queue  The name of the queue.
     * @param tenant The tenant to whom the message belongs.
     * @param key    The de-duplication key of the message to be acked.
     * @return true if acked message did exist in queue and false otherwise
     */
    boolean ack(String queue, String tenant, String key) {
        return TRUE.equals(call(Function.ACK, slot(tenant), bytes(queue), bytes(tenant), bytes(key)));
    }

    /**
     * Collects statistics for the specified queue.
     *
     * @param queue The name of the queue
     * @return {@link QueueStatistics} for the queue.
     */
    abstract QueueStatistics getQueueStatistics(String queue);

    /**
     * Purges all of the tenant's messages from the specified queue.
     *
     * @param queue  The name of the queue.
     * @param tenant The tenant for whom messages will be purged.
     * @return The total number of messages purged.
     */
    long purge(String queue, String tenant) {
        return (Long) call(Function.PURGE, slot(tenant), bytes(queue), bytes(tenant));
    }

    protected byte[] loadScript() {
        try {
            return IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("/queue.lua")), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load script.", e);
        }
    }

    Object call(Function function, byte[] key, byte[]... args) {
        List<byte[]> argsList = new ArrayList<>(2 + args.length);
        argsList.add(function.getName());
        argsList.add(key);
        argsList.addAll(Arrays.asList(args));
        return evalsha(Collections.singletonList(key), argsList);
    }

    abstract Object evalsha(List<byte[]> keys, List<byte[]> args);

    abstract byte[] slot(String tenant);

    enum Function {
        ENQUEUE("enqueue"),
        DEQUEUE("dequeue"),
        ACK("ack"),
        GET_QUEUE_STATS("queuestats"),
        PEEK("peek"),
        PURGE("purge");

        private final String name;

        Function(String name) {
            this.name = name;
        }

        public byte[] getName() {
            return bytes(name);
        }
    }
}
