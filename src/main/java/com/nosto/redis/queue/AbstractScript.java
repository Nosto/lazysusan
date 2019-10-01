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

import org.apache.commons.io.IOUtils;

/**
 * Java wrapper for executing a Redis script.
 */
abstract class AbstractScript {
    /**
     * Lua return true gets mapped to 1L.
     */
    private static final Long TRUE_RESPONSE = 1L;

    /**
     * Adds a message to a queue.
     *
     * @param now Time now
     * @param queue The name of the queue.
     * @param tenantMessage The message to be added.
     * @return The {@link EnqueueResult} which describes how the message was enqueued.
     */
    public EnqueueResult enqueue(Instant now, String queue, TenantMessage tenantMessage) {
        boolean result = TRUE_RESPONSE.equals(call(Function.ENQUEUE,
                slot(tenantMessage.getTenant()),
                bytes(queue),
                bytes(now.toEpochMilli()),
                bytes(now.plus(tenantMessage.getInvisiblePeriod()).toEpochMilli()),
                bytes(tenantMessage.getTenant()),
                bytes(tenantMessage.getKey()),
                tenantMessage.getPayload()));

        if (result) {
            return EnqueueResult.SUCCESS;
        }

        return EnqueueResult.DUPLICATE_OVERWRITTEN;
    }

    /**
     * Removes messages from a queue.
     *
     * Removes N messages but guarantees only one message per tenant is removed.
     * This allows easy rate-limit implementation while reducing Redis round-trips.
     *
     * Note that removed messages will become invisible but stay in Redis until
     * {@link #ack} is called.
     *
     * @param now Time now
     * @param queue The name of the queue.
     * @param maxKeys Maximum number of keys to remove.
     * @param invisiblePeriod The duration messages should remain invisible after being dequeued.
     * @return A list of removed messages
     */
    public abstract List<TenantMessage> dequeue(Instant now, String queue, int maxKeys, Duration invisiblePeriod);

    /**
     * Acks that a message was processed and it can be permanently removed.
     *
     * @param queue The name of the queue.
     * @param tenant The tenant to whom the message belongs.
     * @param key The de-duplication key of the message to be acked.
     */
    public void ack(String queue, String tenant, String key) {
        call(Function.ACK, slot(tenant), bytes(queue), bytes(tenant), bytes(key));
    }

    /**
     * Collects statistics for the specified queue.
     *
     * @param queue The name of the queue
     * @return {@link QueueStatistics} for the queue.
     */
    public abstract QueueStatistics getQueueStatistics(String queue);

    protected byte[] loadScript() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("/queue.lua"), StandardCharsets.UTF_8)
                .getBytes(StandardCharsets.UTF_8);
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

    static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] bytes(long l) {
        return Long.toString(l).getBytes(StandardCharsets.UTF_8);
    }

    static List<TenantMessage> unpackTenantMessage(List<byte[]> response) {
        ArrayList<TenantMessage> result = new ArrayList<>(response.size() >> 1);
        Iterator<?> it = response.iterator();
        while (it.hasNext()) {
            result.add(new TenantMessage(
                    new String((byte[]) it.next()),
                    new String((byte[]) it.next()),
                    (byte[]) it.next(),
                    Duration.ofMillis((Long) it.next())));
        }
        return result;
    }

    static QueueStatistics unpackQueueStatistics(List<Object> response) {
        Map<String, TenantStatistics> tenantStatisticsMap = new HashMap<>();
        Iterator<Object> it = response.iterator();
        while (it.hasNext()) {
            TenantStatistics tenantStatistics = new TenantStatistics(
                    new String((byte[]) it.next()),
                    (Long) it.next(),
                    (Long) it.next());
            tenantStatisticsMap.put(tenantStatistics.getTenant(), tenantStatistics);
        }
        return new QueueStatistics(tenantStatisticsMap);
    }

    enum Function {
        ENQUEUE("enqueue"),
        DEQUEUE("dequeue"),
        ACK("ack"),
        GET_QUEUE_STATS("queuestats");

        private final byte[] name;

        Function(String name) {
            this.name = bytes(name);
        }

        public byte[] getName() {
            return name;
        }
    }

    public static class TenantMessage {
        private final String tenant;
        private final String key;
        private final byte[] payload;
        private final Duration invisiblePeriod;

        TenantMessage(String tenant, String key, byte[] payload, Duration invisiblePeriod) {
            this.tenant = tenant;
            this.key = key;
            this.payload = payload;
            this.invisiblePeriod = invisiblePeriod;
        }

        public String getTenant() {
            return tenant;
        }

        public String getKey() {
            return key;
        }

        public byte[] getPayload() {
            return payload;
        }

        public Duration getInvisiblePeriod() {
            return invisiblePeriod;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantMessage that = (TenantMessage) o;
            return tenant.equals(that.tenant) &&
                    key.equals(that.key) &&
                    Arrays.equals(payload, that.payload) &&
                    invisiblePeriod.equals(that.invisiblePeriod);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(tenant, key, invisiblePeriod);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }

        @Override
        public String toString() {
            return "TenantMessage{" +
                    "tenant='" + tenant + '\'' +
                    ", key='" + key + '\'' +
                    ", payload=" + Arrays.toString(payload) +
                    ", invisiblePeriod=" + invisiblePeriod +
                    '}';
        }
    }
}
