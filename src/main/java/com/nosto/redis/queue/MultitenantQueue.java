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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * Enqueue, de-queue and delete messages for multiple tenants.
 */
public class MultitenantQueue {
    private final String queueName;
    private final AbstractScript redisScript;

    /**
     * Connect the multi-tenant queue to a single Redis node.
     * @param queueName The name of the queue.
     * @param jedisPool The Redis node to connect to.
     * @param dbIndex The index of the DB to use.
     */
    public MultitenantQueue(String queueName,
                            JedisPool jedisPool,
                            int dbIndex) {
        this(queueName, new SingleNodeScript(jedisPool, dbIndex));
    }

    /**
     * Connect the multi-tenant queue to a Redis cluster.
     * @param queueName The name of the queue.
     * @param jedisCluster The Redis cluster to connect to.
     * @param shards The number of shards used for balancing the data across the cluster.
     */
    public MultitenantQueue(String queueName,
                            JedisCluster jedisCluster,
                            int shards) {
        this(queueName, new ClusterScript(jedisCluster, shards));
    }

    MultitenantQueue(String queueName, AbstractScript redisScript) {
        this.queueName = Objects.requireNonNull(queueName);
        this.redisScript = redisScript;
    }

    /**
     * Enqueue a message for a tenant.
     * @param tenantMessage The message to be enqueued.
     * @param dequeueIntervalMillis The interval at which the tenant's messages can be de-queued. If the value is 10,
     *                              a tenant's message can be de-queued once every 10 milliseconds. This value
     *                              overwrites the value used to enqueue previous messages for the same tenant.
     * @return How the message was enqueued.
     */
    public EnqueueResult enqueue(TenantMessage tenantMessage, long dequeueIntervalMillis) {
        return redisScript.enqueue(Instant.now(), dequeueIntervalMillis, queueName, tenantMessage);
    }

    /**
     * De-queue messages.
     * @param messageInvisibilityPeriod The amount of time before a message can be de-queued again. To avoid the message
     *        being processed more than once, it should be deleted before {@code messageInvisibilityPeriod}
     *        has elapsed by calling {@link #delete(String, String)}.
     * @param maximumMessages The maximum number of messages to de-queue. When connecting to a Redis cluster,
     *        this number is multiplied by the number of shards because each shard is de-queued.
     * @return De-queued messages.
     */
    public List<TenantMessage> dequeue(Duration messageInvisibilityPeriod, int maximumMessages) {
        return redisScript.dequeue(Instant.now(), messageInvisibilityPeriod, queueName, maximumMessages);
    }

    /**
     * Delete a message from the queue.
     * @param tenant The tenant associated with the message.
     * @param messageKey The key that identifies the message.
     */
    public void delete(String tenant, String messageKey) {
        redisScript.ack(queueName, tenant, messageKey);
    }

    /**
     * Fetch a message from the queue. Unlike {@link #dequeue(Duration, int)}, the message does not become invisible.
     * @param tenant The tenant associated with the message.
     * @return A message at the top of the queue, if one exists for the given tenant.
     */
    public Optional<TenantMessage> peek(String tenant) {
        return redisScript.peek(Instant.now(), queueName, tenant);
    }

    /**
     * @return Message counts for this queue.
     */
    public QueueStatistics getStatistics() {
        return redisScript.getQueueStatistics(queueName);
    }

    /**
     * Purges all of the tenant's messages.
     * @param tenant The tenant for whom messages will be purged.
     * @return The total number of messages purged.
     */
    public long purge(String tenant) {
        return redisScript.purge(queueName, tenant);
    }
}
