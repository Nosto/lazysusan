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
     *
     * @param queueName       The name of the queue.
     * @param jedisPool       The Redis node to connect to.
     * @param dbIndex         The index of the DB to use.
     * @param dequeueStrategy Defines how dequeue behaves if more messages are dequeued than there are tenants in queue.
     */
    public MultitenantQueue(String queueName,
                            JedisPool jedisPool,
                            int dbIndex,
                            DequeueStrategy dequeueStrategy) {
        this(queueName, new SingleNodeScript(jedisPool, dbIndex, dequeueStrategy));
    }

    /**
     * Connect the multi-tenant queue to a Redis cluster.
     *
     * @param queueName       The name of the queue.
     * @param jedisCluster    The Redis cluster to connect to.
     * @param shards          The number of shards used for balancing the data across the cluster.
     * @param dequeueStrategy Defines how dequeue behaves if more messages are dequeued than there are tenants in queue.
     */
    public MultitenantQueue(String queueName,
                            JedisCluster jedisCluster,
                            int shards,
                            DequeueStrategy dequeueStrategy) {
        this(queueName, new ClusterScript(jedisCluster, shards, dequeueStrategy));
    }

    MultitenantQueue(String queueName,
                     AbstractScript redisScript) {
        this.queueName = Objects.requireNonNull(queueName);
        this.redisScript = redisScript;
    }

    /**
     * Enqueue a message for a tenant.
     *
     * @param tenantMessage   The message to be enqueued.
     * @param dequeueInterval The interval at which the tenant's messages can be de-queued. If the value is
     *                        10 milliseconds, a tenant's message can be de-queued once every 10 milliseconds.
     *                        This value overwrites the value used to enqueue previous messages for the same tenant.
     * @return How the message was enqueued.
     */
    @SuppressWarnings("UnusedReturnValue")
    public EnqueueResult enqueue(TenantMessage tenantMessage, Duration dequeueInterval) {
        return redisScript.enqueue(Instant.now(), dequeueInterval, queueName, tenantMessage);
    }

    /**
     * De-queue messages.
     *
     * @param messageInvisibilityPeriod The amount of time before a message can be de-queued again. To avoid the message
     *                                  being processed more than once, it should be deleted before {@code messageInvisibilityPeriod}
     *                                  has elapsed by calling {@link #delete(String, String)}.
     * @param maximumMessages           The maximum number of messages to de-queue. When connecting to a Redis cluster,
     *                                  this number is multiplied by the number of shards because each shard is de-queued.
     * @return De-queued messages.
     */
    public List<TenantMessage> dequeue(Duration messageInvisibilityPeriod, int maximumMessages) {
        return redisScript.dequeue(Instant.now(), messageInvisibilityPeriod, queueName, maximumMessages);
    }

    /**
     * Delete a message from the queue.
     *
     * @param tenant     The tenant associated with the message.
     * @param messageKey The key that identifies the message.
     * @return true if deleted message did exist in queue and false otherwise
     *         If processing of dequeued message takes more time than defined invisibility period
     *         then same message can be dequeued again after invisibility period has passed. This
     *         can result to situation where multiple workers read same message and try to delete it
     *         after processing. If deleted message does not exist it indicates that it was dequeued
     *         and deleted already before meaning that invisibility period might be too short.
     */
    public boolean delete(String tenant, String messageKey) {
        return redisScript.ack(queueName, tenant, messageKey);
    }

    /**
     * Fetch a message from the queue. Unlike {@link #dequeue(Duration, int)}, the message does not become invisible.
     *
     * @param tenant The tenant associated with the message.
     * @return A message at the top of the queue, if one exists for the given tenant.
     */
    public Optional<TenantMessage> peek(String tenant) {
        return redisScript.peek(Instant.now(), queueName, tenant);
    }

    /**
     * Message counts for this queue.
     *
     * @return Message counts for this queue.
     */
    public QueueStatistics getStatistics() {
        return redisScript.getQueueStatistics(queueName);
    }

    /**
     * Purges all of the tenant's messages.
     *
     * @param tenant The tenant for whom messages will be purged.
     * @return The total number of messages purged.
     */
    public long purge(String tenant) {
        return redisScript.purge(queueName, tenant);
    }
}
