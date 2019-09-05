
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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.nosto.redis.queue.jackson.PolymorphicJacksonMessageConverter;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.BinaryScriptingCommands;

public class ConnectionManager {
    private final AbstractScript redisScript;
    private final MessageConverter messageConverter;
    private final int dequeueSize;
    private final Duration pollPeriod;
    private final Map<String, Map<Class<?>, MessageHandler<?>>> messageHandlers;
    private final ScheduledThreadPoolExecutor queuePollerThreadPool;
    private final ReentrantLock startUpShutdownLock;

    private ConnectionManager(AbstractScript redisScript,
                              MessageConverter messageConverter,
                              Duration pollPeriod,
                              int dequeueSize,
                              Map<String, Map<Class<?>, MessageHandler<?>>> messageHanders) {
        this.redisScript = redisScript;
        this.messageConverter = messageConverter;
        this.dequeueSize = dequeueSize;
        this.pollPeriod = pollPeriod;
        this.messageHandlers = messageHanders;

        this.queuePollerThreadPool = new ScheduledThreadPoolExecutor(messageHanders.size());
        this.startUpShutdownLock = new ReentrantLock();
    }

    /**
     * @param queueName The queue to send messages to.
     * @param keyFunction The function that provides a unique key for enqueued messages.
     * @param <T> The type of message that will be enqueued.
     * @return
     */
    public <T> MessageSender<T> createSender(String queueName, Function<T, String> keyFunction) {
        return new MessageSenderImpl<>(redisScript, queueName, keyFunction, messageConverter);
    }

    /**
     * Start polling for messages.
     * @throws IllegalStateException if {@link #start()} was previously called.
     * @throws IllegalStateException if {@link #shutdown(Duration)}} or {@link #shutdownNow()} were previously called.
     * @throws IllegalStateException if no queue message handlers have been configured.
     */
    public void start() {
        startUpShutdownLock.lock();
        try {
            if (messageHandlers.isEmpty()) {
                throw new IllegalStateException("No queue message handlers have been defined.");
            }

            if (isRunning()) {
                throw new IllegalStateException("Already running.");
            }

            if (queuePollerThreadPool.isShutdown()) {
                throw new IllegalStateException("Already shut down.");
            }

            messageHandlers.forEach((queueName, queueMessageHandlers) -> {
                QueuePoller queuePoller =
                        new QueuePoller(redisScript, messageConverter, queueName, dequeueSize, queueMessageHandlers);

                queuePollerThreadPool.scheduleAtFixedRate(queuePoller,
                        pollPeriod.toMillis(),
                        pollPeriod.toMillis(),
                        TimeUnit.MILLISECONDS);
            });
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    /**
     * Stop polling for messages.
     * @param timeout The amount of time to wait for polling and message handling to stop.
     * @return {@code true} if shut down has completed within {@code timeout}.
     */
    public boolean shutdown(Duration timeout) throws InterruptedException {
        startUpShutdownLock.lock();
        try {
            queuePollerThreadPool.shutdown();
            return queuePollerThreadPool.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    /**
     * Stop polling for messages and cancel any message handlers that are currently running.
     * @return A {@link List} of cancelled message handlers for each queue.
     */
    public List<Runnable> shutdownNow() {
        startUpShutdownLock.lock();
        try {
            return queuePollerThreadPool.shutdownNow();
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    /**
     * @return true if Redis is being polled or if any dequeued messages are being handled.
     */
    public boolean isRunning() {
        return !queuePollerThreadPool.isShutdown() && queuePollerThreadPool.getTaskCount() > 0L;
    }

    /**
     * @return A new {@link Factory} for creating a new instance of {@link ConnectionManager}.
     */
    public static Factory factory() {
        return new Factory();
    }

    public static class Factory {
        private AbstractScript redisScript;
        private Duration pollPeriod = Duration.ofMillis(100);
        private int dequeueSize = 100;
        private MessageConverter messageConverter = new PolymorphicJacksonMessageConverter();
        private Map<String, Map<Class<?>, MessageHandler<?>>> messageHandlers = new HashMap<>();

        private Factory() {
        }

        /**
         * Connect to a single Redis instance.
         */
        public Factory withRedisClient(BinaryScriptingCommands redisClient) {
            Objects.requireNonNull(redisClient);

            try {
                return withRedisScript(new SingleNodeScript(redisClient));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect to redis.", e);
            }
        }

        /**
         * Connect to a Redis cluster.
         */
        public Factory withRedisClient(BinaryJedisCluster rediClusterClient, int numberSlots) {
            Objects.requireNonNull(rediClusterClient);

            if (numberSlots <= 0) {
                throw new IllegalArgumentException("numberSlots must be positive: " + numberSlots);
            }

            try {
                return withRedisScript(new ClusterScript(rediClusterClient, numberSlots));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect to redis.", e);
            }
        }

        Factory withRedisScript(AbstractScript redisScript) {
            this.redisScript = Objects.requireNonNull(redisScript);
            return this;
        }

        /**
         * The interval to wait between polling Redis for new messages.
         * Default value is 100 milliseconds.
         */
        public Factory withPollPeriod(Duration pollPeriod) {
            this.pollPeriod = Objects.requireNonNull(pollPeriod);
            return this;
        }

        public Factory withDequeueSize(int dequeueSize) {
            if (dequeueSize <= 0) {
                throw new IllegalArgumentException("dequeueSize must be positive: " + dequeueSize);
            }

            this.dequeueSize = dequeueSize;
            return this;
        }

        /**
         * Determine how messages are serialised to Redis and deserialised from Redis.
         * Default value is an instance of {@link PolymorphicJacksonMessageConverter}.
         */
        public Factory withMessageConverter(MessageConverter messageConverter) {
            this.messageConverter = Objects.requireNonNull(messageConverter);
            return this;
        }

        /**
         * Add a new configuration for handling queue messages.
         */
        public QueueHandlerConfigurationFactory withQueueHandler(String queueName) {
            return new QueueHandlerConfigurationFactory(this, queueName);
        }

        private Factory withMessageHandler(String queueName, Map<Class<?>, MessageHandler<?>> messageHandlers) {
            if (messageHandlers.containsKey(queueName)) {
                throw new IllegalArgumentException("Message handlers already added for queue " + queueName);
            }

            this.messageHandlers.put(queueName, messageHandlers);
            return this;
        }

        public ConnectionManager build() {
            Objects.requireNonNull(redisScript, "Redis client was not configured.");

            return new ConnectionManager(redisScript, messageConverter, pollPeriod, dequeueSize, messageHandlers);
        }
    }

    public static class QueueHandlerConfigurationFactory {
        private final Factory connectionManagerFactory;
        private final String queueName;
        private final Map<Class<?>, MessageHandler<?>> messageHandlers;

        private QueueHandlerConfigurationFactory(Factory connectionManagerFactory, String queueName) {
            this.connectionManagerFactory = connectionManagerFactory;

            this.queueName = Objects.requireNonNull(queueName)
                    .trim()
                    .toLowerCase();

            this.messageHandlers = new HashMap<>();
        }

        /**
         * Add a new {@link MessageHandler} for this queue.
         */
        public <T> QueueHandlerConfigurationFactory withMessageHandler(Class<T> messageClass, MessageHandler<T> messageHandler) {
            messageHandlers.put(messageClass, messageHandler);
            return this;
        }

        public ConnectionManager.Factory build() {
            if (messageHandlers.isEmpty()) {
                throw new IllegalArgumentException("No message handlers were configured.");
            }

            return connectionManagerFactory.withMessageHandler(queueName, messageHandlers);
        }
    }
}
