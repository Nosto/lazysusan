
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nosto.redis.queue.jackson.PolymorphicJacksonMessageConverter;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.BinaryScriptingCommands;

/**
 * A class for enqeueing and for polling for messages.
 */
public class ConnectionManager {
    private static final Logger logger = LogManager.getLogger(ConnectionManager.class);

    private final AbstractScript script;
    private final MessageConverter messageConverter;
    private final int dequeueSize;
    private final Duration pollPeriod;
    private final Map<String, Map<Class<?>, MessageHandler<?>>> messageHandlers;
    private List<QueuePoller> runningPollers;
    private final ReentrantLock startUpShutdownLock;

    private ThreadPoolExecutor queuePollerThreadPool;

    private ConnectionManager(AbstractScript script,
                              MessageConverter messageConverter,
                              Duration pollPeriod,
                              int dequeueSize,
                              Map<String, Map<Class<?>, MessageHandler<?>>> messageHanders) {
        this.script = script;
        this.messageConverter = messageConverter;
        this.dequeueSize = dequeueSize;
        this.pollPeriod = pollPeriod;
        this.messageHandlers = messageHanders;

        this.runningPollers = new ArrayList<>(messageHandlers.size());
        this.startUpShutdownLock = new ReentrantLock();
    }

    /**
     * @param queueName The queue to send messages to.
     * @param keyFunction The function that provides a unique key for enqueued messages.
     * @param <T> The type of message that will be enqueued.
     * @return
     */
    public <T> MessageSender<T> createSender(String queueName, Function<T, String> keyFunction) {
        return new MessageSenderImpl<>(script, queueName, keyFunction, messageConverter);
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

            if (queuePollerThreadPool != null && queuePollerThreadPool.isShutdown()) {
                throw new IllegalStateException("Already shut down.");
            }

            this.queuePollerThreadPool = new ThreadPoolExecutor(0, messageHandlers.size(), pollPeriod.toMillis(),
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(messageHandlers.size()), new ThreadPoolExecutor.AbortPolicy());

            Random random = new Random();
            for (Map.Entry<String, Map<Class<?>, MessageHandler<?>>> entry : messageHandlers.entrySet()) {
                String queueName = entry.getKey();
                Map<Class<?>, MessageHandler<?>> queueMessageHandlers = entry.getValue();

                QueuePoller queuePoller = new QueuePoller(script, messageConverter, queueName, dequeueSize, pollPeriod,
                        queueMessageHandlers, random);

                logger.debug("Scheduling poller for queue '{}'", queueName);

                queuePollerThreadPool.submit(queuePoller);
                runningPollers.add(queuePoller);
            }
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
            if (queuePollerThreadPool == null) {
                return true;
            }

            queuePollerThreadPool.shutdown();
            runningPollers.forEach(QueuePoller::stop);
            return queuePollerThreadPool.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    /**
     * Stop polling for messages and cancel any message handlers that are currently running.
     * @return A {@link List} of cancelled tasks.
     */
    public List<Runnable> shutdownNow() {
        startUpShutdownLock.lock();
        try {
            if (queuePollerThreadPool == null) {
                return Collections.emptyList();
            }

            runningPollers.forEach(QueuePoller::stop);
            return queuePollerThreadPool.shutdownNow();
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    /**
     * @return {@code true} if any queue is being polled.
     */
    public boolean isRunning() {
        return queuePollerThreadPool != null && !queuePollerThreadPool.isShutdown();
    }

    /**
     * @return A new {@link Factory} for creating a new instance of {@link ConnectionManager}.
     */
    public static Factory factory() {
        return new Factory();
    }

    public static class Factory {
        private AbstractScript script;
        private Duration pollPeriod = Duration.ofMillis(100);
        private int dequeueSize = 100;
        private MessageConverter messageConverter = new PolymorphicJacksonMessageConverter();
        private Map<String, Map<Class<?>, MessageHandler<?>>> messageHandlers = new HashMap<>();

        private Factory() {
        }

        /**
         * Connect to a single instance.
         */
        public Factory withClient(BinaryScriptingCommands redisClient) {
            Objects.requireNonNull(redisClient);

            try {
                return withScript(new SingleNodeScript(redisClient));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect.", e);
            }
        }

        /**
         * Connect to a cluster.
         */
        public Factory withClient(BinaryJedisCluster rediClusterClient, int numberSlots) {
            Objects.requireNonNull(rediClusterClient);

            if (numberSlots <= 0) {
                throw new IllegalArgumentException("numberSlots must be positive: " + numberSlots);
            }

            try {
                return withScript(new ClusterScript(rediClusterClient, numberSlots));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect.", e);
            }
        }

        Factory withScript(AbstractScript script) {
            this.script = Objects.requireNonNull(script);
            return this;
        }

        /**
         * The interval to wait between polling for messages.
         * Default value is 100 milliseconds.
         */
        public Factory withPollPeriod(Duration pollPeriod) {
            this.pollPeriod = Objects.requireNonNull(pollPeriod);
            return this;
        }

        /**
         * @param dequeueSize The amount of messages to attempt to dequeue in each poll.
         */
        public Factory withDequeueSize(int dequeueSize) {
            if (dequeueSize <= 0) {
                throw new IllegalArgumentException("dequeueSize must be positive: " + dequeueSize);
            }

            this.dequeueSize = dequeueSize;
            return this;
        }

        /**
         * Determine how messages are serialised and deserialised.
         * Default value is an instance of {@link PolymorphicJacksonMessageConverter}.
         */
        public Factory withMessageConverter(MessageConverter messageConverter) {
            this.messageConverter = Objects.requireNonNull(messageConverter);
            return this;
        }

        /**
         * Add a new configuration for handling dequeued messages.
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
            Objects.requireNonNull(script, "Redis client was not configured.");

            return new ConnectionManager(script, messageConverter, pollPeriod, dequeueSize, messageHandlers);
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
         * Add a new {@link MessageHandler} for dequeued messages.
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
