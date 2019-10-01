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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosto.redis.queue.jackson.PolymorphicJacksonMessageConverter;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * A class for enqueuing and for polling for messages.
 */
public final class ConnectionManager {
    private static final Logger LOGGER = LogManager.getLogger(ConnectionManager.class);

    private final AbstractScript script;
    private final MessageConverter messageConverter;
    private final Duration waitAfterEmptyDequeue;
    private Map<String, MessageHandlers> messageHandlers;
    private final List<QueuePoller> runningPollers;
    private final ReentrantLock startUpShutdownLock;

    private ExecutorService queuePollerThreadPool;

    private ConnectionManager(AbstractScript script,
                              MessageConverter messageConverter,
                              Duration waitAfterEmptyDequeue,
                              Map<String, MessageHandlers> messageHandlers) {
        this.script = script;
        this.messageConverter = messageConverter;
        this.waitAfterEmptyDequeue = waitAfterEmptyDequeue;
        this.messageHandlers = messageHandlers;

        this.runningPollers = new ArrayList<>(this.messageHandlers.size());
        this.startUpShutdownLock = new ReentrantLock();
    }

    /**
     * @param queueName The queue to send messages to.
     * @param keyFunction The function that provides a unique key for enqueued messages.
     * @param <T> The type of message that will be enqueued.
     * @return A new instance of {@link MessageHandler} for the given queue.
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

            this.queuePollerThreadPool = Executors.newFixedThreadPool(messageHandlers.size());

            Random random = new Random();
            for (Map.Entry<String, MessageHandlers> entry : messageHandlers.entrySet()) {
                String queueName = entry.getKey();

                QueuePoller queuePoller = new QueuePoller(script, messageConverter, queueName, waitAfterEmptyDequeue,
                        entry.getValue(), random);

                LOGGER.debug("Scheduling poller for queue '{}'", queueName);

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
     * @throws InterruptedException if interrupted while waiting.
     */
    public boolean shutdown(Duration timeout) throws InterruptedException {
        startUpShutdownLock.lock();
        try {
            if (queuePollerThreadPool == null) {
                return true;
            }

            runningPollers.forEach(QueuePoller::stop);
            queuePollerThreadPool.shutdown();
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
     * @param queueName The name of the queue
     * @return The queue's statistics.
     */
    public QueueStatistics getQueueStatistics(String queueName) {
        return script.getQueueStatistics(queueName);
    }

    /**
     * @return A new {@link Factory} for creating a new instance of {@link ConnectionManager}.
     */
    public static Factory factory() {
        return new Factory();
    }

    /**
     * Builds a new instance of {@link ConnectionManager}.
     */
    public static final class Factory {
        private AbstractScript script;
        private Duration waitAfterEmptyDequeue = Duration.ofMillis(100);
        private MessageConverter messageConverter;
        private Map<String, MessageHandlers> messageHandlers = new HashMap<>();

        private Factory() {
        }

        /**
         * Connect to a single instance.
         * @param redisClient The client for connecting to the single Redis node.
         * @param dbIndex The db to select before excecuting commands
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code redisClient} is {@code null}.
         * @throws IllegalArgumentException if {@code dbIndex} is less than zero.
         */
        public Factory withClient(JedisPool redisClient, int dbIndex) {
            Objects.requireNonNull(redisClient);

            if (dbIndex < 0) {
                throw new IllegalArgumentException("dbIndex must greater or equal to zero: " + dbIndex);
            }

            try {
                return withScript(new SingleNodeScript(redisClient, dbIndex));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect.", e);
            }
        }

        /**
         * Connect to a cluster.
         * @param rediClusterClient The client for connecting to the Redis cluster.
         * @param numberSlots The number of slots in the cluster.
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code rediClusterClient} is {@code null}.
         * @throws IllegalArgumentException if {@code numberSlots} is less than or equal to zero.
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
         * The interval to wait after a dequeue returns no messages.
         * Default value is 100 milliseconds.
         * @param waitAfterEmptyDequeue The {@link Duration} to wait if no messages were returned by the previous
         *                             dequeue.
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code waitAfterEmptyDequeue} is {@code null}.
         */
        public Factory withWaitAfterEmptyDequeue(Duration waitAfterEmptyDequeue) {
            this.waitAfterEmptyDequeue = Objects.requireNonNull(waitAfterEmptyDequeue);
            return this;
        }

        /**
         * Determine how messages are serialized and deserialized.
         * Default value is an instance of {@link PolymorphicJacksonMessageConverter}.
         * @param messageConverter A {@link MessageConverter} for serializing and deserializing messages.
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code messageConverter} is {@code null}
         */
        public Factory withMessageConverter(MessageConverter messageConverter) {
            this.messageConverter = Objects.requireNonNull(messageConverter);
            return this;
        }

        /**
         * Use {@link PolymorphicJacksonMessageConverter} for serializing and deserializing messages.
         * @return Current {@link Factory} instance.
         */
        public Factory withPolymorphicJacksonMessageConverter() {
            this.messageConverter = new PolymorphicJacksonMessageConverter(Function.identity());
            return this;
        }

        /**
         * Use {@link PolymorphicJacksonMessageConverter} for serializing and deserializing messages.
         * @param customizer Customize {@link PolymorphicJacksonMessageConverter}'s {@link ObjectMapper}.
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code customizer} is {@code null}.
         */
        public Factory withPolymorphicJacksonMessageConverter(Function<ObjectMapper, ObjectMapper> customizer) {
            Objects.requireNonNull(customizer);
            this.messageConverter = new PolymorphicJacksonMessageConverter(customizer);
            return this;
        }

        /**
         * Set the message handlers for a queue.
         * @param queueName The name of the queue.
         * @param afterDequeueInvisiblePeriod Period that messages should be invisible after they are dequeued.
         * @param messageHandlerExecutor The {@link ThreadPoolExecutor} for executing message handlers.
         * @param messageHandlers The handlers for handling messages from the queue. At least one must be defined.
         * @return Current {@link Factory} instance.
         * @throws NullPointerException if {@code queueName} is {@code null}.
         * @throws IllegalArgumentException if {@code queueName} was already set.
         * @throws IllegalStateException if {@code messageHandlerExecutor} has been shut down.
         * @throws IllegalStateException if no message handlers are set.
         * @throws IllegalArgumentException if more than one {@link MessageHandler} returns the same class
         * for {@link MessageHandler#getMessageClass()}
         */
        public Factory withMessageHandlers(String queueName,
                                           Duration afterDequeueInvisiblePeriod,
                                           ThreadPoolExecutor messageHandlerExecutor,
                                           MessageHandler<?>... messageHandlers) {
            queueName = Objects.requireNonNull(queueName).trim();

            Objects.requireNonNull(messageHandlerExecutor);
            if (messageHandlerExecutor.isShutdown()) {
                throw new IllegalStateException("messageHandlerExecutor has been shut down.");
            }

            if (this.messageHandlers.containsKey(queueName)) {
                throw new IllegalArgumentException("Message handlers have already been set for " + queueName);
            }

            if (messageHandlers.length == 0) {
                throw new IllegalArgumentException("No message handlers defined.");
            }

            Stream.of(messageHandlers)
                    .collect(Collectors.groupingBy(MessageHandler::getMessageClass))
                    .forEach((messageClass, handlers) -> {
                        if (handlers.size() > 1) {
                            String message = String.format("More than one MessageHandler handling the same class: %s",
                                    messageClass);
                            throw new IllegalArgumentException(message);
                        }
                    });

            this.messageHandlers.put(queueName,
                    new MessageHandlers(afterDequeueInvisiblePeriod, messageHandlerExecutor, messageHandlers));
            return this;
        }

        /**
         * @return A new instance of {@link ConnectionManager}.
         * @throws NullPointerException if a client was not configured.
         * @throws NullPointerException if a message converter was not configured.
         */
        public ConnectionManager build() {
            Objects.requireNonNull(script, "Redis client was not configured.");
            Objects.requireNonNull(messageConverter, "MessageConverter was not configured.");

            return new ConnectionManager(script, messageConverter, waitAfterEmptyDequeue, messageHandlers);
        }
    }

    protected static final class MessageHandlers {
        private final Duration afterDequeueInvisiblePeriod;
        private final ThreadPoolExecutor messageHandlerExecutor;
        private final Map<Class<?>, MessageHandler<?>> messageHandlers;

        private MessageHandlers(Duration afterDequeueInvisiblePeriod,
                                ThreadPoolExecutor messageHandlerExecutor,
                                MessageHandler<?>... messageHandlers) {
            this.afterDequeueInvisiblePeriod = afterDequeueInvisiblePeriod;
            this.messageHandlerExecutor = messageHandlerExecutor;
            this.messageHandlers = Stream.of(messageHandlers)
                    .collect(Collectors.toMap(MessageHandler::getMessageClass, Function.identity()));
        }

        protected Duration getAfterDequeueInvisiblePeriod() {
            return afterDequeueInvisiblePeriod;
        }

        protected ThreadPoolExecutor getMessageHandlerExecutor() {
            return messageHandlerExecutor;
        }

        public Map<Class<?>, MessageHandler<?>> getMessageHandlers() {
            return messageHandlers;
        }
    }
}
