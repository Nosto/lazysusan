
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
import java.util.function.Function;

import com.nosto.redis.queue.jackson.PolymorphicJacksonMessageConverter;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.BinaryScriptingCommands;

public class ConnectionManager {
    private final AbstractScript redisScript;
    private final MessageConverter messageConverter;
    private final Map<String, QueueHandlerConfiguration> queueMessageHandlers;

    private final MessagePoller messagePoller;

    private ConnectionManager(AbstractScript redisScript,
                              Duration pollPeriod,
                              MessageConverter messageConverter,
                              Map<String, QueueHandlerConfiguration> messageHanders) {
        this.redisScript = redisScript;
        this.messageConverter = messageConverter;
        this.queueMessageHandlers = messageHanders;

        messagePoller = new MessagePoller(redisScript, pollPeriod, messageConverter, queueMessageHandlers);
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
     * @throws IllegalStateException if no queue message handlers have been configured.
     */
    public void start() {
        messagePoller.start();
    }

    /**
     * Stop polling for messages.
     * @param timeout The amount of time to wait for polling and message handling to stop.
     * @return {@code true} if shut down has completed within {@code timeout}.
     * @throws IllegalStateException if {@link #start()} was not previously called.
     */
    public boolean shutdown(Duration timeout) {
        return shutdown(messagePoller -> messagePoller.shutdown(timeout));
    }

    /**
     * Stop polling for messages and cancel any message handlers that are currently running.
     * @return A {@link List} of cancelled message handlers for each queue.
     */
    public Map<String, List<Runnable>> shutdownNow() {
        return shutdown(MessagePoller::shutdownNow);
    }

    private <T> T shutdown(Function<MessagePoller, T> shutdownFunction) {
        return shutdownFunction.apply(messagePoller);
    }

    /**
     * @return true if Redis is being polled or if any dequeued messages are being handled.
     */
    public boolean isRunning() {
        return messagePoller.isRunning();
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
        private MessageConverter messageConverter = new PolymorphicJacksonMessageConverter();
        private Map<String, QueueHandlerConfiguration> queueHandlerConfigurations = new HashMap<>();

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
        public QueueHandlerConfigurationFactory withQueueHandler(String queueName, int maxConcurrentHandlers) {
            return new QueueHandlerConfigurationFactory(this, queueName, maxConcurrentHandlers);
        }

        private Factory withMessageHandler(String queueName,
                                          int maxConcurrentHandlers,
                                          Map<Class<?>, MessageHandler<?>> messageHandlers) {


            if (messageHandlers.containsKey(queueName)) {
                throw new IllegalArgumentException("Message handlers already added for queue " + queueName);
            }

            this.queueHandlerConfigurations.put(queueName, new QueueHandlerConfiguration(maxConcurrentHandlers, messageHandlers));
            return this;
        }

        public ConnectionManager build() {
            Objects.requireNonNull(redisScript, "Redis client was not configured.");

            return new ConnectionManager(redisScript, pollPeriod, messageConverter, queueHandlerConfigurations);
        }
    }

    public static class QueueHandlerConfigurationFactory {
        private final Factory connectionManagerFactory;
        private final String queueName;
        private final int maxConcurrentHandlers;
        private final Map<Class<?>, MessageHandler<?>> messageHandlers;

        private QueueHandlerConfigurationFactory(Factory connectionManagerFactory,
                                                 String queueName,
                                                 int maxConcurrentHandlers) {
            this.connectionManagerFactory = connectionManagerFactory;

            this.queueName = Objects.requireNonNull(queueName)
                    .trim()
                    .toLowerCase();

            if (maxConcurrentHandlers <= 0) {
                throw new IllegalArgumentException("maxConcurrentHandlers must be positive: " + maxConcurrentHandlers);
            }

            this.maxConcurrentHandlers = maxConcurrentHandlers;

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

            return connectionManagerFactory.withMessageHandler(queueName, maxConcurrentHandlers, messageHandlers);
        }
    }
}
