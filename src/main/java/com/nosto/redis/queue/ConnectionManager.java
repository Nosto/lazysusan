
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
import java.util.Timer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.nosto.redis.queue.jackson.PolymorphicJacksonMessageConverter;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.BinaryScriptingCommands;

public class ConnectionManager {
    private final AbstractScript redisScript;
    private final Duration pollDuration;
    private final MessageConverter messageConverter;
    private final Map<String, QueueMessageHandlers> queueMessageHandlers;

    private final ReentrantLock startUpShutdownLock;
    private final MessagePoller messagePoller;

    private Timer timer;

    private ConnectionManager(AbstractScript redisScript,
                              Duration pollDuration,
                              MessageConverter messageConverter,
                              Map<String, QueueMessageHandlers> messageHanders) {
        this.redisScript = redisScript;
        this.pollDuration = pollDuration;
        this.messageConverter = messageConverter;
        this.queueMessageHandlers = messageHanders;

        startUpShutdownLock = new ReentrantLock();
        messagePoller = new MessagePoller(redisScript, messageConverter, queueMessageHandlers);
    }

    public <T> MessageSender<T> createSender(String queueName, Function<T, String> keyFunction) {
        return new MessageSenderImpl<>(redisScript, queueName, keyFunction, messageConverter);
    }

    public void start() {
        startUpShutdownLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("ConnectionManager has already started.");
            }

            if (queueMessageHandlers.isEmpty()) {
                throw new IllegalStateException("No message handlers have been configured.");
            }

            timer = new Timer();
            timer.scheduleAtFixedRate(messagePoller, 1, pollDuration.toMillis());
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    public boolean shutdown(Duration timeout) {
        return shutdown(messagePoller -> messagePoller.shutdown(timeout));
    }

    public Map<String, List<Runnable>> shutdownNow() {
        return shutdown(MessagePoller::shutdownNow);
    }

    private <T> T shutdown(Function<MessagePoller, T> shutdownFunction) {
        startUpShutdownLock.lock();
        try {
            if (timer == null) {
                throw new IllegalStateException("ConnectionManager is not running.");
            }

            timer.cancel();

            return shutdownFunction.apply(messagePoller);
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    public boolean isRunning() {
        return timer != null || messagePoller.getActiveMessageHandlerCount() > 0;
    }

    public static class Factory {
        private AbstractScript redisScript;
        private Duration pollDuration = Duration.ofMillis(100);
        private MessageConverter messageConverter = new PolymorphicJacksonMessageConverter();
        private Map<String, QueueMessageHandlers> messageHandlers = new HashMap<>();

        public Factory withRedisClient(BinaryScriptingCommands redisClient) {
            Objects.requireNonNull(redisClient);

            try {
                return withRedisScript(new SingleNodeScript(redisClient));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot connect to redis.", e);
            }
        }

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

        public Factory withPollDuration(Duration pollDuration) {
            this.pollDuration = Objects.requireNonNull(pollDuration);
            return this;
        }

        public Factory withMessageConverter(MessageConverter messageConverter) {
            this.messageConverter = Objects.requireNonNull(messageConverter);
            return this;
        }

        public QueueHandlerFactory withQueueHandler(String queueName, int maxConcurrentHandlers) {
            return new QueueHandlerFactory(this, queueName, maxConcurrentHandlers);
        }

        private Factory withMessageHandler(String queueName,
                                          int maxConcurrentHandlers,
                                          Map<Class<?>, MessageHandler<?>> messageHandlers) {


            if (messageHandlers.containsKey(queueName)) {
                throw new IllegalArgumentException("Message handlers already added for queue " + queueName);
            }

            this.messageHandlers.put(queueName, new QueueMessageHandlers(maxConcurrentHandlers, messageHandlers));
            return this;
        }

        public ConnectionManager build() {
            Objects.requireNonNull(redisScript, "Redis client was not configured.");

            return new ConnectionManager(redisScript, pollDuration, messageConverter, messageHandlers);
        }
    }

    public static class QueueHandlerFactory {
        private final Factory connectionManagerFactory;
        private final String queueName;
        private final int maxConcurrentHandlers;
        private final Map<Class<?>, MessageHandler<?>> messageHandlers;

        private QueueHandlerFactory(Factory connectionManagerFactory,
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

        public <T> QueueHandlerFactory withMessageHandler(Class<T> messageClass, MessageHandler<T> messageHandler) {
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
