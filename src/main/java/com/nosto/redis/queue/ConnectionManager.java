
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

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.BinaryScriptingCommands;

public class ConnectionManager {
    private final AbstractScript redis;
    private final Duration pollDuration;
    private final MessageConverter messageConverter;
    private final Map<String, QueueMessageHandlers> queueMessageHandlers;

    private final ReentrantLock startUpShutdownLock;
    private final MessagePoller messagePoller;

    private Timer timer;

    private ConnectionManager(AbstractScript redis,
                              Duration pollDuration,
                              MessageConverter messageConverter,
                              Map<String, QueueMessageHandlers> messageHanders) {
        this.redis = redis;
        this.pollDuration = pollDuration;
        this.messageConverter = messageConverter;
        this.queueMessageHandlers = messageHanders;

        startUpShutdownLock = new ReentrantLock();
        messagePoller = new MessagePoller(redis, messageConverter, queueMessageHandlers);
    }

    public <T> MessageSender<T> createSender(String queueName, Function<T, String> keyFunction) {
        return new MessageSenderImpl<>(redis, queueName, keyFunction, messageConverter);
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

    public void shutdown(Duration timeout) {
        startUpShutdownLock.lock();
        try {
            if (timer == null) {
                throw new IllegalStateException("ConnectionManager is not running.");
            }

            timer.cancel();

            messagePoller.awaitTermination(timeout);
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    public Map<String, List<Runnable>> shutdownNow() {
        startUpShutdownLock.lock();
        try {
            if (timer == null) {
                throw new IllegalStateException("ConnectionManager is not running.");
            }

            timer.cancel();

            return messagePoller.shutdownNow();
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    public boolean isRunning() {
        return timer != null || messagePoller.getActiveMessageHandlerCount() > 0;
    }

    public static class Factory {
        private AbstractScript redis;
        private Duration pollDuration = Duration.ofMillis(100);
        private MessageConverter messageConverter = new PolymorphicJacksonMessageConverter();
        private Map<String, QueueMessageHandlers> messageHandlers = new HashMap<>();

        public Factory withRedisClient(BinaryScriptingCommands redisClient) throws IOException {
            redis = new SingleNodeScript(redisClient);
            return this;
        }

        public Factory withRedisClient(BinaryJedisCluster rediClusterClient, int numberSlots) throws IOException {
            redis = new ClusterScript(rediClusterClient, numberSlots);
            return this;
        }

        public Factory withPollDuration(Duration pollDuration) {
            this.pollDuration = pollDuration;
            return this;
        }

        public Factory withMessageConverter(MessageConverter messageConverter) {
            this.messageConverter = messageConverter;
            return this;
        }

        public Factory withMessageHandler(String queueName,
                                          int maxConcurrentHandlers,
                                          Map<Class<?>, MessageHandler<?>> messageHandlers) {
            if (messageHandlers.containsKey(queueName)) {
                throw new IllegalArgumentException("Message handlers already added for queue " + queueName);
            }

            this.messageHandlers.put(queueName, new QueueMessageHandlers(maxConcurrentHandlers, messageHandlers));
            return this;
        }

        public ConnectionManager build() {
            Objects.requireNonNull(redis, "Redis client was not configured.");

            return new ConnectionManager(redis, pollDuration, messageConverter, messageHandlers);
        }
    }
}
