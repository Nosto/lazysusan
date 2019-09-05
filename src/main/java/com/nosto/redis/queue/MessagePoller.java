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

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class MessagePoller extends TimerTask {
    private static final Logger logger = LogManager.getLogger(MessagePoller.class);

    private final AbstractScript redis;
    private final Map<String, QueueMessageHandlers> messageHanders;
    private final MessageConverter messageConverter;
    private final Map<String, ThreadPoolExecutor> threadPools;

    MessagePoller(AbstractScript redis,
                  MessageConverter messageConverter,
                  Map<String, QueueMessageHandlers> messageHanders) {
        this.redis = redis;
        this.messageConverter = messageConverter;
        this.messageHanders = messageHanders;

        threadPools = new HashMap<>();
        messageHanders.forEach((queueName, messageHandler) -> {
            logger.debug("Creating thread pool for {} with max pool size {}",
                    queueName, messageHandler.getMaxConcurrentHandlers());

            threadPools.put(queueName, new ThreadPoolExecutor(
                    0,
                    messageHandler.getMaxConcurrentHandlers(),
                    50L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(messageHandler.getMaxConcurrentHandlers()),
                    new NamedThreadFactory(queueName + "-handler-"),
                    new ThreadPoolExecutor.AbortPolicy()));
        });
    }

    @Override
    public void run() {
        Instant now = Instant.now();

        messageHanders.forEach((queueName, handlers) -> {
            ThreadPoolExecutor threadPool = threadPools.get(queueName);

            int maxHandlerCount = threadPool.getMaximumPoolSize();
            int activeHandlerCount = threadPool.getActiveCount();
            int batchSize = maxHandlerCount - activeHandlerCount;
            if (batchSize <= 0) {
                logger.debug("{} handers are busy for queue {}. Cannot poll for new messages.",
                        activeHandlerCount, queueName);
                return;
            }

            redis.dequeue(now, queueName, batchSize).forEach(message -> {
                logger.debug("Received message for tenent {} and key {}",
                        message.getTenant(), message.getKey());

                handleMessage(threadPool, queueName, handlers, message);
            });
        });
    }

    private void handleMessage(ThreadPoolExecutor threadPool,
                               String queueName,
                               QueueMessageHandlers handlers,
                               AbstractScript.TenantMessage message) {
        threadPool.execute(() -> {
            try {
                Object payload = messageConverter.deserialize(message.getPayload());
                Objects.requireNonNull(payload, "Message payload was empty.");

                MessageHandler handler = handlers.getMessageHandler(payload.getClass())
                        .orElseThrow(() -> new IllegalStateException("No handler found for payload " + payload.getClass()));

                handler.handleMessage(message.getTenant(), payload);

                redis.ack(queueName, message.getTenant(), message.getKey());
            } catch (Exception e) {
                logger.error("Could not handle message.", e);
            }
        });
    }

    private <R> Stream<Map.Entry<String, R>> processThreadPools(BiFunction<String, ThreadPoolExecutor, R> function) {
        return threadPools.entrySet()
                .stream()
                .map(e -> {
                    String queueName = e.getKey();
                    R r = function.apply(queueName, e.getValue());
                    return new AbstractMap.SimpleEntry<>(queueName, r);
                });
    }

    boolean shutdown(Duration timeout) {
        return processThreadPools((queueName, threadPoolExecutor) -> {
            threadPoolExecutor.shutdown();

            try {
                return threadPoolExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Got interrupted while awaiting {} thread pool termination.",
                        queueName, e);
            }

            return false;
        }).map(Map.Entry::getValue).allMatch(Boolean.TRUE::equals);
    }

    Map<String, List<Runnable>> shutdownNow() {
        return processThreadPools((queueName, threadPoolExecutor) -> threadPoolExecutor.shutdownNow())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    boolean isRunning() {
        return processThreadPools((queueName, threadPoolExecutor) -> threadPoolExecutor)
                .map(Map.Entry::getValue)
                .anyMatch(tp -> !(tp.isShutdown() && tp.isTerminated()));
    }
}
