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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class MessagePoller {
    private static final Logger logger = LogManager.getLogger(MessagePoller.class);

    private static final int CORE_THREADPOOL_SIZE = 0;
    private static final long THREAD_KEEP_ALIVE_TIME_MS = 50L;

    private final AbstractScript redis;
    private final Duration pollDuration;
    private final MessageConverter messageConverter;
    private final Map<String, QueueHandlerConfiguration> messageHanders;

    private final Map<String, ThreadPoolExecutor> threadPools;
    private final ReentrantLock startUpShutdownLock;
    private final ScheduledExecutorService scheduledExecutorService;

    MessagePoller(AbstractScript redis,
                  Duration pollDuration,
                  MessageConverter messageConverter,
                  Map<String, QueueHandlerConfiguration> messageHanders) {
        this.redis = redis;
        this.pollDuration = pollDuration;
        this.messageConverter = messageConverter;
        this.messageHanders = messageHanders;

        threadPools = new HashMap<>();
        startUpShutdownLock = new ReentrantLock();
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    void start() {
        logger.info("Start");

        startUpShutdownLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Already running");
            }

            if (messageHanders.isEmpty()) {
                throw new IllegalStateException("No message handlers have been configured.");
            }

            messageHanders.forEach((queueName, messageHandler) -> {
                logger.debug("Creating thread pool for {} with max pool size {}",
                        queueName, messageHandler.getMaxConcurrentHandlers());

                threadPools.put(queueName, new ThreadPoolExecutor(
                        CORE_THREADPOOL_SIZE,
                        messageHandler.getMaxConcurrentHandlers(),
                        THREAD_KEEP_ALIVE_TIME_MS,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(messageHandler.getMaxConcurrentHandlers())));
            });

            scheduledExecutorService.scheduleAtFixedRate(this::poll,
                    pollDuration.toMillis(),
                    pollDuration.toMillis(),
                    TimeUnit.MILLISECONDS);
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    boolean shutdown(Duration timeout) {
        logger.info("Shutdown with timeout {}ms", timeout.toMillis());

        startUpShutdownLock.lock();
        try {
            scheduledExecutorService.shutdown();

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
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    Map<String, List<Runnable>> shutdownNow() {
        logger.info("Shutdown now.");

        startUpShutdownLock.lock();
        try {
            scheduledExecutorService.shutdown();

            return processThreadPools((queueName, threadPoolExecutor) -> threadPoolExecutor.shutdownNow())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } finally {
            startUpShutdownLock.unlock();
        }
    }

    boolean isRunning() {
        return processThreadPools((queueName, threadPoolExecutor) -> threadPoolExecutor)
                .map(Map.Entry::getValue)
                .anyMatch(tp -> !(tp.isShutdown() && tp.isTerminated()));
    }

    private void poll() {
        try {
            Instant now = Instant.now();

            messageHanders.forEach((queueName, handlers) -> {
                ThreadPoolExecutor threadPool = threadPools.get(queueName);
                if (threadPool == null) {
                    logger.error("No thread pool for queue {}", queueName);
                    return;
                }

                int maxHandlerCount = threadPool.getMaximumPoolSize();
                int activeHandlerCount = threadPool.getActiveCount();
                int batchSize = maxHandlerCount - activeHandlerCount;
                if (batchSize <= 0) {
                    logger.debug("{} handlers are busy for queue {}. Cannot poll for new messages.",
                            activeHandlerCount, queueName);
                    return;
                }

                List<AbstractScript.TenantMessage> messages = redis.dequeue(now, queueName, batchSize);
                for (AbstractScript.TenantMessage message : messages) {
                    logger.debug("Received message for tenent {} and key {}",
                            message.getTenant(), message.getKey());

                    handleMessage(threadPool, queueName, handlers, message);
                }
            });
        } catch (Exception e) {
            logger.error("Error while polling.", e);
        }
    }

    private void handleMessage(ThreadPoolExecutor threadPool,
                               String queueName,
                               QueueHandlerConfiguration handlers,
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
}
