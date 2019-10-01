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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nosto.redis.queue.AbstractScript.TenantMessage;
import com.nosto.redis.queue.ConnectionManager.MessageHandlers;

class QueuePoller implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(QueuePoller.class);

    private static final int RANDOM_BOUND = 50;

    private final AbstractScript redis;
    private final MessageConverter messageConverter;
    private final String queueName;
    private final Duration waitAfterEmptyDequeue;
    private final Duration afterDequeueInvisiblePeriod;
    private final ThreadPoolExecutor messageHandlerExecutor;
    private final Map<Class<?>, MessageHandler<?>> messageHandlers;
    private final Random random;

    private boolean stop;

    QueuePoller(AbstractScript redis,
                MessageConverter messageConverter,
                String queueName,
                Duration waitAfterEmptyDequeue,
                MessageHandlers messageHandlers,
                Random random) {
        this.redis = redis;
        this.messageConverter = messageConverter;
        this.queueName = queueName;
        this.waitAfterEmptyDequeue = waitAfterEmptyDequeue;
        this.random = random;
        this.afterDequeueInvisiblePeriod = messageHandlers.getAfterDequeueInvisiblePeriod();
        this.messageHandlerExecutor = messageHandlers.getMessageHandlerExecutor();
        this.messageHandlers = messageHandlers.getMessageHandlers();
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                if (poll() == 0) {
                    long sleepMS = waitAfterEmptyDequeue.toMillis() + Math.abs(random.nextInt(RANDOM_BOUND));
                    LOGGER.debug("Received zero messages in last poll. Sleeping for {}ms.", sleepMS);

                    TimeUnit.MILLISECONDS.sleep(sleepMS);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Got interrupted while sleeping. Continuing.", e);
            } catch (Exception e) {
                LOGGER.error("Error while polling. Continuing.", e);
            }
        }
    }

    public void stop() {
        LOGGER.info("Stopping.");
        this.stop = true;
    }

    private int poll() {
        int availableWorkers = messageHandlerExecutor.getMaximumPoolSize() - messageHandlerExecutor.getActiveCount();
        if (availableWorkers == 0) {
            LOGGER.debug("All message handlers are busy.");
            return 0;
        }

        List<AbstractScript.TenantMessage> messages =
                redis.dequeue(Instant.now(), queueName, availableWorkers, afterDequeueInvisiblePeriod);
        LOGGER.debug("Dequeued {} messages for queue '{}'", messages.size(), queueName);

        Map<TenantMessage, CompletableFuture<Boolean>> messageAndResults = new HashMap<>(messages.size());

        for (AbstractScript.TenantMessage message : messages) {
            LOGGER.debug("Received message for tenent '{}' and key '{}'",
                    message.getTenant(), message.getKey());

            Object payload = messageConverter.deserialize(message.getPayload());
            Objects.requireNonNull(payload, "Message payload was empty.");

            MessageHandler handler = messageHandlers.get(payload.getClass());
            Objects.requireNonNull(handler, "No handler found for payload " + payload.getClass());

            CompletableFuture<Boolean> messageHandlerResult = new CompletableFuture<>();
            messageAndResults.put(message, messageHandlerResult);

            try {
                messageHandlerExecutor.submit(() -> {
                   try {
                       boolean handledSuccessfully = handler.handleMessage(message.getTenant(), payload);
                       messageHandlerResult.complete(handledSuccessfully);
                   } catch (Exception e) {
                       messageHandlerResult.completeExceptionally(e);
                   }
                });
            } catch (Exception e) {
                messageHandlerResult.completeExceptionally(e);
            }
        }

        for (Entry<TenantMessage, CompletableFuture<Boolean>> messageAndResult : messageAndResults.entrySet()) {
            TenantMessage message = messageAndResult.getKey();
            CompletableFuture<Boolean> result = messageAndResult.getValue();

            try {
                Boolean handledSuccessfully =
                        result.get(message.getInvisiblePeriod().toMillis(), TimeUnit.MILLISECONDS);

                if (Boolean.TRUE == handledSuccessfully) {
                    redis.ack(queueName, message.getTenant(), message.getKey());
                } else {
                    LOGGER.warn("Message handler did not complete successfully.");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while waiting for message handler to complete for message key: {}",
                        message.getKey(), e);
            } catch (ExecutionException e) {
                LOGGER.error("Message handler failed for message key: {}",
                        message.getKey(), e);
            } catch (TimeoutException e) {
                LOGGER.error("Message handler did not complete in time for message key: {}",
                        message.getKey(), e);
            }
        }

        return messages.size();
    }

    @Override
    public String toString() {
        return "QueuePoller{" +
                "queueName='" + queueName + '\'' +
                ", stop=" + stop +
                '}';
    }
}
