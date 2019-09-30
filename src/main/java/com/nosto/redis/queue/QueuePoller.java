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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nosto.redis.queue.AbstractScript.TenantMessage;

class QueuePoller implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(QueuePoller.class);

    private static final int RANDOM_BOUND = 50;

    private final AbstractScript redis;
    private final MessageConverter messageConverter;
    private final String queueName;
    private final ThreadPoolExecutor messageHandlerExecutor;
    private final Duration waitAfterEmptyDequeue;
    private final Map<Class<?>, MessageHandler<?>> messageHandlers;
    private final Random random;
    private Duration shutdownTimeout;

    QueuePoller(AbstractScript redis,
                MessageConverter messageConverter,
                String queueName,
                int messageHandlerWorkers,
                Duration waitAfterEmptyDequeue,
                List<MessageHandler<?>> messageHandlers,
                Random random) {
        this.redis = redis;
        this.messageConverter = messageConverter;
        this.queueName = queueName;
        this.messageHandlerExecutor = new ThreadPoolExecutor(messageHandlerWorkers, messageHandlerWorkers,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        this.waitAfterEmptyDequeue = waitAfterEmptyDequeue;
        this.messageHandlers = messageHandlers.stream()
                .collect(Collectors.toMap(MessageHandler::getMessageClass, Function.identity()));
        this.random = random;
    }

    @Override
    public void run() {
        while (!messageHandlerExecutor.isShutdown()) {
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

        try {
            messageHandlerExecutor.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while awaiting message handlers to terminate.", e);
        }
    }

    void shutdown(Duration shutdownTimeout) {
        LOGGER.info("Shutting down.");
        this.shutdownTimeout = shutdownTimeout;
        messageHandlerExecutor.shutdown();
    }

    void shutdownNow() {
        messageHandlerExecutor.shutdownNow();
    }

    private int poll() {
        int availableWorkers = messageHandlerExecutor.getMaximumPoolSize() - messageHandlerExecutor.getActiveCount();
        if (availableWorkers == 0) {
            LOGGER.debug("All message handlers are busy.");
            return 0;
        }

        List<AbstractScript.TenantMessage> messages = redis.dequeue(Instant.now(), queueName, availableWorkers);
        LOGGER.debug("Dequeued {} messages for queue '{}'", messages.size(), queueName);

        List<CompletableFuture<Void>> messageHandlerResults = new ArrayList<>(messages.size());

        for (AbstractScript.TenantMessage message : messages) {
            LOGGER.debug("Received message for tenent '{}' and key '{}'",
                    message.getTenant(), message.getKey());

            Object payload = messageConverter.deserialize(message.getPayload());
            Objects.requireNonNull(payload, "Message payload was empty.");

            MessageHandler handler = messageHandlers.get(payload.getClass());
            Objects.requireNonNull(handler, "No handler found for payload " + payload.getClass());

            CompletableFuture<Void> messageHandlerResult = new CompletableFuture<>();
            messageHandlerResults.add(messageHandlerResult);

            messageHandlerExecutor.submit(() -> {
                try {
                    handler.handleMessage(message.getTenant(), payload);
                    messageHandlerResult.complete(null);
                } catch (Exception e) {
                    messageHandlerResult.completeExceptionally(e);
                }
            });
        }

        for (int i = 0; i < messageHandlerResults.size(); i++) {
            CompletionStage<Void> messageHandlerResult = messageHandlerResults.get(i);

            try {
                TenantMessage message = messages.get(i);

                messageHandlerResult.toCompletableFuture()
                        .get(message.getInvisiblePeriod().toMillis(), TimeUnit.MILLISECONDS);

                redis.ack(queueName, message.getTenant(), message.getKey());
            } catch (InterruptedException e) {
                LOGGER.error("Got interrupted while waiting for message handling to complete.", e);
            } catch (ExecutionException e) {
                LOGGER.error("Error handling message.", e);
            } catch (TimeoutException e) {
                LOGGER.error("Message was not handled in time.", e);
            }
        }

        return messages.size();
    }

    @Override
    public String toString() {
        return "QueuePoller{" +
                "queueName='" + queueName + '\'' +
                ", running=" + !messageHandlerExecutor.isShutdown() +
                '}';
    }
}
