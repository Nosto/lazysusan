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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class QueuePoller implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(QueuePoller.class);

    private static final int RANDOM_BOUND = 50;

    private final AbstractScript redis;
    private final MessageConverter messageConverter;
    private final String queueName;
    private final int dequeueSize;
    private final Duration pollPeriod;
    private final Map<Class<?>, MessageHandler<?>> messageHandlers;
    private final Random random;
    private boolean stop;

    QueuePoller(AbstractScript redis,
                MessageConverter messageConverter,
                String queueName,
                int dequeueSize,
                Duration pollPeriod,
                List<MessageHandler<?>> messageHandlers,
                Random random) {
        this.redis = redis;
        this.messageConverter = messageConverter;
        this.queueName = queueName;
        this.dequeueSize = dequeueSize;
        this.pollPeriod = pollPeriod;
        this.messageHandlers = messageHandlers.stream()
                .collect(Collectors.toMap(MessageHandler::getMessageClass, Function.identity()));
        this.random = random;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                if (poll() == 0) {
                    long sleepMS = pollPeriod.toMillis() + Math.abs(random.nextInt(RANDOM_BOUND));
                    LOGGER.debug("Received zero messages in last poll. Sleeping for {}ms.", sleepMS);

                    TimeUnit.MILLISECONDS.sleep(sleepMS);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Got interrupted while sleeping. Stopping.", e);
                stop = true;
            } catch (Exception e) {
                LOGGER.error("Error while polling. Continuing", e);
            }
        }
    }

    public void stop() {
        LOGGER.info("Stopping.");
        this.stop = true;
    }

    private int poll() {
        List<AbstractScript.TenantMessage> messages = redis.dequeue(Instant.now(), queueName, dequeueSize);
        LOGGER.debug("Dequeued {} messages for queue '{}'", messages.size(), queueName);

        for (AbstractScript.TenantMessage message : messages) {
            LOGGER.debug("Received message for tenent '{}' and key '{}'",
                    message.getTenant(), message.getKey());

            Object payload = messageConverter.deserialize(message.getPayload());
            Objects.requireNonNull(payload, "Message payload was empty.");

            MessageHandler handler = messageHandlers.get(payload.getClass());
            Objects.requireNonNull(handler, "No handler found for payload " + payload.getClass());

            handler.handleMessage(message.getTenant(), payload);

            redis.ack(queueName, message.getTenant(), message.getKey());
        }

        return messages.size();
    }
}
