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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class QueuePoller implements Runnable {
    private static final Logger logger = LogManager.getLogger(QueuePoller.class);

    private final AbstractScript redis;

    private final MessageConverter messageConverter;
    private final String queueName;
    private final int dequeueSize;
    private final Map<Class<?>, MessageHandler<?>> messageHandlers;

    QueuePoller(AbstractScript redis,
                MessageConverter messageConverter,
                String queueName,
                int dequeueSize,
                Map<Class<?>, MessageHandler<?>> messageHandlers) {
        this.redis = redis;
        this.messageConverter = messageConverter;
        this.queueName = queueName;
        this.dequeueSize = dequeueSize;
        this.messageHandlers = messageHandlers;
    }

    @Override
    public void run() {
        try {
            poll();
        } catch (Exception e) {
            logger.error("Error while polling.", e);
        }
    }

    public String getQueueName() {
        return queueName;
    }

    private void poll() {
        List<AbstractScript.TenantMessage> messages = redis.dequeue(Instant.now(), queueName, dequeueSize);
        logger.debug("Dequeued {} messages for queue '{}'", messages.size(), queueName);

        for (AbstractScript.TenantMessage message : messages) {
            logger.debug("Received message for tenent '{}' and key '{}'",
                    message.getTenant(), message.getKey());

            Object payload = messageConverter.deserialize(message.getPayload());
            Objects.requireNonNull(payload, "Message payload was empty.");

            MessageHandler handler = messageHandlers.get(payload.getClass());
            Objects.requireNonNull(handler, "No handler found for payload " + payload.getClass());

            handler.handleMessage(message.getTenant(), payload);

            redis.ack(queueName, message.getTenant(), message.getKey());
        }
    }
}
