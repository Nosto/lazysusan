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
import java.util.Objects;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class MessageSenderImpl<T> implements MessageSender<T> {
    private static final Logger LOGGER = LogManager.getLogger(MessageSenderImpl.class);

    private final AbstractScript redis;
    private final String queueName;
    private final Function<T, String> keyFunction;
    private final MessageConverter messageConverter;

    MessageSenderImpl(AbstractScript script,
                             String queueName,
                             Function<T, String> keyFunction,
                             MessageConverter messageConverter) {
        this.redis = script;
        this.queueName = queueName;
        this.keyFunction = keyFunction;
        this.messageConverter = messageConverter;
    }

    @Override
    public EnqueueResult send(String tenant, Duration invisiblePeriod, T message) {
        String key = keyFunction.apply(message);
        Objects.requireNonNull(key);

        byte[] messagePayload = messageConverter.serialize(message);
        if (messagePayload.length == 0) {
            throw new IllegalArgumentException("Empty message payload.");
        }

        AbstractScript.TenantMessage tenantMessage = new AbstractScript.TenantMessage(tenant, key, messagePayload);
        LOGGER.debug("Enqueueing message tenant for '{}' and key '{}'",
                tenant, key);
        return redis.enqueue(Instant.now(), invisiblePeriod, queueName, tenantMessage);
    }
}
