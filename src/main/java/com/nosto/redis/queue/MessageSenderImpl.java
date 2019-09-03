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
import java.util.function.Function;

class MessageSenderImpl<T> implements MessageSender<T> {
    private final AbstractScript redis;
    private final String queueName;
    private final Function<T, String> keyFunction;
    private final MessageConverter messageConverter;

    public MessageSenderImpl(AbstractScript redis,
                             String queueName,
                             Function<T, String> keyFunction,
                             MessageConverter messageConverter) {
        this.redis = redis;
        this.queueName = queueName;
        this.keyFunction = keyFunction;
        this.messageConverter = messageConverter;
    }

    @Override
    public void send(String tenant, Duration invisiblePeriod, T message) {
        String key = keyFunction.apply(message);
        byte[] messagePayload = messageConverter.serialize(message);
        AbstractScript.TenantMessage tenantMessage = new AbstractScript.TenantMessage(tenant, key, messagePayload);
        redis.enqueue(Instant.now(), invisiblePeriod, queueName, tenantMessage);
    }
}
