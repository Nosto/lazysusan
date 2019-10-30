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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.nosto.redis.queue.ConnectionManager.QueueHandler;

class StubScript extends AbstractScript {
    private final MessageConverter messageConverter;
    private final Map<String, QueueHandler> queueHandlers;

    StubScript(MessageConverter messageConverter, Map<String, QueueHandler> queueHandlers) {
        this.messageConverter = messageConverter;
        this.queueHandlers = queueHandlers;
    }

    @Override
    public List<TenantMessage> dequeue(Instant now, Duration invisiblePeriod, String queue, int maxKeys) {
        return Collections.emptyList();
    }

    @Override
    public QueueStatistics getQueueStatistics(String queue) {
        throw new IllegalStateException("Not supported.");
    }

    @Override
    Object evalsha(List<byte[]> keys, List<byte[]> args) {
        throw new IllegalStateException("Not supported.");
    }

    @Override
    byte[] slot(String tenant) {
        return new byte[0];
    }

    @Override
    public void ack(String queue, String tenant, String key) {
        // do nothing
    }

    @Override
    public EnqueueResult enqueue(Instant now, Duration invisiblePeriod, String queue, TenantMessage tenantMessage) {
        Object payload = messageConverter.deserialize(tenantMessage.getPayload());
        Objects.requireNonNull(payload, "Message payload was empty.");

        MessageHandler handler = Optional.ofNullable(queueHandlers.get(queue))
                .map(QueueHandler::getMessageHandlers)
                .map(messageHandlers -> messageHandlers.get(payload.getClass()))
                .orElseThrow(() -> new NullPointerException("No handler found for payload " + payload.getClass()));
        handler.handleMessage(tenantMessage.getTenant(), payload);

        return EnqueueResult.SUCCESS;
    }
}
