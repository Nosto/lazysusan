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

/**
 * A typesafe interface used for enqueuing messages.
 * @param <T>
 */
public interface MessageSender<T> {
    /**
     * Enqueue a message.
     * @param tenant The tenant associated with the message.
     * @param invisiblePeriod The {@link Duration} that a dequeued message should be invisible while a
     * {@link MessageHandler} is processing the message. The message becomes visible again after this time
     * if the handler fails to handle the message.
     * @param message The message payload.
     * @return {@code true} if the message was successfully enqueued.<br>
     * {@code false} if an existing message has the same key.
     */
    boolean send(String tenant, Duration invisiblePeriod, T message);
}
