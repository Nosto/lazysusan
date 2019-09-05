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

public interface MessageSender<T> {
    /**
     * Enqueue a message.
     * @param tenant
     * @param invisiblePeriod
     * @param message
     * @return {@code true} if the message was successfully enqueued.
     * A message may not be enqueued if an existing message has the same key.
     */
    boolean send(String tenant, Duration invisiblePeriod, T message);
}
