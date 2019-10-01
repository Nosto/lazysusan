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

/**
 * Handle dequeued messages.
 * @param <T>
 */
public interface MessageHandler<T> {
    /**
     * Handle a dequeued message. The message will become available again for processing if this method returns
     * {@code false} or throws an exception.
     *
     * @param tenant The message's tenant.
     * @param message The deserialized message payload.
     * @return {@code true} if the message was successfully handled.
     * {@code false} if the message was not successfully handled.
     */
    boolean handleMessage(String tenant, T message);

    /**
     * @return The {@link Class} that this implementation handles.
     */
    Class<T> getMessageClass();
}
