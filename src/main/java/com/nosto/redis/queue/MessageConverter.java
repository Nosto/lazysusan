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

/**
 * This is used for serialising messages before enqueuing them and for
 * deserialising dequeued messages so they can be passed to their corresponding
 * {@link MessageHandler}.
 */
public interface MessageConverter {
    /**
     * @param messagePayload The message to be enqueued.
     * @return The serialised message.
     */
    byte[] serialize(Object messagePayload);

    /**
     * @param messagePayload A dequeued message.
     * @return The deserialised message.
     */
    Object deserialize(byte[] messagePayload);
}
