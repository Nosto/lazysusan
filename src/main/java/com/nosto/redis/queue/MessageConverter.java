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
 * This is used for serializing messages before enqueuing them and for
 * deserializing dequeued messages so they can be passed to their corresponding {@link MessageHandler}.
 */
public interface MessageConverter {
    /**
     * @param messagePayload The message to be enqueued.
     * @return The serialized message.
     */
    byte[] serialize(Object messagePayload);

    /**
     * @param messagePayload A dequeued message.
     * @return The deserialized message.
     */
    Object deserialize(byte[] messagePayload);
}
