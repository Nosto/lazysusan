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

public interface MessageHandler<T> {
    /**
     * Handle a dequeued message.
     *
     * @param tenant
     * @param message
     */
    void handleMessage(String tenant, T message);

    Class<T> getMessageClass();
}
