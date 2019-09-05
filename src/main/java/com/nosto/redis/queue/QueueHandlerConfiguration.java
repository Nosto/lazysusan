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

import java.util.Map;
import java.util.Optional;

class QueueHandlerConfiguration {
    private final int maxConcurrentHandlers;
    private final Map<Class<?>, MessageHandler<?>> messageHandlers;

    QueueHandlerConfiguration(int maxConcurrentHandlers, Map<Class<?>, MessageHandler<?>> messageHandlers) {
        this.maxConcurrentHandlers = maxConcurrentHandlers;
        this.messageHandlers = messageHandlers;
    }

    int getMaxConcurrentHandlers() {
        return maxConcurrentHandlers;
    }

    Optional<MessageHandler> getMessageHandler(Class c) {
        return Optional.ofNullable(messageHandlers.get(c));
    }
}
