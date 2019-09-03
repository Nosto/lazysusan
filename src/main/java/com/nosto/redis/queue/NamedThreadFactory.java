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

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class NamedThreadFactory implements ThreadFactory {
    private final String threadNamePrefix;
    private final AtomicInteger threadCount;
    private final ThreadGroup group;

    NamedThreadFactory(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
        threadCount = new AtomicInteger();
        group = Optional.ofNullable(System.getSecurityManager())
                .map(SecurityManager::getThreadGroup)
                .orElseGet(Thread.currentThread()::getThreadGroup);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(group, r, threadNamePrefix + threadCount.getAndIncrement(), 0);
    }
}
