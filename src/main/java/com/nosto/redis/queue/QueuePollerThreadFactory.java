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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class QueuePollerThreadFactory implements ThreadFactory {
    private static final String THREAD_NAME_PREFIX = "QueuePoller-";

    private final ThreadFactory threadFactory = Executors.defaultThreadFactory();
    private final AtomicInteger threadCount = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = threadFactory.newThread(r);
        thread.setName(THREAD_NAME_PREFIX + threadCount.getAndIncrement());
        return thread;
    }
}
