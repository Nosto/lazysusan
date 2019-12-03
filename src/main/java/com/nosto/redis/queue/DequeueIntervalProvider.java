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
 * The interval at which a message can be de-queued can be set on a per tenant basis.
 * For example, if a tenant's dequeue interval is {@code Duration.withSeconds(1)}, 1 message can be dequeued
 * every 1 second for the specified tenant.
 */
public interface DequeueIntervalProvider {
    /**
     * Get the interval at which a message can be de-queued for a tenant.
     * @param tenant The tenant.
     * @return The dequeue rate for the specified {@code tenant}.
     */
    Duration getDequeueInterval(String tenant);
}
