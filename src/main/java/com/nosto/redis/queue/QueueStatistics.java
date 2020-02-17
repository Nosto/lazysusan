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

import java.util.Collections;
import java.util.Map;

/**
 * Contains all statistics fetched for a queue.
 */
public final class QueueStatistics {
    private final Map<String, TenantStatistics> tenantStatistics;

    /**
     * Queue statistics.
     * @param tenantStatistics Statistics per tenant.
     */
    public QueueStatistics(Map<String, TenantStatistics> tenantStatistics) {
        this.tenantStatistics = Collections.unmodifiableMap(tenantStatistics);
    }

    /**
     * @return {@link TenantStatistics} for each tenant.
     */
    public Map<String, TenantStatistics> getTenantStatistics() {
        return tenantStatistics;
    }
}
