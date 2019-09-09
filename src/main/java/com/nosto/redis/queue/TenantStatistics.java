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

import java.util.Objects;

/**
 * Queue statistics associated with a tenant.
 */
public final class TenantStatistics {
    private final String tenant;
    private final long invisibleMessageCount;
    private final long visibleMessageCount;

    TenantStatistics(String tenant, long invisibleMessageCount, long visibleMessageCount) {
        this.tenant = tenant;
        this.invisibleMessageCount = invisibleMessageCount;
        this.visibleMessageCount = visibleMessageCount;
    }

    /**
     * @return The tenant.
     */
    public String getTenant() {
        return tenant;
    }

    /**
     * @return Total number of invisible messages for the tenant.
     */
    public long getInvisibleMessageCount() {
        return invisibleMessageCount;
    }

    /**
     * @return Total number of visible messages for the tenant.
     */
    public long getVisibleMessageCount() {
        return visibleMessageCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TenantStatistics that = (TenantStatistics) o;
        return invisibleMessageCount == that.invisibleMessageCount &&
                visibleMessageCount == that.visibleMessageCount &&
                Objects.equals(tenant, that.tenant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenant, invisibleMessageCount, visibleMessageCount);
    }

    @Override
    public String toString() {
        return "TenantStatistics{" +
                "tenant='" + tenant + '\'' +
                ", invisibleMessageCount=" + invisibleMessageCount +
                ", visibleMessageCount=" + visibleMessageCount +
                '}';
    }
}
