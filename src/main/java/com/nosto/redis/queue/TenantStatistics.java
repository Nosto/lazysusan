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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Queue statistics associated with a tenant.
 */
public final class TenantStatistics {
    private final String tenant;
    private final long invisibleMessageCount;
    private final long visibleMessageCount;

    /**
     * @param tenant The tenant.
     * @param invisibleMessageCount Total number of invisible messages for the tenant.
     * @param visibleMessageCount Total number of visible messages for the tenant.
     */
    public TenantStatistics(String tenant, long invisibleMessageCount, long visibleMessageCount) {
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
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
