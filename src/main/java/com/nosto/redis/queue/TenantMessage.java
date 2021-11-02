/*
 *  Copyright (c) 2020 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A representation of a tenant's message that can be enqueued.
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class TenantMessage {
    private final String tenant;
    private final String key;
    private final byte[] payload;

    /**
     * Create a new {@link TenantMessage}.
     *
     * @param tenant  The tenant associated with the message.
     * @param key     A key that identifies the message. Messages with the same key are treated as duplicates.
     * @param payload The message payload.
     */
    public TenantMessage(String tenant, String key, byte[] payload) {
        this.tenant = tenant;
        this.key = key;
        this.payload = payload;
    }

    /**
     * @return The tenant associated with the message.
     */
    public String getTenant() {
        return tenant;
    }

    /**
     * @return A key that identifies the message. Messages with the same key are treated as duplicates.
     */
    public String getKey() {
        return key;
    }

    /**
     * @return The message payload.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getPayload() {
        return payload;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
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
