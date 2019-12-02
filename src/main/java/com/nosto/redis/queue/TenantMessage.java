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

import java.util.Arrays;
import java.util.Objects;

/**
 * A representation of a tenant's message that can be enqueued.
 */
public final class TenantMessage {
    private final String tenant;
    private final String key;
    private final byte[] payload;

    /**
     * Create a new {@link TenantMessage}.
     * @param tenant The tenant associated with the message.
     * @param key A key that identifies the message. Messages with the same key are treated as duplicates.
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
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TenantMessage that = (TenantMessage) o;
        return Objects.equals(tenant, that.tenant) &&
                Objects.equals(key, that.key) &&
                Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenant, key, payload);
    }

    @Override
    public String toString() {
        return "TenantMessage{" +
                "tenant='" + tenant + '\'' +
                ", key='" + key + '\'' +
                ", payload=" + Arrays.toString(payload) +
                '}';
    }
}
