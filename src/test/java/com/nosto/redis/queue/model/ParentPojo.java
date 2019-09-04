/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ParentPojo {
    private final String propertyA;

    @JsonCreator
    public ParentPojo(@JsonProperty("propertyA") String propertyA) {
        this.propertyA = propertyA;
    }

    public String getPropertyA() {
        return propertyA;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParentPojo that = (ParentPojo) o;
        return Objects.equals(propertyA, that.propertyA);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyA);
    }

    @Override
    public String toString() {
        return "ParentPojo{" +
                "propertyA='" + propertyA + '\'' +
                '}';
    }
}
