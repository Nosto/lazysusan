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

public class Child2Pojo extends ParentPojo {
    private final String propertyB;

    @JsonCreator
    public Child2Pojo(@JsonProperty("propertyA") String propertyA,
                      @JsonProperty("propertyB") String propertyB) {
        super(propertyA);
        this.propertyB = propertyB;
    }

    public String getPropertyB() {
        return propertyB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Child2Pojo that = (Child2Pojo) o;
        return Objects.equals(propertyB, that.propertyB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyB);
    }

    @Override
    public String toString() {
        return "Child2Pojo{" +
                "propertyB='" + propertyB + '\'' +
                '}';
    }
}
