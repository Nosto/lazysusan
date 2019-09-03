/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.nosto.redis.queue.PolymorphicJacksonMessageConverter;

public class PolymorphicJacksonMessageConverterTest {
    private PolymorphicJacksonMessageConverter converter;

    @Before
    public void setUp() {
        converter = new PolymorphicJacksonMessageConverter();
    }

    @Test
    public void convertParent() {
        Parent p = new Parent("a");
        byte[] b = converter.serialize(p);
        p = converter.deserialize(b);

        assertEquals("a", p.propertyA);
    }

    class Parent {
        String propertyA;

        Parent(String propertyA) {
            this.propertyA = propertyA;
        }
    }

    class Child1 extends Parent {
        String propertyB;

        Child1(String propertyA, String propertyB) {
            super(propertyA);
            this.propertyB = propertyB;
        }
    }

    class Child2 extends Parent {
        String propertyB;

        Child2(String propertyA, String propertyB) {
            super(propertyA);
            this.propertyB = propertyB;
        }
    }
}
