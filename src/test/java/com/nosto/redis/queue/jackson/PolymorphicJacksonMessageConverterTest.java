/*******************************************************************************
 * Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue.jackson;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.nosto.redis.queue.model.Child1Pojo;
import com.nosto.redis.queue.model.Child2Pojo;
import com.nosto.redis.queue.model.ParentPojo;

public class PolymorphicJacksonMessageConverterTest {
    private PolymorphicJacksonMessageConverter converter;

    @Before
    public void setUp() {
        converter = new PolymorphicJacksonMessageConverter();
    }

    /**
     * Simple test verifies a class can be serialized and deserialized.
     */
    @Test
    public void convertParent() {
        ParentPojo p = new ParentPojo("a");

        byte[] bytes = converter.serialize(p);
        p = (ParentPojo) converter.deserialize(bytes);

        assertEquals("a", p.getPropertyA());
    }

    /**
     * All properties of child and parent class are serialized and deserialized.
     */
    @Test
    public void convertChild1() {
        Child1Pojo child = new Child1Pojo("a", "b");

        byte[] bytes = converter.serialize(child);
        child = (Child1Pojo) converter.deserialize(bytes);

        assertEquals("a", child.getPropertyA());
        assertEquals("b", child.getPropertyB());
    }

    /**
     * Child2 is serialized and deserialized even though it has the same properties
     * as Child1.
     */
    @Test
    public void convertChild2() {
        Child2Pojo child = new Child2Pojo("a", "b");

        byte[] bytes = converter.serialize(child);
        child = (Child2Pojo) converter.deserialize(bytes);

        assertEquals("a", child.getPropertyA());
        assertEquals("b", child.getPropertyB());
    }
}
