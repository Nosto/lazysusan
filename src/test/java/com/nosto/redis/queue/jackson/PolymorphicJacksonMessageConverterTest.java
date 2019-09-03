/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
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

public class PolymorphicJacksonMessageConverterTest {
    private PolymorphicJacksonMessageConverter converter;

    @Before
    public void setUp() {
        converter = new PolymorphicJacksonMessageConverter();
    }

    @Test
    public void convertParent() {
        Parent p = new Parent();
        p.setPropertyA("a");

        byte[] bytes = converter.serialize(p);
        p = (Parent) converter.deserialize(bytes);

        assertEquals("a", p.propertyA);
    }

    @Test
    public void convertChild1() {
        Child1 child = new Child1();
        child.setPropertyA("a");
        child.setPropertyB("b");

        byte[] bytes = converter.serialize(child);
        child = (Child1) converter.deserialize(bytes);

        assertEquals("a", child.getPropertyA());
        assertEquals("b", child.getPropertyB());
    }

    @Test
    public void convertChild2() {
        Child2 child = new Child2();
        child.setPropertyA("a");
        child.setPropertyB("b");

        byte[] bytes = converter.serialize(child);
        child = (Child2) converter.deserialize(bytes);

        assertEquals("a", child.getPropertyA());
        assertEquals("b", child.getPropertyB());
    }

    static class Parent {
        private String propertyA;

        public String getPropertyA() {
            return propertyA;
        }

        public void setPropertyA(String propertyA) {
            this.propertyA = propertyA;
        }
    }

     static class Child1 extends Parent {
        private String propertyB;

         public String getPropertyB() {
             return propertyB;
         }

         public void setPropertyB(String propertyB) {
             this.propertyB = propertyB;
         }
     }

     static class Child2 extends Parent {
        private String propertyB;

         public String getPropertyB() {
             return propertyB;
         }

         public void setPropertyB(String propertyB) {
             this.propertyB = propertyB;
         }
     }
}
