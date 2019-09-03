/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PolymorphicJacksonMessageConverter implements MessageConverter {
    private final ObjectMapper objectMapper;

    public PolymorphicJacksonMessageConverter() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public <T> byte[] serialize(T messagePayload) {
        try {
            SerializingWrapper<T> wrappedPayload = new SerializingWrapper<>(messagePayload);
            return objectMapper.writeValueAsBytes(wrappedPayload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize payload.", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] messagePayload) {
        try {
            DeserializingWrapper wrappedPayload = objectMapper.readValue(messagePayload, DeserializingWrapper.class);
            Class<T> c = (Class<T>) Class.forName(wrappedPayload.getClassName());
            return objectMapper.convertValue(wrappedPayload.getValue(), c);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Unable to deserialize payload.", e);
        }
    }

    static abstract class Wrapper<T> {
        @JsonProperty("@class")
        private final String className;
        @JsonProperty("value")
        private final T value;

        protected Wrapper(String className, T value) {
            this.className = className;
            this.value = value;
        }

        String getClassName() {
            return className;
        }

        public T getValue() {
            return value;
        }
    }

    static class SerializingWrapper<T> extends Wrapper<T> {
        protected SerializingWrapper(T value) {
            super(value.getClass().getName(), value);
        }
    }

    static class DeserializingWrapper extends Wrapper<Map<String, Object>> {
        protected DeserializingWrapper(@JsonProperty("@class") String className,
                                       @JsonProperty("value") Map<String, Object> value) {
            super(className, value);
        }
    }
}
