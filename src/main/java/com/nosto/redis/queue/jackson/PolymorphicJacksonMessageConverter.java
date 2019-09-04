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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.nosto.redis.queue.MessageConverter;

public class PolymorphicJacksonMessageConverter implements MessageConverter {
    private final ObjectMapper objectMapper;

    public PolymorphicJacksonMessageConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.setDefaultTyping(new StdTypeResolverBuilder()
                .init(JsonTypeInfo.Id.CLASS, null)
                .inclusion(JsonTypeInfo.As.PROPERTY));
    }

    @Override
    public byte[] serialize(Object messagePayload) {
        try {
            return objectMapper.writeValueAsBytes(messagePayload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize payload.", e);
        }
    }

    @Override
    public Object deserialize(byte[] messagePayload) {
        try {
            return objectMapper.readValue(new String(messagePayload, StandardCharsets.UTF_8), Object.class);
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize payload.", e);
        }
    }
}
