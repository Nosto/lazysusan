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

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.function.Function;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosto.redis.SingleNodeRedisConnector;

public class ConnectionManagerTest {
    @Test
    public void testSingleNode() {
        MessageHandler<Message1> message1Handler = mock(MessageHandler.class);
        MessageHandler<Message2> message2Handler = mock(MessageHandler.class);

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisClient(new SingleNodeRedisConnector().getJedis())
                .withQueueHandler("queue1", 1)
                    .withMessageHandler(Message1.class, message1Handler)
                    .withMessageHandler(Message2.class, message2Handler)
                    .build()
                .build();

        connectionManager.start();

        MessageSender<Message1> m1Sender = connectionManager.createSender("queue1", Message1::getParam1);
        m1Sender.send("t1", Duration.ofMillis(1), new Message1("p1"));
        m1Sender.send("t1", Duration.ofMillis(1), new Message1("p2"));
        m1Sender.send("t2", Duration.ofMillis(1), new Message1("p1"));

        MessageSender<Message2> m2Sender = connectionManager.createSender("queue1", Message2::getParam2);
        m2Sender.send("t1", Duration.ofMillis(1), new Message2("p1"));
        m2Sender.send("t2", Duration.ofMillis(1), new Message2("p1"));
        m2Sender.send("t2", Duration.ofMillis(1), new Message2("p2"));

        verify(message1Handler, timeout(100)).handleMessage(eq("t1"), argMatcher(Message1::getParam1, "p1"));
        verify(message1Handler, timeout(100)).handleMessage(eq("t1"), argMatcher(Message1::getParam1, "p1"));
        verify(message1Handler, timeout(100)).handleMessage(eq("t1"), argMatcher(Message1::getParam1, "p2"));

        verify(message2Handler, timeout(100)).handleMessage(eq("t1"), argMatcher(Message2::getParam2, "p1"));
        verify(message2Handler, timeout(100)).handleMessage(eq("t2"), argMatcher(Message2::getParam2, "p1"));
        verify(message2Handler, timeout(100)).handleMessage(eq("t2"), argMatcher(Message2::getParam2, "p2"));

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);

        verifyNoMoreInteractions(message1Handler);
        verifyNoMoreInteractions(message2Handler);
    }

    private <T, R> T argMatcher(Function<T, R> valueExtractor, R value) {
        return argThat(new ArgumentMatcher<T>() {
            @Override
            public boolean matches(Object argument) {
                T arg = (T) argument;
                return value.equals(valueExtractor.apply(arg));
            }
        });
    }

    public static class Message1 {
        private final String param1;

        public Message1(@JsonProperty("param1") String param1) {
            this.param1 = param1;
        }

        public String getParam1() {
            return param1;
        }
    }

    public static class Message2 {
        private final String param2;

        public Message2(@JsonProperty("param2") String param2) {
            this.param2 = param2;
        }

        public String getParam2() {
            return param2;
        }
    }
}
