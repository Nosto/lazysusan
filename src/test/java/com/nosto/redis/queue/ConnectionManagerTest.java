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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectionManagerTest extends AbstractScriptTest {
    @Test
    public void receivedMessages() {
        MessageHandler<Message1> m1Handler = mock(MessageHandler.class);
        MessageHandler<Message2> m2Handler = mock(MessageHandler.class);

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisScript(script)
                .withQueueHandler("queue1", 2)
                    .withMessageHandler(Message1.class, m1Handler)
                    .withMessageHandler(Message2.class, m2Handler)
                    .build()
                .build();

        connectionManager.start();

        MessageSender<Message1> m1Sender = connectionManager.createSender("queue1", m -> "m1_" + m.getParam1());

        Duration invisiblePeriod = Duration.ofMillis(50);
        boolean sent = m1Sender.send("t1", invisiblePeriod, new Message1("p1"));
        assertTrue(sent);

        sent = m1Sender.send("t1", invisiblePeriod, new Message1("p2"));
        assertTrue(sent);

        sent = m1Sender.send("t2", invisiblePeriod, new Message1("p1"));
        assertTrue(sent);

        MessageSender<Message2> m2Sender = connectionManager.createSender("queue1", m -> "m2_" + m.getParam2());

        sent = m2Sender.send("t1", invisiblePeriod, new Message2("p1"));
        assertTrue(sent);

        sent = m2Sender.send("t2", invisiblePeriod, new Message2("p1"));
        assertTrue(sent);

        sent = m2Sender.send("t2", invisiblePeriod, new Message2("p2"));
        assertTrue(sent);

        verifyMessagesReceived(Message1.class, m1Handler, "t1", new Message1("p1"), new Message1("p2"));
        verifyMessagesReceived(Message1.class, m1Handler, "t2", new Message1("p1"));

        verifyMessagesReceived(Message2.class, m2Handler, "t1", new Message2("p1"));
        verifyMessagesReceived(Message2.class, m2Handler, "t2", new Message2("p1"), new Message2("p2"));

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);

        verifyNoMoreInteractions(m1Handler);
        verifyNoMoreInteractions(m2Handler);
    }

    private <T> void verifyMessagesReceived(Class<T> c, MessageHandler<T> mockMessageHandler, String expectedTenant, T... expectedMessages) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass(c);

        verify(mockMessageHandler, timeout(1000).times(expectedMessages.length))
                .handleMessage(eq(expectedTenant), messageCaptor.capture());

        assertEquals(Set.of(expectedMessages), Set.copyOf(messageCaptor.getAllValues()));
    }

    public static class Message1 {
        private final String param1;

        public Message1(@JsonProperty("param1") String param1) {
            this.param1 = param1;
        }

        public String getParam1() {
            return param1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Message1 message1 = (Message1) o;
            return param1.equals(message1.param1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(param1);
        }

        @Override
        public String toString() {
            return "Message1{" +
                    "param1='" + param1 + '\'' +
                    '}';
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Message2 message2 = (Message2) o;
            return param2.equals(message2.param2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(param2);
        }

        @Override
        public String toString() {
            return "Message2{" +
                    "param2='" + param2 + '\'' +
                    '}';
        }
    }
}
