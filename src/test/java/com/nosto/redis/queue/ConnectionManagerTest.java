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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosto.redis.SingleNodeRedisConnector;

public class ConnectionManagerTest {
    @Test
    public void testSingleNode() {
        Map<String, List<Message1>> m1Messages = new HashMap<>();
        Map<String, List<Message2>> m2Messages = new HashMap<>();

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisClient(new SingleNodeRedisConnector().getJedis())
                .withQueueHandler("queue", 1)
                .withMessageHandler(Message1.class, (t, m) ->  m1Messages.computeIfAbsent(t, k -> new ArrayList<>()).add(m))
                .withMessageHandler(Message2.class, (t, m) ->  m2Messages.computeIfAbsent(t, k -> new ArrayList<>()).add(m))
                .build()
                .build();

        connectionManager.start();

        MessageSender<Message1> m1Sender = connectionManager.createSender("queue1", Message1::getParam1);
        m1Sender.send("t1", Duration.ofSeconds(1), new Message1("p1"));
        m1Sender.send("t1", Duration.ofSeconds(1), new Message1("p2"));
        m1Sender.send("t2", Duration.ofSeconds(1), new Message1("p1"));

        MessageSender<Message2> m2Sender = connectionManager.createSender("queue2", Message2::getParam2);
        m2Sender.send("t1", Duration.ofSeconds(1), new Message2("p1"));
        m2Sender.send("t2", Duration.ofSeconds(1), new Message2("p1"));
        m2Sender.send("t2", Duration.ofSeconds(1), new Message2("p2"));

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);
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
