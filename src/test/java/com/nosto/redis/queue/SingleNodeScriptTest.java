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

import com.nosto.redis.SingleNodeRedisConnector;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

public class SingleNodeScriptTest extends AbstractScriptTest {
    @ClassRule
    public static SingleNodeRedisConnector jedis = new SingleNodeRedisConnector();

    public SingleNodeScriptTest() throws IOException {
        super(new SingleNodeScript(jedis.getJedis()));
    }

    @Before
    public void flush() {
        jedis.getJedis().flushDB();
    }
}
