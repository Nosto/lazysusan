/*
 *  Copyright (c) 2023 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */

package com.nosto.docker;

import com.palantir.docker.compose.execution.DockerCompose;
import com.palantir.docker.compose.logging.LogCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Slf4jLogCollector implements LogCollector {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jLogCollector.class);

    @Override
    public void collectLogs(DockerCompose dockerCompose) throws IOException, InterruptedException {
        for (String service : dockerCompose.services()) {
            try {
                collectLogs(service, dockerCompose);
            } catch (RuntimeException e) {
                logger.error("Failed to collect logs for " + service);
            }
        }
    }

    private void collectLogs(String service, DockerCompose dockerCompose) {
        try {
            OutputStream logsOutputStream = new ByteArrayOutputStream();
            dockerCompose.writeLogs(service, logsOutputStream);
            logger.info(logsOutputStream.toString());
        } catch (IOException e) {
            logger.error("Failed to collect logs for " + service, e);
        }
    }
}
