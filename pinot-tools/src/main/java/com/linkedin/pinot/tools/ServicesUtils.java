/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import com.linkedin.pinot.tools.admin.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.linkedin.pinot.tools.SchemaInfo.*;

public class ServicesUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServicesUtils.class);

    private void startZookeeper() throws IOException {
        StartZookeeperCommand zkStarter = new StartZookeeperCommand();
        zkStarter.execute();
    }

    private void startContollers() throws Exception {
        StartControllerCommand controllerStarter = new StartControllerCommand();
        controllerStarter.setControllerPort(SchemaInfo.DEFAULT_CONTROLLER_PORT)
                .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS)
                .setDataDir(DEFAULT_DATA_DIR);
        controllerStarter.execute();
    }

    private void startBrokers() throws Exception {
        StartBrokerCommand brokerStarter = new StartBrokerCommand();
        brokerStarter.setPort(DEFAULT_BROKER_PORT)
                .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS);
        brokerStarter.execute();
    }

    private void startServers() throws Exception {
        StartServerCommand serverStarter = new StartServerCommand();
        serverStarter.setPort(DEFAULT_SERVER_PORT)
                .setDataDir(DATA_DIR)
                .setSegmentDir(SEGMENT_DIR)
                .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS);
        serverStarter.execute();
    }

    private void startAllServices() throws Exception {
        startZookeeper();
        startContollers();
        startBrokers();
        startServers();
    }

    public void stopAllServices() throws Exception {
        StopProcessCommand stopper = new StopProcessCommand(false);
        stopper.stopController().stopBroker().stopServer().stopZookeeper();
        stopper.execute();
    }

    public static void main(String[] args) throws Exception {
        ServicesUtils seg = new ServicesUtils();

        LOGGER.info("----- Starting All Services -----");
        seg.startAllServices();
    }
}
