/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.starter.helix;

import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultHelixStarterServerConfig {
  private DefaultHelixStarterServerConfig() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultHelixStarterServerConfig.class);

  public static ServerConf getDefaultHelixServerConfig(PinotConfiguration externalConfigs) {
    PinotConfiguration defaultConfigs = loadDefaultServerConf();

    for (String key : externalConfigs.getKeys()) {
      defaultConfigs.setProperty(key, externalConfigs.getRawProperty(key));

      LOGGER.info("External config key: {}, value: {}", key, externalConfigs.getProperty(key));
    }
    return new ServerConf(defaultConfigs);
  }

  public static PinotConfiguration loadDefaultServerConf() {
    PinotConfiguration serverConf = new PinotConfiguration();

    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR,
        CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
        CommonConstants.Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR);

    serverConf
        .addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_READ_MODE, CommonConstants.Server.DEFAULT_READ_MODE);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS,
        CommonConstants.Server.DEFAULT_DATA_MANAGER_CLASS);

    // query executor parameters
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_CLASS,
        CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_CLASS);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS,
        "ValidSegmentPruner,DataSchemaSegmentPruner,ColumnValueSegmentPruner,SelectionQuerySegmentPruner");

    // request handler factory parameters
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_REQUEST_HANDLER_FACTORY_CLASS,
        CommonConstants.Server.DEFAULT_REQUEST_HANDLER_FACTORY_CLASS);

    // netty port
    serverConf
        .addProperty(CommonConstants.Server.CONFIG_OF_NETTY_PORT, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);

    return serverConf;
  }
}
