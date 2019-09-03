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

import java.util.Iterator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.server.conf.ServerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultHelixStarterServerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultHelixStarterServerConfig.class);

  public static ServerConf getDefaultHelixServerConfig(Configuration externalConfigs) {
    Configuration defaultConfigs = loadDefaultServerConf();
    @SuppressWarnings("unchecked")
    Iterator<String> iterable = externalConfigs.getKeys();
    while (iterable.hasNext()) {
      String key = iterable.next();
      defaultConfigs.setProperty(key, externalConfigs.getProperty(key));
      LOGGER.info("External config key: {}, value: {}", key, externalConfigs.getProperty(key));
    }
    return new ServerConf(defaultConfigs);
  }

  public static Configuration loadDefaultServerConf() {
    Configuration serverConf = new PropertiesConfiguration();
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
        "DataSchemaSegmentPruner,ColumnValueSegmentPruner,ValidSegmentPruner,PartitionSegmentPruner");
    serverConf.addProperty("pinot.server.query.executor.pruner.DataSchemaSegmentPruner.id", "0");
    serverConf.addProperty("pinot.server.query.executor.pruner.ColumnValueSegmentPruner.id", "1");
    serverConf.addProperty("pinot.server.query.executor.pruner.ValidSegmentPruner.id", "2");
    serverConf.addProperty("pinot.server.query.executor.pruner.PartitionSegmentPruner.id", "3");

    // request handler factory parameters
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_REQUEST_HANDLER_FACTORY_CLASS,
        CommonConstants.Server.DEFAULT_REQUEST_HANDLER_FACTORY_CLASS);

    // netty port
    serverConf
        .addProperty(CommonConstants.Server.CONFIG_OF_NETTY_PORT, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);

    return serverConf;
  }
}
