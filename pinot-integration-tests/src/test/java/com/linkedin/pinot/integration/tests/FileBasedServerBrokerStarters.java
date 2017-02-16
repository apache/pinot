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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.pruner.DataSchemaSegmentPruner;
import com.linkedin.pinot.core.query.pruner.TimeSegmentPruner;
import com.linkedin.pinot.core.query.pruner.ValidSegmentPruner;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Iterator;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedServerBrokerStarters {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedServerBrokerStarters.class);
  private ServerInstance serverInstance;
  private BrokerServerBuilder bld;

  /*
   * Constants :
   * change these if you need to
   * */

  // common properties
  public static final String[] TABLE_NAMES = { "table1", "table2" };

  // server properties
  public static final String SERVER_PORT = "8000";
  public static final String SERVER_INDEX_DIR = "/tmp/index";
  public static final String SERVER_BOOTSTRAP_DIR = "/tmp/boostrap";
  public static final String SERVER_INDEX_READ_MODE = "heap";
  // broker properties
  public static final String BROKER_CLIENT_PORT = "8001";

  public static final String prefix = "pinot.server.instance.";

  public FileBasedServerBrokerStarters() {

  }

  private String getKey(String... keys) {
    return StringUtils.join(keys, ".");
  }

  private PropertiesConfiguration brokerProperties() {
    final PropertiesConfiguration brokerConfiguration = new PropertiesConfiguration();

    //transport properties

    //config based routing
    brokerConfiguration.addProperty("pinot.broker.transport.routingMode", "CONFIG");

    //two tables
    brokerConfiguration.addProperty("pinot.broker.transport.routing.tableName", StringUtils.join(TABLE_NAMES, ","));

    // table conf

    for (final String table : TABLE_NAMES) {
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", table, "numNodesPerReplica"), "1");
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", table, "serversForNode.0"), "localhost:"
          + SERVER_PORT);
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", table, "serversForNode.default"),
          "localhost:" + SERVER_PORT);
    }
    // client properties
    brokerConfiguration.addProperty("pinot.broker.client.enableConsole", "true");
    brokerConfiguration.addProperty("pinot.broker.client.queryPort", BROKER_CLIENT_PORT);
    brokerConfiguration.addProperty("pinot.broker.client.consolePath", "dont/need/this");
    brokerConfiguration.setDelimiterParsingDisabled(false);
    return brokerConfiguration;
  }

  private PropertiesConfiguration serverProperties() {
    final PropertiesConfiguration serverConfiguration = new PropertiesConfiguration();
    serverConfiguration.addProperty(getKey("pinot.server.instance", "id"), "0");
    serverConfiguration.addProperty(getKey("pinot.server.instance", "bootstrap.segment.dir"), SERVER_BOOTSTRAP_DIR);
    serverConfiguration.addProperty(getKey("pinot.server.instance", "dataDir"), SERVER_INDEX_DIR);
    serverConfiguration.addProperty(getKey("pinot.server.instance", "tableName"), StringUtils.join(TABLE_NAMES, ',')
        .trim());
    for (final String table : TABLE_NAMES) {
      serverConfiguration.addProperty(getKey("pinot.server.instance", table.trim(), "numQueryExecutorThreads"), "50");
      serverConfiguration.addProperty(getKey("pinot.server.instance", table.trim(), "dataManagerType"), "offline");
      serverConfiguration
          .addProperty(getKey("pinot.server.instance", table.trim(), "readMode"), SERVER_INDEX_READ_MODE);
    }
    serverConfiguration.addProperty("pinot.server.instance.data.manager.class",
        FileBasedInstanceDataManager.class.getName());
    serverConfiguration.addProperty("pinot.server.instance.segment.metadata.loader.class",
        ColumnarSegmentMetadataLoader.class.getName());
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.class",
        StringUtil.join(",", TimeSegmentPruner.class.getSimpleName(), DataSchemaSegmentPruner.class.getSimpleName(),
            ValidSegmentPruner.class.getSimpleName()));
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.TimeSegmentPruner.id", "0");
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.DataSchemaSegmentPruner.id", "1");
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.ValidSegmentPruner.id", "2");
    serverConfiguration.addProperty("pinot.server.query.executor.class", ServerQueryExecutorV1Impl.class.getName());
    serverConfiguration.addProperty("pinot.server.netty.port", SERVER_PORT);
    serverConfiguration.setDelimiterParsingDisabled(false);
    return serverConfiguration;
  }

  @SuppressWarnings("unchecked")
  private void log(PropertiesConfiguration props, String configsFor) {
    LOGGER.info("******************************* configs for : " + configsFor
        + " : ********************************************");

    final Iterator<String> keys = props.getKeys();

    while (keys.hasNext()) {
      final String key = keys.next();
      LOGGER.info(key + " : " + props.getProperty(key));
    }

    LOGGER.info("******************************* configs end for : " + configsFor
        + " : ****************************************");
  }

  private void startServer() {
    try {
      serverInstance.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  private void stopServer() {
    try {
      serverInstance.shutDown();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  private void startBroker() {
    try {
      bld.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  private void stopBroker() {
    try {
      bld.stop();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  public void startAll() throws Exception {
    final PropertiesConfiguration broker = brokerProperties();
    final PropertiesConfiguration server = serverProperties();
    log(broker, "broker");
    log(server, "server");

    System.out.println("************************ 1");
    serverInstance = new ServerInstance();
    System.out.println("************************ 2");
    serverInstance.init(new ServerConf(server), new MetricsRegistry());
    System.out.println("************************ 3");
    bld = new BrokerServerBuilder(broker, null, null, null);
    System.out.println("************************ 4");
    bld.buildNetwork();
    System.out.println("************************ 5");
    bld.buildHTTP();
    System.out.println("************************ 6");

    startServer();
    startBroker();

  }

  public void stopAll() {
    stopServer();
    stopBroker();
  }
}
