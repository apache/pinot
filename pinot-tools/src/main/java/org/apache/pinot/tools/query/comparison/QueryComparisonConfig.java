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
package org.apache.pinot.tools.query.comparison;

import java.io.File;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class QueryComparisonConfig extends PropertiesConfiguration {
  private static final String CLUSTER_NAME = "cluster.name";

  private static final String ZOOKEEPER_ADDRESS = "zookeeper.address";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String BROKER_HOST = "broker.host";
  private static final String BROKER_PORT = "broker.port";
  private static final String SERVER_PORT = "server.port";
  private static final String PERF_URL = "perf.url";

  // Used if a reference cluster needs to be started for testing.
  private static final String REF_CONTROLLER_PORT = "ref.controller.port";
  private static final String REF_BROKER_HOST = "ref.broker.host";
  private static final String REF_BROKER_PORT = "ref.broker.port";
  private static final String REF_SERVER_PORT = "ref.server.port";

  private static final String TABLE_NAME = "table.name";
  private static final String TABLE_CONFIG_FILE = "table.config.file";

  public static final String SEGMENT_NAME = "segment.name";
  private static final String INPUT_DATA_DIR = "input.data.dir";
  private static final String SCHEMA_FILE_NAME = "schema.file.name";
  private static final String SEGMENTS_DIR = "segments.dir";

  private static final String QUERY_FILE = "query.file";
  private static final String RESULT_FILE = "result.file";
  private static final String START_ZOOKEEPER = "start.zookeeper";
  private static final String START_CLUSTER = "start.cluster";
  private static final String TIME_COLUMN_NAME = "time.column.name";

  // Used for different test mode
  private static final String FUNCTION_MODE = "function.mode";
  private static final String PERF_MODE = "perf.mode";

  private static final String TIME_UNIT = "time.unit";
  private static final String DEFAULT_CLUSTER_NAME = "QueryComparisonCluster";
  private static final String DEFAULT_ZOOKEEPER_ADDRESS = "localhost:2181";

  private static final String DEFAULT_CONTROLLER_PORT = "9000";
  private static final String DEFAULT_BROKER_HOST = "localhost";
  private static final String DEFAULT_BROKER_PORT = String.valueOf(CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);
  private static final String DEFAULT_SERVER_PORT = String.valueOf(CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);

  private static final String DEFAULT_REF_CONTROLLER_PORT = "10000";
  private static final String DEFAULT_REF_BROKER_HOST = "localhost";
  private static final String DEFAULT_REF_BROKER_PORT = "10001";
  private static final String DEFAULT_REF_SERVER_PORT = "10002";

  private static final boolean DEFAULT_START_ZOOKEEPER = true;
  private static final boolean DEFAULT_ENABLE_STAR_TREE = false;

  private static final boolean DEFAULT_FUNCTION_MODE = true;
  private static final boolean DEFAULT_PERF_MODE = false;

  public static final String DEFAULT_SEGMENT_NAME = "pinotSegment";

  private static final boolean DEFAULT_START_CLUSTER = true;
  private static final String DEFAULT_TIME_COLUMN_NAME = "daysSinceEpoch";
  private static final String DEFAULT_TIME_UNIT = "DAYS";

  public QueryComparisonConfig(File file)
      throws ConfigurationException {
    super(file);
  }

  public String getClusterName() {
    String value = (String) getProperty(CLUSTER_NAME);
    return (value != null) ? value : DEFAULT_CLUSTER_NAME;
  }

  public String getZookeeperAddress() {
    String value = (String) getProperty(ZOOKEEPER_ADDRESS);
    return (value != null) ? value : DEFAULT_ZOOKEEPER_ADDRESS;
  }

  public boolean getStartZookeeper() {
    String value = (String) getProperty(START_ZOOKEEPER);
    return (value != null) ? Boolean.valueOf(value) : DEFAULT_START_ZOOKEEPER;
  }

  public boolean getStartCluster() {
    String value = (String) getProperty(START_CLUSTER);
    return (value != null) ? Boolean.valueOf(value) : DEFAULT_START_CLUSTER;
  }

  public boolean getFunctionMode() {
    String value = (String) getProperty(FUNCTION_MODE);
    return (value != null) ? Boolean.valueOf(value) : DEFAULT_FUNCTION_MODE;
  }

  public boolean getPerfMode() {
    String value = (String) getProperty(PERF_MODE);
    return (value != null) ? Boolean.valueOf(value) : DEFAULT_PERF_MODE;
  }

  public String getControllerPort() {
    String value = (String) getProperty(CONTROLLER_PORT);
    return (value != null) ? value : DEFAULT_CONTROLLER_PORT;
  }

  public String getBrokerHost() {
    String value = (String) getProperty(BROKER_HOST);
    return (value != null) ? value : DEFAULT_BROKER_HOST;
  }

  public String getBrokerPort() {
    String value = (String) getProperty(BROKER_PORT);
    return (value != null) ? value : DEFAULT_BROKER_PORT;
  }

  public String getServerPort() {
    String value = (String) getProperty(SERVER_PORT);
    return (value != null) ? value : DEFAULT_SERVER_PORT;
  }

  public String getPerfUrl() {
    String value = (String) getProperty(PERF_URL);
    return value;
  }

  public String getRefControllerPort() {
    String value = (String) getProperty(REF_CONTROLLER_PORT);
    return (value != null) ? value : DEFAULT_REF_CONTROLLER_PORT;
  }

  public String getRefBrokerHost() {
    String value = (String) getProperty(REF_BROKER_HOST);
    return (value != null) ? value : DEFAULT_REF_BROKER_HOST;
  }

  public String getRefBrokerPort() {
    String value = (String) getProperty(REF_BROKER_PORT);
    return (value != null) ? value : DEFAULT_REF_BROKER_PORT;
  }

  public String getRefServerPort() {
    String value = (String) getProperty(REF_SERVER_PORT);
    return (value != null) ? value : DEFAULT_REF_SERVER_PORT;
  }

  public String getTableName() {
    return (String) getProperty(TABLE_NAME);
  }

  public String getTableConfigFile() {
    return (String) getProperty(TABLE_CONFIG_FILE);
  }

  public String getSegmentName() {
    String value = (String) getProperty(SEGMENT_NAME);
    return (value != null) ? value : DEFAULT_SEGMENT_NAME;
  }

  public String getInputDataDir() {
    return (String) getProperty(INPUT_DATA_DIR);
  }

  public String getSegmentsDir() {
    return (String) getProperty(SEGMENTS_DIR);
  }

  public String getSchemaFileName() {
    return (String) getProperty(SCHEMA_FILE_NAME);
  }

  public String getQueryFile() {
    return (String) getProperty(QUERY_FILE);
  }

  public String getResultFile() {
    return (String) getProperty(RESULT_FILE);
  }

  public String getTimeColumnName() {
    String value = (String) getProperty(TIME_COLUMN_NAME);
    return (value != null) ? value : DEFAULT_TIME_COLUMN_NAME;
  }

  public String getTimeUnit() {
    String value = (String) getProperty(TIME_UNIT);
    return (value != null) ? value : DEFAULT_TIME_UNIT;
  }
}
