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
package org.apache.pinot.tools.perf;

import org.apache.pinot.common.utils.CommonConstants;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;


public class PerfBenchmarkDriverConf {

  String clusterName = "PinotPerfTestCluster";
  /*
   * zookeeper configuration
   */ String zkHost = "localhost";
  int zkPort = 2191;
  boolean startZookeeper = true;

  //server configuration
  String serverInstanceName;
  String serverHost = "localhost";
  int serverPort = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  //if true, serverHost is set to localhost
  boolean startServer = true;
  //deletes all indexes on startup
  boolean cleanOnStartup = false;
  String serverInstanceDataDir;
  String serverInstanceSegmentTarDir;

  //controller configuration
  String controllerHost = "localhost";
  int controllerPort = 8100;
  String controllerDataDir;
  //if this is true, controllerHost is automatically set to "localhost"
  boolean startController = true;

  //broker configuration
  int brokerPort = CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;
  String brokerHost = "localhost";
  boolean startBroker = true;

  //resource configuration

  boolean configureResources = false;

  String tableName;

  String schemaFileNamePath;

  //Query
  boolean runQueries = false;

  String queriesDirectory;

  String resultsOutputDirectory;

  boolean verbose = false;

  String dialect = "pql";

  public String getClusterName() {
    return clusterName;
  }

  public String getZkHost() {
    return zkHost;
  }

  public int getZkPort() {
    return zkPort;
  }

  public boolean isStartZookeeper() {
    return startZookeeper;
  }

  public String getServerHost() {
    return serverHost;
  }

  public String getServerInstanceName() {
    return serverInstanceName;
  }

  public int getServerPort() {
    return serverPort;
  }

  public boolean shouldStartServer() {
    return startServer;
  }

  public boolean isCleanOnStartup() {
    return cleanOnStartup;
  }

  public String getServerInstanceDataDir() {
    return serverInstanceDataDir;
  }

  public String getServerInstanceSegmentTarDir() {
    return serverInstanceSegmentTarDir;
  }

  public String getControllerHost() {
    return controllerHost;
  }

  public int getControllerPort() {
    return controllerPort;
  }

  public String getControllerDataDir() {
    return controllerDataDir;
  }

  public boolean shouldStartController() {
    return startController;
  }

  public int getBrokerPort() {
    return brokerPort;
  }

  public String getBrokerHost() {
    return brokerHost;
  }

  public void setBrokerHost(String brokerHost) {
    this.brokerHost = brokerHost;
  }

  public boolean shouldStartBroker() {
    return startBroker;
  }

  public String getQueriesDirectory() {
    return queriesDirectory;
  }

  public String getResultsOutputDirectory() {
    return resultsOutputDirectory;
  }

  public void setStartServer(boolean startServer) {
    this.startServer = startServer;
  }

  public void setStartController(boolean startController) {
    this.startController = startController;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public void setZkPort(int zkPort) {
    this.zkPort = zkPort;
  }

  public void setStartZookeeper(boolean startZookeeper) {
    this.startZookeeper = startZookeeper;
  }

  public void setServerInstanceName(String serverInstanceName) {
    this.serverInstanceName = serverInstanceName;
  }

  public void setServerHost(String serverHost) {
    this.serverHost = serverHost;
  }

  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }

  public void setCleanOnStartup(boolean cleanOnStartup) {
    this.cleanOnStartup = cleanOnStartup;
  }

  public void setServerInstanceDataDir(String serverInstanceDataDir) {
    this.serverInstanceDataDir = serverInstanceDataDir;
  }

  public void setServerInstanceSegmentTarDir(String serverInstanceSegmentTarDir) {
    this.serverInstanceSegmentTarDir = serverInstanceSegmentTarDir;
  }

  public void setControllerHost(String controllerHost) {
    this.controllerHost = controllerHost;
  }

  public void setControllerPort(int controllerPort) {
    this.controllerPort = controllerPort;
  }

  public void setControllerDataDir(String controllerDataDir) {
    this.controllerDataDir = controllerDataDir;
  }

  public void setBrokerPort(int brokerPort) {
    this.brokerPort = brokerPort;
  }

  public void setStartBroker(boolean startBroker) {
    this.startBroker = startBroker;
  }

  public boolean isRunQueries() {
    return runQueries;
  }

  public void setRunQueries(boolean runQueries) {
    this.runQueries = runQueries;
  }

  public void setQueriesDirectory(String queriesDirectory) {
    this.queriesDirectory = queriesDirectory;
  }

  public void setResultsOutputDirectory(String resultsOutputDirectory) {
    this.resultsOutputDirectory = resultsOutputDirectory;
  }

  public boolean isConfigureResources() {
    return configureResources;
  }

  public void setConfigureResources(boolean configureResources) {
    this.configureResources = configureResources;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getSchemaFileNamePath() {
    return schemaFileNamePath;
  }

  public void setSchemaFileNamePath(String schemaFileNamePath) {
    this.schemaFileNamePath = schemaFileNamePath;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public String getDialect() {
    return dialect;
  }

  public void setDialect(String dialect) {
    this.dialect = dialect;
  }
}
