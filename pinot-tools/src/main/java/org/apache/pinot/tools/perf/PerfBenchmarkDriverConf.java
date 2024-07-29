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

import org.apache.pinot.spi.utils.CommonConstants;


public class PerfBenchmarkDriverConf {

  String _clusterName = "PinotPerfTestCluster";
  /*
   * zookeeper configuration
   */ String _zkHost = "localhost";
  int _zkPort = 2191;
  boolean _startZookeeper = true;

  //server configuration
  String _serverInstanceName;
  String _serverHost = "localhost";
  int _serverPort = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  //if true, serverHost is set to localhost
  boolean _startServer = true;
  //deletes all indexes on startup
  boolean _cleanOnStartup = false;
  String _serverInstanceDataDir;
  String _serverInstanceSegmentTarDir;

  //controller configuration
  String _controllerHost = "localhost";
  int _controllerPort = 8100;
  String _controllerDataDir;
  //if this is true, controllerHost is automatically set to "localhost"
  boolean _startController = true;

  //broker configuration
  int _brokerPort = CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;
  String _brokerHost = "localhost";
  String _brokerURL;
  boolean _startBroker = true;

  //resource configuration

  boolean _configureResources = false;

  String _tableName;

  String _schemaFileNamePath;

  //Query
  boolean _runQueries = false;

  String _queriesDirectory;

  String _resultsOutputDirectory;

  boolean _verbose = false;
  private String _user;
  private String _password;
  private String _authToken;
  private String _authTokenUrl;

  public String getClusterName() {
    return _clusterName;
  }

  public String getZkHost() {
    return _zkHost;
  }

  public int getZkPort() {
    return _zkPort;
  }

  public boolean isStartZookeeper() {
    return _startZookeeper;
  }

  public String getServerHost() {
    return _serverHost;
  }

  public String getServerInstanceName() {
    return _serverInstanceName;
  }

  public int getServerPort() {
    return _serverPort;
  }

  public boolean shouldStartServer() {
    return _startServer;
  }

  public boolean isCleanOnStartup() {
    return _cleanOnStartup;
  }

  public String getServerInstanceDataDir() {
    return _serverInstanceDataDir;
  }

  public String getServerInstanceSegmentTarDir() {
    return _serverInstanceSegmentTarDir;
  }

  public String getControllerHost() {
    return _controllerHost;
  }

  public int getControllerPort() {
    return _controllerPort;
  }

  public String getControllerDataDir() {
    return _controllerDataDir;
  }

  public boolean shouldStartController() {
    return _startController;
  }

  public int getBrokerPort() {
    return _brokerPort;
  }

  public String getBrokerHost() {
    return _brokerHost;
  }

  public void setBrokerHost(String brokerHost) {
    _brokerHost = brokerHost;
  }

  public String getBrokerURL() {
    return _brokerURL;
  }

  public void setBrokerURL(String brokerURL) {
    _brokerURL = brokerURL;
  }

  public boolean shouldStartBroker() {
    return _startBroker;
  }

  public String getQueriesDirectory() {
    return _queriesDirectory;
  }

  public String getResultsOutputDirectory() {
    return _resultsOutputDirectory;
  }

  public void setStartServer(boolean startServer) {
    _startServer = startServer;
  }

  public void setStartController(boolean startController) {
    _startController = startController;
  }

  public void setClusterName(String clusterName) {
    _clusterName = clusterName;
  }

  public void setZkHost(String zkHost) {
    _zkHost = zkHost;
  }

  public void setZkPort(int zkPort) {
    _zkPort = zkPort;
  }

  public void setStartZookeeper(boolean startZookeeper) {
    _startZookeeper = startZookeeper;
  }

  public void setServerInstanceName(String serverInstanceName) {
    _serverInstanceName = serverInstanceName;
  }

  public void setServerHost(String serverHost) {
    _serverHost = serverHost;
  }

  public void setServerPort(int serverPort) {
    _serverPort = serverPort;
  }

  public void setCleanOnStartup(boolean cleanOnStartup) {
    _cleanOnStartup = cleanOnStartup;
  }

  public void setServerInstanceDataDir(String serverInstanceDataDir) {
    _serverInstanceDataDir = serverInstanceDataDir;
  }

  public void setServerInstanceSegmentTarDir(String serverInstanceSegmentTarDir) {
    _serverInstanceSegmentTarDir = serverInstanceSegmentTarDir;
  }

  public void setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
  }

  public void setControllerPort(int controllerPort) {
    _controllerPort = controllerPort;
  }

  public void setControllerDataDir(String controllerDataDir) {
    _controllerDataDir = controllerDataDir;
  }

  public void setBrokerPort(int brokerPort) {
    _brokerPort = brokerPort;
  }

  public void setStartBroker(boolean startBroker) {
    _startBroker = startBroker;
  }

  public boolean isRunQueries() {
    return _runQueries;
  }

  public void setRunQueries(boolean runQueries) {
    _runQueries = runQueries;
  }

  public void setQueriesDirectory(String queriesDirectory) {
    _queriesDirectory = queriesDirectory;
  }

  public void setResultsOutputDirectory(String resultsOutputDirectory) {
    _resultsOutputDirectory = resultsOutputDirectory;
  }

  public boolean isConfigureResources() {
    return _configureResources;
  }

  public void setConfigureResources(boolean configureResources) {
    _configureResources = configureResources;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public String getSchemaFileNamePath() {
    return _schemaFileNamePath;
  }

  public void setSchemaFileNamePath(String schemaFileNamePath) {
    _schemaFileNamePath = schemaFileNamePath;
  }

  public boolean isVerbose() {
    return _verbose;
  }

  public void setVerbose(boolean verbose) {
    _verbose = verbose;
  }

  public void setUser(String user) {
    _user = user;
  }

  public String getUser() {
    return _user;
  }

  public void setPassword(String password) {
    _password = password;
  }

  public String getPassword() {
    return _password;
  }

  public void setAuthToken(String authToken) {
    _authToken = authToken;
  }

  public String getAuthToken() {
    return _authToken;
  }

  public void setAuthTokenUrl(String authTokenUrl) {
    _authTokenUrl = authTokenUrl;
  }

  public String getAuthTokenUrl() {
    return _authTokenUrl;
  }
}
