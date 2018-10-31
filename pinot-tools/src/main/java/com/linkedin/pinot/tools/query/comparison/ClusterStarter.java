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
package com.linkedin.pinot.tools.query.comparison;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.tools.admin.command.AddTableCommand;
import com.linkedin.pinot.tools.admin.command.CreateSegmentCommand;
import com.linkedin.pinot.tools.admin.command.DeleteClusterCommand;
import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.StartBrokerCommand;
import com.linkedin.pinot.tools.admin.command.StartControllerCommand;
import com.linkedin.pinot.tools.admin.command.StartServerCommand;
import com.linkedin.pinot.tools.admin.command.StartZookeeperCommand;
import com.linkedin.pinot.tools.admin.command.UploadSegmentCommand;
import com.linkedin.pinot.tools.perf.PerfBenchmarkDriver;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;


public class ClusterStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStarter.class);

  private String _controllerPort;
  private String _brokerHost;
  private String _brokerPort;
  private String _serverPort;
  private String _perfUrl;
  private String _zkAddress;
  private String _clusterName;
  private String _localhost;

  private String _tableName;
  private String _tableConfigFile;
  private String _timeColumnName;
  private String _timeUnit;

  private String _inputDataDir;
  private String _segmentName;
  private String _schemaFileName;
  private String _segmentDirName;

  private boolean _startZookeeper;
  private boolean _enableStarTreeIndex;

  private static final long TIMEOUT_IN_MILLISECONDS = 200 * 1000;

  ClusterStarter(QueryComparisonConfig config)
      throws SocketException, UnknownHostException {

    _segmentName = config.getSegmentName();
    _schemaFileName = config.getSchemaFileName();
    _inputDataDir = config.getInputDataDir();
    _segmentDirName = config.getSegmentsDir();

    _localhost = NetUtil.getHostAddress();

    _zkAddress = config.getZookeeperAddress();
    _clusterName = config.getClusterName();

    _controllerPort = config.getControllerPort();
    _brokerHost = config.getBrokerHost();
    _brokerPort = config.getBrokerPort();
    _serverPort = config.getServerPort();
    _perfUrl = config.getPerfUrl();
    _startZookeeper = config.getStartZookeeper();

    _tableName = config.getTableName();
    _timeColumnName = config.getTimeColumnName();
    _timeUnit = config.getTimeUnit();
    _tableConfigFile = config.getTableConfigFile();

    _enableStarTreeIndex = false;
  }

  public ClusterStarter setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public ClusterStarter setBrokerHost(String brokerHost) {
    _brokerHost = brokerHost;
    return this;
  }

  public ClusterStarter setBrokerPort(String brokerPort) {
    _brokerPort = brokerPort;
    return this;
  }

  public ClusterStarter setServerPort(String serverPort) {
    _serverPort = serverPort;
    return this;
  }

  public ClusterStarter setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public ClusterStarter setStartZookeeper(boolean startZookeeper) {
    _startZookeeper = startZookeeper;
    return this;
  }

  public ClusterStarter setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public ClusterStarter setSegmentDirName(String segmentDirName) {
    _segmentDirName = segmentDirName;
    return this;
  }

  public ClusterStarter setEnableStarTree(boolean value) {
    _enableStarTreeIndex = value;
    return this;
  }

  public ClusterStarter setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  private void startZookeeper()
      throws IOException {
    if (_startZookeeper) {
      StartZookeeperCommand zkStarter = new StartZookeeperCommand();
      zkStarter.execute();
    }
  }

  private void startController()
      throws Exception {

    // Delete existing cluster first.
    DeleteClusterCommand deleteClusterCommand = new DeleteClusterCommand().setClusterName(_clusterName);
    deleteClusterCommand.execute();

    StartControllerCommand controllerStarter =
        new StartControllerCommand().setControllerPort(_controllerPort).setZkAddress(_zkAddress)
            .setClusterName(_clusterName);

    controllerStarter.execute();
  }

  private void startBroker()
      throws Exception {
    StartBrokerCommand brokerStarter =
        new StartBrokerCommand().setClusterName(_clusterName).setPort(Integer.valueOf(_brokerPort));
    brokerStarter.execute();
  }

  private void startServer()
      throws Exception {
    StartServerCommand serverStarter =
        new StartServerCommand().setPort(Integer.valueOf(_serverPort)).setClusterName(_clusterName);
    serverStarter.execute();
  }

  private void addTable()
      throws Exception {
    if (_tableConfigFile != null) {
      AddTableCommand addTableCommand =
          new AddTableCommand().setControllerPort(_controllerPort).setFilePath(_tableConfigFile).setExecute(true);
      addTableCommand.execute();
      return;
    }

    if (_tableName == null) {
      LOGGER.error("Table info not specified in configuration, please specify either config file or table name");
      return;
    }

    String controllerAddress = "http://" + _localhost + ":" + _controllerPort;
    String tableJSONConfigString =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(_tableName)
            .setTimeColumnName(_timeColumnName)
            .setTimeType(_timeUnit)
            .setNumReplicas(3)
            .setBrokerTenant("broker")
            .setServerTenant("server")
            .build()
            .toJSONConfigString();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(controllerAddress).forTableCreate(), tableJSONConfigString);
  }

  private void uploadData()
      throws Exception {
    UploadSegmentCommand segmentUploader =
        new UploadSegmentCommand().setSegmentDir(_segmentDirName).setControllerHost(_localhost)
            .setControllerPort(_controllerPort);
    segmentUploader.execute();
    PerfBenchmarkDriver.waitForExternalViewUpdate(_zkAddress, _clusterName, TIMEOUT_IN_MILLISECONDS);
  }

  private void createSegments()
      throws Exception {
    if (_inputDataDir != null) {
      CreateSegmentCommand segmentCreator =
          new CreateSegmentCommand().setDataDir(_inputDataDir).setSchemaFile(_schemaFileName)
              .setTableName(_tableName).setSegmentName(_segmentName).setOutDir(_segmentDirName).setOverwrite(true)
              .setEnableStarTreeIndex(_enableStarTreeIndex);

      segmentCreator.execute();
    }
  }

  public void start()
      throws Exception {
    startZookeeper();
    startController();
    startBroker();
    startServer();
    addTable();

    createSegments();
    uploadData();
  }

  public String query(String query)
      throws Exception {
    LOGGER.debug("Running query on Pinot Cluster");
    PostQueryCommand queryRunner =
        new PostQueryCommand().setQuery(query).setBrokerHost(_brokerHost).setBrokerPort(_brokerPort);
    return queryRunner.run();
  }

  public int perfQuery(String query)
      throws Exception {
    LOGGER.debug("Running perf query on Pinot Cluster");
    String encodedQuery = URLEncoder.encode(query, "UTF-8");
    String brokerUrl = _perfUrl + encodedQuery;
    LOGGER.info("Executing command: " + brokerUrl);
    URLConnection conn = new URL(brokerUrl).openConnection();
    conn.setDoOutput(true);

    long startTime = System.currentTimeMillis();
    InputStream input = conn.getInputStream();
    long endTime = System.currentTimeMillis();

    BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    LOGGER.debug("Actual response: " + sb.toString());

    return (int) (endTime - startTime);
  }
}
