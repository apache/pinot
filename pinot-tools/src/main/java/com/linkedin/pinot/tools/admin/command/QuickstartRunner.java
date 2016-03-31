/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.admin.command;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.QuickstartTableRequest;


public class QuickstartRunner {
  private static final String _clusterName = "QuickStartCluster";

  private final static String _localHost = "localhost";
  private final static String _controllerAddress = "http://localhost:9000";
  private final static String _segmentName = "baseballStats";
  private final static String TMP_DIR = System.getProperty("java.io.dir") + File.separator;

  private static int _zkPort = 2123;
  private final static String _zkAddress = "localhost:" + String.valueOf(_zkPort);

  private static int _brokerPort = 7000;
  private static int _serverPort = 8000;
  private static int _controllerPort = 9000;

  protected enum quickstartCommands {
    run,
    info,
    help;
  }

  private boolean _isStopped = false;
  private final List<QuickstartTableRequest> tableRequests;
  private File _tempDir;
  private int numServers;
  private int numBrokers;
  private int numControllers;
  private List<Integer> controllerPorts = new ArrayList<>();
  private List<Integer> serverPorts = new ArrayList<>();
  private List<Integer> brokerPorts = new ArrayList<>();
  private List<String> segments = new ArrayList<>();
  private List<String> dirs = new ArrayList<>();
  private boolean enableIsolation = true;

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, File tempDir) throws Exception {
    this.tableRequests = tableRequests;
    this._tempDir = tempDir;
    this.numBrokers = numBrokers;
    this.numControllers = numControllers;
    this.numServers = numServers;
    clean();
  }

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, File tempDir, boolean enableIsolation) throws Exception {
    this.tableRequests = tableRequests;
    this._tempDir = tempDir;
    this.numBrokers = numBrokers;
    this.numControllers = numControllers;
    this.numServers = numServers;
    this.enableIsolation = enableIsolation;
    clean();
  }

  void StartZookeeper() throws IOException {
    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(_zkPort);
    zkStarter.execute();
  }

  void StartController() throws Exception {
    for (int i = 0; i < numControllers; i++) {
      StartControllerCommand controllerStarter = new StartControllerCommand();
      controllerStarter.setControllerPort(String.valueOf(_controllerPort)).setZkAddress(_zkAddress)
          .setClusterName(_clusterName).setTenantIsolation(enableIsolation);
      controllerStarter.execute();
      controllerPorts.add(new Integer(_controllerPort));
      _controllerPort = _controllerPort + 1;
    }
  }

  void StartBroker() throws Exception {
    for (int i = 0; i < numBrokers; i++) {
      StartBrokerCommand brokerStarter = new StartBrokerCommand();
      brokerStarter.setClusterName(_clusterName).setPort(_brokerPort).setZkAddress(_zkAddress);
      brokerStarter.execute();
      brokerPorts.add(new Integer(_brokerPort));
      _brokerPort = _brokerPort + 1;
    }
  }

  void StartServer() throws Exception {
    for (int i = 0; i < numServers; i++) {
      StartServerCommand serverStarter = new StartServerCommand();
      serverStarter.setClusterName(_clusterName).setZkAddress(_zkAddress).setPort(_serverPort)
          .setDataDir(TMP_DIR + "PinotServerData" + String.valueOf(i))
          .setSegmentDir(TMP_DIR + "PinotServerSegment" + String.valueOf(i));
      serverStarter.execute();
      dirs.add(TMP_DIR + "PinotServerData" + String.valueOf(i));
      dirs.add(TMP_DIR + "PinotServerSegment" + String.valueOf(i));
      serverPorts.add(new Integer(_serverPort));
      _serverPort = _serverPort + 1;
    }
  }

  public void startAll() throws Exception {
    StartZookeeper();
    StartController();
    StartBroker();
    StartServer();
  }

  public void clean() throws Exception {
    File controllerDir = new File(TMP_DIR + "PinotController");
    File serverDir1 = new File(TMP_DIR + "PinotServer/test");
    File serverDir2 = new File(TMP_DIR + "PinotServer/test/8003/index");
    FileUtils.deleteDirectory(controllerDir);
    FileUtils.deleteDirectory(serverDir1);
    FileUtils.deleteDirectory(serverDir2);
    FileUtils.deleteDirectory(_tempDir);
    for (String dir : dirs) {
      FileUtils.deleteDirectory(new File(dir));
    }
  }

  public void stop() throws Exception {
    if (_isStopped) {
      return;
    }

    StopProcessCommand stopper = new StopProcessCommand(false);
    stopper.stopController().stopBroker().stopServer().stopZookeeper();
    stopper.execute();

    _isStopped = true;
  }

  public void createServerTenantWith(int numOffline, int numRealtime, String tenantName) throws Exception {
    AddTenantCommand command = new AddTenantCommand().setControllerUrl("http://localhost:" + controllerPorts.get(0))
        .setName(tenantName).setOffline(1).setRealtime(1).setInstances(2).setRole(TenantRole.SERVER).setExecute(true);
    command.execute();
  }

  public void createBrokerTenantWith(int number, String tenantName) throws Exception {
    AddTenantCommand command = new AddTenantCommand().setControllerUrl("http://localhost:" + controllerPorts.get(0))
        .setName(tenantName).setInstances(number).setRole(TenantRole.BROKER).setExecute(true);
    command.execute();
  }

  public void addSchema() throws Exception {
    for (QuickstartTableRequest request : tableRequests) {
      AddSchemaCommand schemaAdder = new AddSchemaCommand();
      schemaAdder.setControllerHost(_localHost).setControllerPort(String.valueOf(controllerPorts.get(0)));
      schemaAdder.setSchemaFilePath(request.getSchemaFile().getAbsolutePath()).setExecute(true);
      schemaAdder.execute();
    }
  }

  public void addTable() throws Exception {

    for (QuickstartTableRequest request : tableRequests) {
      AddTableCommand tableAdder = new AddTableCommand();
      tableAdder.setFilePath(request.getTableRequestFile().getAbsolutePath())
          .setControllerPort(String.valueOf(controllerPorts.get(0))).setExecute(true);
      tableAdder.execute();
    }

  }

  public void buildSegment() throws Exception {
    for (QuickstartTableRequest request : tableRequests) {
      if (request.getTableType() == TableType.OFFLINE) {
        CreateSegmentCommand segmentBuilder = new CreateSegmentCommand();
        File tempDir = new File(_tempDir, request.getTableName() + "_segment");
        segmentBuilder.setDataDir(request.getDataDir().getAbsolutePath()).setFormat(request.getSegmentFileFormat())
            .setSchemaFile(request.getSchemaFile().getAbsolutePath()).setTableName(request.getTableName())
            .setSegmentName(request.getTableName() + "_" + String.valueOf(System.currentTimeMillis()))
            .setOutDir(tempDir.getAbsolutePath()).setOverwrite(true);
        segmentBuilder.execute();
        segments.add(tempDir.getAbsolutePath());
      }
    }
  }

  public void pushSegment() throws Exception {

    for (String segmentDir : segments) {
      UploadSegmentCommand segmentUploader = new UploadSegmentCommand();
      segmentUploader.setControllerHost(_localHost).setControllerPort(String.valueOf(--_controllerPort))
          .setSegmentDir(new File(segmentDir).getAbsolutePath());
      segmentUploader.execute();
    }
  }

  public JSONObject runQuery(String query) throws Exception {
    PostQueryCommand queryCommand = new PostQueryCommand();
    int brokerPort = brokerPorts.get(new java.util.Random().nextInt(brokerPorts.size()));
    queryCommand.setBrokerPort(String.valueOf(brokerPort)).setQuery(query);
    String result = queryCommand.run();
    return new JSONObject(result);
  }

  public void printUsageAndInfo() {
    StringBuilder bld = new StringBuilder();
    bld.append("Ctrl C to terminate quickstart");
    bld.append("\n");

    bld.append("query console can be found on http://localhost:9000/query");
    bld.append("\n");

    bld.append("some sample queries that you can try out");
    bld.append("\n");

    bld.append("<TOTAL Number Of Records : > select count(*) from baseballStats limit 0");
    bld.append("\n");

    bld.append(
        "<Top 10 Batters in the year 2000 : > select sum('runs') from baseballStats where yearID='2000' group by playerName top 10 limit 0");
    bld.append("\n");

    bld.append("You can also run the query on the terminal by typing RUN || run <QUERY>");

    System.out.println(bld.toString());
  }

  public File getTempDir() {
    return _tempDir;
  }
}
