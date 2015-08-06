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

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import com.linkedin.pinot.core.data.readers.FileFormat;


public class QuickstartRunner {
  private static final String _clusterName = "QuickStartCluster";
  private final static String _zkAddress = "localhost:2181";

  private final static String _localHost = "localhost";
  private final static String _controllerPort = "9000";
  private final static String _controllerAddress = "http://localhost:9000";
  private final static String _segmentName = "baseballStats";
  private final static String TMP_DIR = System.getProperty("java.io.dir") + File.separator;

  private final static int _zkPort = 2181;
  private final static int _brokerPort = 8099;
  private final static int _serverPort = 8098;

  protected enum quickstartCommands {
    run,
    info,
    help;
  }

  private final File _schemaFile, _dataDir, _tempDir;
  private final String _tableName;
  private boolean _isStopped = false;
  private String _tableCreateRequestJsonFile;

  public QuickstartRunner(File schemaFile, File dataDir, File tempDir, String tableName,
      String tableCreateRequestJsonFile) throws Exception {
    this._schemaFile = schemaFile;
    this._dataDir = dataDir;
    this._tempDir = tempDir;
    this._tableName = tableName;
    _tableCreateRequestJsonFile = tableCreateRequestJsonFile;
    clean();
  }

  void StartZookeeper() throws IOException {
    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(_zkPort);
    zkStarter.execute();
  }

  void StartController() throws Exception {
    StartControllerCommand controllerStarter = new StartControllerCommand();
    controllerStarter.setControllerPort(_controllerPort).setZkAddress(_zkAddress).setClusterName(_clusterName);
    controllerStarter.execute();
  }

  void StartBroker() throws Exception {
    StartBrokerCommand brokerStarter = new StartBrokerCommand();
    brokerStarter.setClusterName(_clusterName).setPort(_brokerPort).setZkAddress(_zkAddress);
    brokerStarter.execute();
  }

  void StartServer() throws Exception {
    StartServerCommand serverStarter = new StartServerCommand();
    serverStarter.setClusterName(_clusterName).setZkAddress(_zkAddress).setPort(_serverPort)
        .setDataDir(TMP_DIR + "PinotServerData").setSegmentDir(TMP_DIR + "PinotServerSegment");
    serverStarter.execute();
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

  public void addSchema() throws Exception {
    AddSchemaCommand schemaAdder = new AddSchemaCommand();
    schemaAdder.setControllerHost(_localHost).setControllerPort(_controllerPort)
        .setSchemaFilePath(_schemaFile.getAbsolutePath()).setExecute(true);

    schemaAdder.execute();
  }

  public void addTable() throws Exception {
    AddTableCommand tableAdder = new AddTableCommand();
    tableAdder.setFilePath(_tableCreateRequestJsonFile).setControllerPort(_controllerPort).setExecute(true);
    tableAdder.execute();
  }

  public void buildSegment() throws Exception {
    CreateSegmentCommand segmentBuilder = new CreateSegmentCommand();
    segmentBuilder.setDataDir(_dataDir.getAbsolutePath()).setFormat(FileFormat.CSV)
        .setSchemaFile(_schemaFile.getAbsolutePath()).setTableName(_tableName).setSegmentName(_segmentName)
        .setOutputDir(new File(_tempDir, "segments").getAbsolutePath()).setOverwrite(true);
    segmentBuilder.execute();
  }

  public void pushSegment() throws Exception {
    UploadSegmentCommand segmentUploader = new UploadSegmentCommand();

    segmentUploader.setControllerHost(_localHost).setControllerPort(_controllerPort)
        .setSegmentDir(new File(_tempDir, "segments").getAbsolutePath());
    segmentUploader.execute();
  }

  public JSONObject runQuery(String query) throws Exception {
    PostQueryCommand queryCommand = new PostQueryCommand();
    queryCommand.setBrokerPort(Integer.toString(_brokerPort)).setQuery(query);
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

    bld.append("<Top 10 Batters in the year 2000 : > select sum('runs') from baseballStats where yearID='2000' group by playerName top 10 limit 0");
    bld.append("\n");

    bld.append("You can also run the query on the terminal by typing RUN || run <QUERY>");

    System.out.println(bld.toString());
  }

  public File getSchemaFile() {
    return _schemaFile;
  }

  public File getDataFile() {
    return _dataDir;
  }

  public File getTempDir() {
    return _tempDir;
  }

  public String getTableName() {
    return _tableName;
  }
}
