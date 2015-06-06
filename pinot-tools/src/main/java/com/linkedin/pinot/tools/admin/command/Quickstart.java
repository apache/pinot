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

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.core.data.readers.CSVRecordReader;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;


public class Quickstart {

  protected enum quickstartCommands {
    exit,
    quit,
    run,
    info,
    help;
  }

  private ControllerStarter controller;
  private BrokerServerBuilder broker;
  private HelixServerStarter server;

  private final File schemaFile, dataFile, tempDir;
  private final String tableName;
  private boolean isStopped = false;

  public Quickstart(File schemaFile, File dataFile, File tempDir, String tableName) throws Exception {
    this.schemaFile = schemaFile;
    this.dataFile = dataFile;
    this.tempDir = tempDir;
    this.tableName = tableName;
    clean();
  }

  public void startAll() throws Exception {
    ZkStarter.startLocalZkServer(2122);
    controller = ControllerStarter.startDefault();
    broker = HelixBrokerStarter.startDefault().getBrokerServerBuilder();
    server = HelixServerStarter.startDefault();
  }

  public void clean() throws Exception {
    File controllerDir = new File("/tmp/PinotController");
    File serverDir1 = new File("/tmp/PinotServer/test");
    File serverDir2 = new File("/tmp/PinotServer/test/8003/index");
    FileUtils.deleteDirectory(controllerDir);
    FileUtils.deleteDirectory(serverDir1);
    FileUtils.deleteDirectory(serverDir2);
    FileUtils.deleteDirectory(tempDir);
  }

  public void stop() throws Exception {
    if (isStopped) {
      return;
    }

    if (server != null) {
      server.stop();
    }

    if (broker != null) {
      broker.stop();
    }

    if (controller != null) {
      controller.stop();
    }

    ZkStarter.stopLocalZkServer(true);

    isStopped = true;
  }

  public void addSchema() throws FileNotFoundException {
    FileUploadUtils.sendFile("localhost", "9000", "schemas", schemaFile.getName(), new FileInputStream(schemaFile),
        schemaFile.length());
  }

  public void addTable() throws JSONException, UnsupportedEncodingException, IOException {
    JSONObject request = ControllerRequestBuilder.addOfflineTableRequest(tableName, "", "", 1);
    AbstractBaseCommand.sendPostRequest("http://localhost:9000/tables", request.toString());
  }

  public void buildSegment() throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setTableName("baseballStats");
    config.setInputFileFormat(FileFormat.CSV);
    config.setIndexOutputDir(new File(tempDir, "segments").getAbsolutePath());
    config.setSegmentName("baseballStats_1");

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setCsvDateColumns(new HashSet<String>());
    readerConfig.setCsvDelimiter(",");

    CSVRecordReader reader = new CSVRecordReader(dataFile.getAbsolutePath(), readerConfig, schema);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, reader);
    driver.build();

    File tarsDir = new File(tempDir, "tars");
    tarsDir.mkdir();
    TarGzCompressionUtils.createTarGzOfDirectory(new File(tempDir, "segments").listFiles()[0].getAbsolutePath(),
        new File(tarsDir, "baseballStats_1").getAbsolutePath());
  }

  public void pushSegment() throws FileNotFoundException {
    for (File file : new File(tempDir, "tars").listFiles()) {
      FileUploadUtils.sendSegmentFile("localhost", "9000", file.getName(), new FileInputStream(file), file.length());
    }
  }

  public JSONObject runQuery(String query) throws Exception {
    return AbstractBaseCommand.postQuery(query, "http://localhost:5001");
  }

  public void printUsageAndInfo() {
    StringBuilder bld = new StringBuilder();
    bld.append("type exit || quit to terminate quickstart");
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
    return schemaFile;
  }

  public File getDataFile() {
    return dataFile;
  }

  public File getTempDir() {
    return tempDir;
  }

  public String getTableName() {
    return tableName;
  }

  public static void main(String[] args) throws Exception {

    File dataFile = new File(Quickstart.class.getClassLoader().getResource("sample_data/baseball.csv").getFile());
    File schemaFile = new File(Quickstart.class.getClassLoader().getResource("sample_data/baseball.schema").getFile());
    File tempDir = new File("/tmp/" + String.valueOf(System.currentTimeMillis()));
    String tableName = "baseballStats";
    final Quickstart runner = new Quickstart(schemaFile, dataFile, tempDir, tableName);

    runner.startAll();
    runner.addSchema();
    runner.addTable();
    runner.buildSegment();
    runner.pushSegment();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          runner.clean();
          runner.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    Desktop desktop = Desktop.isDesktopSupported() ? Desktop.getDesktop() : null;
    if (desktop != null && desktop.isSupported(Desktop.Action.BROWSE)) {
      try {
        desktop.browse(new URI("http://localhost:9000/query/"));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println("quickstart setup complete");
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String line;

      while ((line = br.readLine()) != null) {
        try {
          if (line.startsWith("run")) {
            String query = line.substring(line.indexOf("run") + "run".length(), line.length());
            System.out.println("running query : " + query);
            System.out.println(runner.runQuery(query).toString(1));
            continue;
          }

          quickstartCommands cmd = quickstartCommands.valueOf(line);
          if (cmd == quickstartCommands.quit || cmd == quickstartCommands.exit) {
            runner.clean();
            runner.stop();
            System.exit(0);
          }

          runner.printUsageAndInfo();
        } catch (Exception e) {
          System.out.println("cannot understand command : " + line + " : only commands understood are : ");
        }

      }

    } catch (IOException io) {
      io.printStackTrace();
    }
  }
}
