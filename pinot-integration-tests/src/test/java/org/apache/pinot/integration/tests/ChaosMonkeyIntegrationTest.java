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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jnr.constants.platform.Signal;
import jnr.posix.POSIXFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Monkeys and chaos.
 */
// TODO: clean up this test
public class ChaosMonkeyIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChaosMonkeyIntegrationTest.class);
  private static final String TOTAL_RECORD_COUNT = "1000";
  private static final String SEGMENT_COUNT = "25";
  private final List<Process> _processes = new ArrayList<>();
  private final String AVRO_DIR = "/tmp/temp-avro-" + getClass().getName();
  private final String SEGMENT_DIR = "/tmp/temp-segment-" + getClass().getName();

  private Process runAdministratorCommand(String[] args) {
    String classpath = System.getProperty("java.class.path");
    System.getProperties().setProperty("pinot.admin.system.exit", "false");
    List<String> completeArgs = new ArrayList<>();
    completeArgs.add("java");
    completeArgs.add("-Xms4G");
    completeArgs.add("-Xmx4G");
    completeArgs.add("-cp");
    completeArgs.add(classpath);
    completeArgs.add(PinotAdministrator.class.getName());
    completeArgs.addAll(Arrays.asList(args));

    try {
      Process process = new ProcessBuilder(completeArgs).redirectError(ProcessBuilder.Redirect.INHERIT).
          redirectOutput(ProcessBuilder.Redirect.INHERIT).start();
      synchronized (_processes) {
        _processes.add(process);
      }
      return process;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void sendSignalToProcess(Process process, Signal signal) {
    int processPid = getProcessPid(process);
    if (processPid != -1) {
      LOGGER.info("Sending signal {} to process {}", signal.intValue(), processPid);
      POSIXFactory.getNativePOSIX().kill(processPid, signal.intValue());
    }
  }

  private int getProcessPid(Process process) {
    Class<? extends Process> clazz = process.getClass();
    try {
      Field field = clazz.getDeclaredField("pid");
      field.setAccessible(true);
      return field.getInt(process);
    } catch (NoSuchFieldException e) {
      return -1;
    } catch (IllegalAccessException e) {
      return -1;
    }
  }

  private Process startZookeeper() {
    return runAdministratorCommand(new String[]{"StartZookeeper", "-zkPort", "2191"});
  }

  private Process startController() {
    return runAdministratorCommand(new String[]{
        "StartController", "-zkAddress", "localhost:2191", "-controllerPort", "39000", "-dataDir",
        "/tmp/ChaosMonkeyClusterController"
    });
  }

  private Process startBroker() {
    return runAdministratorCommand(new String[]{"StartBroker", "-brokerPort", "8099", "-zkAddress", "localhost:2191"});
  }

  private Process startServer() {
    return runAdministratorCommand(new String[]{
        "StartServer", "-serverPort", "8098", "-zkAddress", "localhost:2191", "-dataDir",
        "/tmp/ChaosMonkeyCluster/data", "-segmentDir", "/tmp/ChaosMonkeyCluster/segments"
    });
  }

  private void generateData()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema.json"));
    String schemaAnnotationsFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema-annotations.json"));
    runAdministratorCommand(new String[]{
        "GenerateData", "-numRecords", TOTAL_RECORD_COUNT, "-numFiles", SEGMENT_COUNT, "-schemaFile", schemaFile,
        "-schemaAnnotationFile", schemaAnnotationsFile, "-overwrite", "-outDir", AVRO_DIR
    }).waitFor();
  }

  private void createTable()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema.json"));
    String createTableFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-create-table.json"));
    runAdministratorCommand(new String[]{
        "AddTable", "-controllerPort", "39000", "-schemaFile", schemaFile, "-tableConfigFile", createTableFile, "-exec"
    }).waitFor();
  }

  private void convertData()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema.json"));
    runAdministratorCommand(new String[]{
        "CreateSegment", "-schemaFile", schemaFile, "-dataDir", AVRO_DIR, "-tableName", "myTable", "-segmentName",
        "my_table", "-outDir", SEGMENT_DIR, "-overwrite"
    }).waitFor();
  }

  private void uploadData()
      throws InterruptedException {
    runAdministratorCommand(new String[]{"UploadSegment", "-controllerPort", "39000", "-segmentDir", SEGMENT_DIR})
        .waitFor();
  }

  private int countRecords() {
    Connection connection = ConnectionFactory.fromHostList("localhost:8099");
    return connection.execute("select count(*) from myTable").getResultSet(0).getInt(0);
  }

  @Test(enabled = false)
  public void testShortZookeeperFreeze()
      throws Exception {
    testFreezeZookeeper(10000L);
  }

  @Test(enabled = false)
  public void testLongZookeeperFreeze()
      throws Exception {
    testFreezeZookeeper(60000L);
  }

  public void testFreezeZookeeper(long freezeLength)
      throws Exception {
    Process zookeeper = startZookeeper();
    Thread.sleep(1000L);

    startController();
    Thread.sleep(3000L);

    startServer();
    startBroker();
    Thread.sleep(3000L);

    createTable();
    generateData();
    convertData();
    uploadData();
    Thread.sleep(5000L);

    long timeInTwoMinutes = System.currentTimeMillis() + 120000L;
    int currentRecordCount = countRecords();
    int expectedRecordCount = Integer.parseInt(TOTAL_RECORD_COUNT);
    while (currentRecordCount != expectedRecordCount && System.currentTimeMillis() < timeInTwoMinutes) {
      Thread.sleep(1000L);
      currentRecordCount = countRecords();
    }
    Assert.assertEquals(currentRecordCount, expectedRecordCount, "All segments did not load within 120 seconds");

    sendSignalToProcess(zookeeper, Signal.SIGSTOP);
    Thread.sleep(freezeLength);
    sendSignalToProcess(zookeeper, Signal.SIGCONT);
    Thread.sleep(5000L);

    timeInTwoMinutes = System.currentTimeMillis() + 120000L;
    currentRecordCount = countRecords();
    while (currentRecordCount != expectedRecordCount && System.currentTimeMillis() < timeInTwoMinutes) {
      Thread.sleep(1000L);
      currentRecordCount = countRecords();
    }
    Assert.assertEquals(currentRecordCount, expectedRecordCount,
        "Record count still inconsistent 120 seconds after zookeeper restart");
  }

  @AfterMethod
  public void tearDown() {
    for (Process process : _processes) {
      process.destroy();
    }
    FileUtils.deleteQuietly(new File(AVRO_DIR));
    FileUtils.deleteQuietly(new File(SEGMENT_DIR));
  }
}
