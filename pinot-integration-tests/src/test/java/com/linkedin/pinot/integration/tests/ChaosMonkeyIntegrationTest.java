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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.common.TestUtils;
import com.linkedin.pinot.tools.admin.PinotAdministrator;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jnr.constants.platform.Signal;
import jnr.posix.POSIXFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Monkeys and chaos.
 */
public class ChaosMonkeyIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChaosMonkeyIntegrationTest.class);
  private List<Process> _processes = new ArrayList<Process>();
  private String AVRO_DIR = "/tmp/temp-avro-" + getClass().getName();
  private String SEGMENT_DIR = "/tmp/temp-segment-" + getClass().getName();
  private static final String TOTAL_RECORD_COUNT = "1000";
  private static final String SEGMENT_COUNT = "25";

  private Process runAdministratorCommand(String[] args) {
    String classpath = System.getProperty("java.class.path");
    List<String> completeArgs = new ArrayList<String>();
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

  private static void sendSignalToProcess(Process process, Signal signal) {
    int processPid = getProcessPid(process);
    if (processPid != -1) {
      LOGGER.info("Sending signal {} to process {}", signal.intValue(), processPid);
      POSIXFactory.getNativePOSIX().kill(processPid, signal.intValue());
    }
  }

  private static int getProcessPid(Process process) {
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

  private void kill(Process process) {
    try {
      synchronized (_processes) {
        _processes.remove(process);
      }
      process.destroy();
      process.waitFor();
    } catch (InterruptedException e) {
      // Ignored
    }
  }

  private void sleep(long millis) {
    long wakeTime = System.currentTimeMillis() + millis;
    while (System.currentTimeMillis() < wakeTime) {
      long timeToSleepFor = wakeTime - System.currentTimeMillis();
      if (0 < timeToSleepFor) {
        try {
          Thread.sleep(timeToSleepFor);
        } catch (InterruptedException e) {
          // Ignored
        }
      }
    }
  }

  private Process startZookeeper() {
    return runAdministratorCommand(new String[]{
        "StartZookeeper", "-zkPort", "2191"
    });
  }

  private Process startController() {
    return runAdministratorCommand(new String[]{
        "StartController", "-zkAddress", "localhost:2191",
        "-controllerPort", "39000", "-dataDir", "/tmp/ChaosMonkeyClusterController"
    });
  }

  private Process startBroker() {
    return runAdministratorCommand(new String[]{
        "StartBroker", "-brokerPort", "8099", "-zkAddress", "localhost:2191"
    });
  }

  private Process startServer() {
    return runAdministratorCommand(new String[]{
        "StartServer", "-serverPort", "8098", "-zkAddress", "localhost:2191",
        "-dataDir", "/tmp/ChaosMonkeyCluster/data", "-segmentDir", "/tmp/ChaosMonkeyCluster/segments"
    });
  }

  private void generateData() throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema.json"));
    String schemaAnnotationsFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema-annotations.json"));
    runAdministratorCommand(new String[]{
        "GenerateData", "-numRecords", TOTAL_RECORD_COUNT, "-numFiles", SEGMENT_COUNT, "-schemaFile", schemaFile,
        "-schemaAnnotationFile", schemaAnnotationsFile, "-overwrite", "-outDir", AVRO_DIR
    }).waitFor();
  }

  private void createTable() throws InterruptedException {
    String createTableFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-create-table.json"));
    runAdministratorCommand(new String[]{
        "AddTable", "-controllerPort", "39000", "-filePath", createTableFile
    }).waitFor();
  }

  private void convertData() throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(ChaosMonkeyIntegrationTest.class.getClassLoader().
        getResource("chaos-monkey-schema.json"));
    runAdministratorCommand(new String[]{
        "CreateSegment", "-schemaFile", schemaFile, "-dataDir", AVRO_DIR, "-tableName", "myTable",
        "-segmentName", "my_table", "-outDir", SEGMENT_DIR, "-overwrite"
    }).waitFor();
  }

  private void uploadData() throws InterruptedException {
    runAdministratorCommand(new String[]{
        "UploadSegment", "-controllerPort", "39000", "-segmentDir", SEGMENT_DIR
    }).waitFor();
  }

  private int countRecords() {
    Connection connection = ConnectionFactory.fromHostList("localhost:8099");
    return connection.execute("select count(*) from myTable").getResultSet(0).getInt(0);
  }

  @Test
  public void testShortZookeeperFreeze() throws Exception {
    testFreezeZookeeper(10000L);
  }

  @Test(enabled = false)
  public void testLongZookeeperFreeze() throws Exception {
    testFreezeZookeeper(60000L);
  }

  public void testFreezeZookeeper(long freezeLength) throws Exception {
    System.out.println("System.getProperty(\"java.class.path\") = " + System.getProperty("java.class.path"));

    Process zookeeper = startZookeeper();
    sleep(1000L);

    Process controller = startController();
    sleep(1000L);

    Process server = startServer();
    sleep(1000L);

    Process broker = startBroker();
    sleep(1000L);

    createTable();
    generateData();
    convertData();
    uploadData();

    sleep(5000L);

    long timeInTwoMinutes = System.currentTimeMillis() + 120000L;
    int currentRecordCount = countRecords();
    int expectedRecordCount = Integer.parseInt(TOTAL_RECORD_COUNT);
    while (currentRecordCount != expectedRecordCount && System.currentTimeMillis() < timeInTwoMinutes) {
      sleep(1000L);
      currentRecordCount = countRecords();
    }
    Assert.assertEquals(currentRecordCount, expectedRecordCount, "All segments did not load within 120 seconds");

    sendSignalToProcess(zookeeper, Signal.SIGSTOP);
    sleep(freezeLength);
    sendSignalToProcess(zookeeper, Signal.SIGCONT);
    sleep(5000L);

    timeInTwoMinutes = System.currentTimeMillis() + 120000L;
    currentRecordCount = countRecords();
    while (currentRecordCount != expectedRecordCount && System.currentTimeMillis() < timeInTwoMinutes) {
      sleep(1000L);
      currentRecordCount = countRecords();
    }
    Assert.assertEquals(currentRecordCount, expectedRecordCount,
        "Record count still inconsistent 120 seconds after zookeeper restart");
  }

  @AfterMethod
  public void tearDown() {
    synchronized (_processes) {
      for (Process process : _processes) {
        process.destroy();
      }
      _processes.clear();
    }
    FileUtils.deleteQuietly(new File(AVRO_DIR));
    FileUtils.deleteQuietly(new File(SEGMENT_DIR));
  }
}
