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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jnr.constants.platform.Signal;
import jnr.posix.POSIXFactory;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.Request;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Monkeys and chaos.
 */
public abstract class PinotAdministratorIntegrationTestBase {
  static final Logger LOGGER = LoggerFactory.getLogger(PinotAdministratorIntegrationTestBase.class);
  static final String TOTAL_RECORD_COUNT = "1000";
  static final String SEGMENT_COUNT = "25";
  static final String AVRO_DIR = "/tmp/temp-avro-" + PinotAdministratorIntegrationTestBase.class.getName();
  static final String SEGMENT_DIR = "/tmp/temp-segment-" + PinotAdministratorIntegrationTestBase.class.getName();

  final List<Process> _processes = new ArrayList<>();

  Process runAdministratorCommand(String[] args) {
    String classpath = System.getProperty("java.class.path");
    System.setProperty("pinot.admin.system.exit", "true");
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

  void sendSignalToProcess(Process process, Signal signal) {
    int processPid = getProcessPid(process);
    if (processPid != -1) {
      LOGGER.info("Sending signal {} to process {}", signal.intValue(), processPid);
      POSIXFactory.getNativePOSIX().kill(processPid, signal.intValue());
    }
  }

  int getProcessPid(Process process) {
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

  Process startZookeeper() {
    return runAdministratorCommand(new String[]{"StartZookeeper", "-zkPort", "2191"});
  }

  Process startController() {
    return runAdministratorCommand(new String[]{
        "StartController", "-zkAddress", "localhost:2191", "-controllerPort", "39000", "-dataDir",
        "/tmp/ChaosMonkeyClusterController"
    });
  }

  Process startBroker() {
    return runAdministratorCommand(new String[]{"StartBroker", "-brokerPort", "8099", "-zkAddress", "localhost:2191"});
  }

  Process startServer() {
    return runAdministratorCommand(new String[]{
        "StartServer", "-serverPort", "8098", "-zkAddress", "localhost:2191", "-dataDir",
        "/tmp/ChaosMonkeyCluster/data", "-segmentDir", "/tmp/ChaosMonkeyCluster/segments"
    });
  }

  void generateData()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader().getResource("chaos-monkey-schema.json")));
    String schemaAnnotationsFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader()
            .getResource("chaos-monkey-schema-annotations.json")));
    int exitCode = runAdministratorCommand(new String[]{
        "GenerateData", "-numRecords", TOTAL_RECORD_COUNT, "-numFiles", SEGMENT_COUNT, "-schemaFile", schemaFile,
        "-schemaAnnotationFile", schemaAnnotationsFile, "-overwrite", "-outDir", AVRO_DIR
    }).waitFor();
    Assert.assertEquals(exitCode, 0);
  }

  void createTable()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader().getResource("chaos-monkey-schema.json")));
    String createTableFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader().getResource("chaos-monkey-create-table.json")));
    int exitCode = runAdministratorCommand(new String[]{
        "AddTable", "-controllerPort", "39000", "-schemaFile", schemaFile, "-tableConfigFile", createTableFile, "-exec"
    }).waitFor();
    Assert.assertEquals(exitCode, 0);
  }

  void convertData()
      throws InterruptedException {
    String schemaFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader().getResource("chaos-monkey-schema.json")));
    String createTableFile = TestUtils.getFileFromResourceUrl(Objects.requireNonNull(
        PinotAdministratorIntegrationTestBase.class.getClassLoader().getResource("chaos-monkey-create-table.json")));
    int exitCode = runAdministratorCommand(new String[]{
        "CreateSegment", "-schemaFile", schemaFile, "-dataDir", AVRO_DIR, "-tableConfigFile", createTableFile,
        "-outDir", SEGMENT_DIR, "-format", "AVRO", "-overwrite"
    }).waitFor();
    Assert.assertEquals(exitCode, 0);
  }

  void uploadData()
      throws InterruptedException {
    int exitCode = runAdministratorCommand(new String[]{
        "UploadSegment", "-controllerPort", "39000", "-segmentDir", SEGMENT_DIR
    }).waitFor();
    Assert.assertEquals(exitCode, 0);
  }

  int countRecords() {
    Connection connection = ConnectionFactory.fromHostList("localhost:8099");
    try {
      return connection.execute(new Request("sql", "select count(*) from myTable")).getResultSet(0).getInt(0);
    } catch (Exception e) {
      LOGGER.error("exception occurred during count record!", e);
      return 0;
    }
  }
}
