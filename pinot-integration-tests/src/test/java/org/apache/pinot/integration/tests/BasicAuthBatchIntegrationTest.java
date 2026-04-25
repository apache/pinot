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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.BootstrapTableTool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER_USER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_TOKEN;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class BasicAuthBatchIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String BOOTSTRAP_DATA_DIR = "/examples/batch/baseballStats";
  private static final String TABLE_NAME = "baseballStats";
  private static final String SCHEMA_FILE = "baseballStats_schema.json";
  private static final String CONFIG_FILE = "baseballStats_offline_table_config.json";
  private static final String DATA_FILE = "baseballStats_data.csv";
  private static final String JOB_FILE = "ingestionJobSpec.yaml";

  private File _quickstartTmpDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start Zookeeper
    startZk();
    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();
    startMinion();

    cleanUpTableAndSchema();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanUpTableAndSchema);
    exception = runCleanup(exception, this::cleanUpQuickstartTmpDir);
    exception = runCleanup(exception, this::stopMinionIfStarted);
    exception = runCleanup(exception, this::stopServerIfStarted);
    exception = runCleanup(exception, this::stopBrokerIfStarted);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
  }

  @Override
  protected void overrideMinionConf(PinotConfiguration minionConf) {
    BasicAuthTestUtils.addMinionConfiguration(minionConf);
  }

  @Override
  protected Map<String, String> getControllerRequestClientHeaders() {
    return AUTH_HEADER;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return false;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return true;
  }

  @Test
  public void testBrokerNoAuth() {
    try {
      sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql", "{\"sql\":\"SELECT now()\"}");
    } catch (IOException e) {
      HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(httpErrorStatusException.getStatusCode(), 401, "must return 401");
    }
  }

  @Test
  public void testBroker()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql", "{\"sql\":\"SELECT now()\"}",
            AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");
  }

  @Test
  public void testControllerGetTables()
      throws Exception {
    JsonNode response =
        JsonUtils.stringToJsonNode(sendGetRequest("http://localhost:" + getControllerPort() + "/tables", AUTH_HEADER));
    Assert.assertTrue(response.get("tables").isArray(), "must return table array");
  }

  @Test
  public void testControllerGetTablesNoAuth() {
    try {
      // NOTE: the endpoint is protected implicitly (without annotation) by BasicAuthAccessControlFactory
      sendGetRequest("http://localhost:" + getControllerPort() + "/tables");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("401"));
    }
  }

  @Test
  public void testIngestionBatch()
      throws Exception {
    cleanUpQuickstartTmpDir();
    _quickstartTmpDir =
        new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(_quickstartTmpDir);

    File baseDir = new File(_quickstartTmpDir, TABLE_NAME);
    File dataDir = new File(baseDir, "rawdata");
    File schemaFile = new File(baseDir, SCHEMA_FILE);
    File configFile = new File(baseDir, CONFIG_FILE);
    File dataFile = new File(dataDir, DATA_FILE);
    File jobFile = new File(baseDir, JOB_FILE);
    try {
      Preconditions.checkState(dataDir.mkdirs());

      FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + SCHEMA_FILE), schemaFile);
      FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + CONFIG_FILE), configFile);
      FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/rawdata/" + DATA_FILE), dataFile);
      FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + JOB_FILE), jobFile);

      // patch ingestion job file
      String jobFileContents;
      try (FileInputStream inputStream = new FileInputStream(jobFile)) {
        jobFileContents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }
      try (FileOutputStream outputStream = new FileOutputStream(jobFile)) {
        IOUtils.write(jobFileContents.replaceAll("9000", String.valueOf(getControllerPort())), outputStream,
            StandardCharsets.UTF_8);
      }

      new BootstrapTableTool("http", "localhost", getControllerPort(), baseDir.getAbsolutePath(),
          AuthProviderUtils.makeAuthProvider(AUTH_TOKEN)).execute();

      Thread.sleep(5000);

      // admin with full access
      JsonNode response = JsonUtils.stringToJsonNode(
          sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql",
              "{\"sql\":\"SELECT count(*) FROM baseballStats\"}", AUTH_HEADER));
      Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
          "must return result with LONG value");
      Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "count(*)",
          "must return column name 'count(*)");
      Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 97889,
          "must return row count 97889");
      Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");

      // user with valid auth but no table access - must return 403
      try {
        sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql",
            "{\"sql\":\"SELECT count(*) FROM baseballStats\"}", AUTH_HEADER_USER);
      } catch (IOException e) {
        HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e.getCause();
        Assert.assertEquals(httpErrorStatusException.getStatusCode(), 403, "must return 403");
      }
    } finally {
      cleanUpQuickstartTmpDir();
    }
  }

  private void cleanUpTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        TABLE_NAME)) {
      dropOfflineTable(TABLE_NAME);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(TABLE_NAME) != null) {
      deleteSchema(TABLE_NAME);
    }
  }

  private void cleanUpQuickstartTmpDir()
      throws IOException {
    if (_quickstartTmpDir != null) {
      FileUtils.deleteDirectory(_quickstartTmpDir);
      _quickstartTmpDir = null;
    }
  }

  private void stopMinionIfStarted() {
    if (_minionStarter != null) {
      stopMinion();
    }
  }

  private void stopServerIfStarted() {
    if (!_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfStarted() {
    if (!_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
