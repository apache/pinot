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
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
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
public class BasicAuthBatchIntegrationTest extends ClusterTest {
  private static final String BOOTSTRAP_DATA_DIR = "/examples/batch/baseballStats";
  private static final String SCHEMA_FILE = "baseballStats_schema.json";
  private static final String CONFIG_FILE = "baseballStats_offline_table_config.json";
  private static final String DATA_FILE = "baseballStats_data.csv";
  private static final String JOB_FILE = "ingestionJobSpec.yaml";

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
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    return BasicAuthTestUtils.addControllerConfiguration(super.getDefaultControllerConfiguration());
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    return BasicAuthTestUtils.addBrokerConfiguration(super.getDefaultBrokerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    return BasicAuthTestUtils.addServerConfiguration(super.getDefaultServerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    return BasicAuthTestUtils.addMinionConfiguration(super.getDefaultMinionConfiguration().toMap());
  }

  @Test
  public void testBrokerNoAuth()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql", "{\"sql\":\"SELECT now()\"}"));
    Assert.assertFalse(response.has("resultTable"), "must not return result table");
    Assert.assertTrue(response.get("exceptions").get(0).get("errorCode").asInt() != 0, "must return error code");
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
  public void testControllerGetTablesNoAuth()
      throws Exception {
    try {
      // NOTE: the endpoint is protected implicitly (without annotation) by BasicAuthAccessControlFactory
      sendGetRequest("http://localhost:" + getControllerPort() + "/tables");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("403"));
    }
  }

  @Test
  public void testIngestionBatch()
      throws Exception {
    File quickstartTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    FileUtils.forceDeleteOnExit(quickstartTmpDir);

    File baseDir = new File(quickstartTmpDir, "baseballStats");
    File dataDir = new File(baseDir, "rawdata");
    File schemaFile = new File(baseDir, SCHEMA_FILE);
    File configFile = new File(baseDir, CONFIG_FILE);
    File dataFile = new File(dataDir, DATA_FILE);
    File jobFile = new File(baseDir, JOB_FILE);
    Preconditions.checkState(dataDir.mkdirs());

    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + SCHEMA_FILE), schemaFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + CONFIG_FILE), configFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/rawdata/" + DATA_FILE), dataFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + JOB_FILE), jobFile);

    // patch ingestion job file
    String jobFileContents = IOUtils.toString(new FileInputStream(jobFile));
    IOUtils
        .write(jobFileContents.replaceAll("9000", String.valueOf(getControllerPort())), new FileOutputStream(jobFile));

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
      Assert.assertTrue(httpErrorStatusException.getStatusCode() == 403, "must return 403");
    }
  }
}
