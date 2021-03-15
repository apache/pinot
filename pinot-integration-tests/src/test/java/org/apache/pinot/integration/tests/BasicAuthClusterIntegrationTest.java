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
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.BootstrapTableTool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class BasicAuthClusterIntegrationTest extends ClusterTest {
  private static final String BOOTSTRAP_DATA_DIR = "/examples/batch/baseballStats";
  private static final String SCHEMA_FILE = "baseballStats_schema.json";
  private static final String CONFIG_FILE = "baseballStats_offline_table_config.json";
  private static final String DATA_FILE = "baseballStats_data.csv";
  private static final String JOB_FILE = "ingestionJobSpec.yaml";

  private static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=====";
  private static final String AUTH_TOKEN_USER = "Basic dXNlcjpzZWNyZXQ==";

  private static final Map<String, String> AUTH_HEADER = Collections.singletonMap("Authorization", AUTH_TOKEN);
  private static final Map<String, String> AUTH_HEADER_USER =
      Collections.singletonMap("Authorization", AUTH_TOKEN_USER);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion(Collections.emptyList(), Collections.emptyList());
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
    Map<String, Object> properties = super.getDefaultControllerConfiguration();
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.tables", "userTableOnly");
    properties.put("controller.admin.access.control.principals.user.permissions", "read");
    return properties;
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    Map<String, Object> properties = super.getDefaultBrokerConfiguration().toMap();
    properties.put("pinot.broker.access.control.class", "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    properties.put("pinot.broker.access.control.principals", "admin, user");
    properties.put("pinot.broker.access.control.principals.admin.password", "verysecret");
    properties.put("pinot.broker.access.control.principals.user.password", "secret");
    properties.put("pinot.broker.access.control.principals.user.tables", "userTableOnly");
    properties.put("pinot.broker.access.control.principals.user.permissions", "read");
    return new PinotConfiguration(properties);
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    Map<String, Object> properties = super.getDefaultServerConfiguration().toMap();
    properties.put("pinot.server.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("pinot.server.segment.upload.auth.token", AUTH_TOKEN);
    properties.put("pinot.server.instance.auth.token", AUTH_TOKEN);
    return new PinotConfiguration(properties);
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    Map<String, Object> properties = super.getDefaultMinionConfiguration().toMap();
    properties.put("segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("task.auth.token", AUTH_TOKEN);
    return new PinotConfiguration(properties);
  }

  @Test
  public void testBrokerNoAuth()
      throws Exception {
    JsonNode response =
        JsonUtils.stringToJsonNode(sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT now()\"}"));
    Assert.assertFalse(response.has("resultTable"), "must not return result table");
    Assert.assertTrue(response.get("exceptions").get(0).get("errorCode").asInt() != 0, "must return error code");
  }

  @Test
  public void testBroker()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT now()\"}", AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");
  }

  @Test
  public void testControllerGetTables()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest("http://localhost:18998/tables", AUTH_HEADER));
    Assert.assertTrue(response.get("tables").isArray(), "must return table array");
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

    // patch ingestion job
    String jobFileContents = IOUtils.toString(new FileInputStream(jobFile));
    IOUtils.write(jobFileContents.replaceAll("9000", String.valueOf(DEFAULT_CONTROLLER_PORT)),
        new FileOutputStream(jobFile));

    new BootstrapTableTool("http", "localhost", DEFAULT_CONTROLLER_PORT, baseDir.getAbsolutePath(), AUTH_TOKEN)
        .execute();

    Thread.sleep(5000);

    // admin with full access
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT count(*) FROM baseballStats\"}",
            AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "count(*)",
        "must return column name 'count(*)");
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 97889,
        "must return row count 97889");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");

    // user with valid auth but no table access
    JsonNode responseUser = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT count(*) FROM baseballStats\"}",
            AUTH_HEADER_USER));
    Assert.assertFalse(responseUser.has("resultTable"), "must not return result table");
    Assert.assertTrue(responseUser.get("exceptions").get(0).get("errorCode").asInt() != 0, "must return error code");
  }

//  // TODO this endpoint should be protected once UI supports auth
//  @Test
//  public void testControllerGetTablesNoAuth()
//      throws Exception {
//    System.out.println(sendGetRequest("http://localhost:18998/tables"));
//  }
}
