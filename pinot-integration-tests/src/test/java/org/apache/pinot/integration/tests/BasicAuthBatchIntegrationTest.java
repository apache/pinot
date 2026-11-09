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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.server.access.BasicAuthAccessFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.dml.DeleteStatement;
import org.apache.pinot.tools.BootstrapTableTool;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER_USER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_TOKEN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Integration test that provides example of
/// [org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator] and
/// [org.apache.pinot.minion.executor.PinotTaskExecutor] and tests simple minion functionality.
///
/// The controller and the broker are started with a [RecordingSqlQueryExecutor], plugged in through their
/// `createSqlQueryExecutor()` hook, so that an authorized `DELETE` can be followed from the query endpoint to the
/// executor of the node.
public class BasicAuthBatchIntegrationTest extends ClusterTest {
  private static final String BOOTSTRAP_DATA_DIR = "/examples/batch/baseballStats";
  private static final String SCHEMA_FILE = "baseballStats_schema.json";
  private static final String CONFIG_FILE = "baseballStats_offline_table_config.json";
  private static final String DATA_FILE = "baseballStats_data.csv";
  private static final String JOB_FILE = "ingestionJobSpec.yaml";
  // Broker principal granted the DELETE permission, which the broker requires to delete rows
  private static final Map<String, String> AUTH_HEADER_DELETER =
      Map.of("Authorization", "Basic ZGVsZXRlcjpkZWxzZWNyZXQ="); // deleter:delsecret
  private static final String DELETE_TABLE = "baseballStats";
  private static final String DELETE_PREDICATE = "playerID = 'unknown'";

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
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
    properties.put("controller.server.admin.auth.token", AUTH_TOKEN);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);
    brokerConf.setProperty("pinot.broker.access.control.principals", "admin, user, deleter");
    brokerConf.setProperty("pinot.broker.access.control.principals.deleter.password", "delsecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.deleter.permissions", "read, delete");
    brokerConf.setProperty("pinot.broker.server.admin.auth.token", AUTH_TOKEN);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
    serverConf.setProperty("pinot.server.admin.access.control.factory.class", BasicAuthAccessFactory.class.getName());
    serverConf.setProperty("pinot.server.admin.access.control.principals", "admin,user");
    serverConf.setProperty("pinot.server.admin.access.control.principals.admin.password", "verysecret");
    serverConf.setProperty("pinot.server.admin.access.control.principals.admin.permissions", "admin");
    serverConf.setProperty("pinot.server.admin.access.control.principals.user.password", "secret");
    serverConf.setProperty("pinot.server.admin.access.control.principals.user.tables", "userTableOnly");
    serverConf.setProperty("pinot.server.admin.access.control.principals.user.permissions", "read");
  }

  @Override
  protected void overrideMinionConf(PinotConfiguration minionConf) {
    BasicAuthTestUtils.addMinionConfiguration(minionConf);
  }

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new RecordingControllerStarter();
  }

  @Override
  protected BaseBrokerStarter createBrokerStarter() {
    return new RecordingBrokerStarter();
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

  /// An authorized `DELETE` reaches the executor the node was started with, through the `createSqlQueryExecutor()`
  /// hook of its starter, once its table is resolved. Runs after [#testIngestionBatch] created the table.
  @Test(dependsOnMethods = "testIngestionBatch")
  public void testDeleteRequiresTheDeletePermission()
      throws Exception {
    // user may only query userTableOnly, with the read permission; admin has every table, and on the controller every
    // permission
    String request = "{\"sql\":\"DELETE FROM " + DELETE_TABLE + " WHERE " + DELETE_PREDICATE + "\"}";
    String userTableRequest = "{\"sql\":\"DELETE FROM userTableOnly WHERE " + DELETE_PREDICATE + "\"}";
    String unknownTableRequest = "{\"sql\":\"DELETE FROM unknownTable WHERE " + DELETE_PREDICATE + "\"}";
    RecordingSqlQueryExecutor brokerExecutor = ((RecordingBrokerStarter) _brokerStarters.get(0)).getSqlQueryExecutor();
    RecordingSqlQueryExecutor controllerExecutor =
        ((RecordingControllerStarter) _controllerStarter).getSqlQueryExecutor();
    waitForTableCache(((RecordingBrokerStarter) _brokerStarters.get(0)).getTableCache(), "broker");
    waitForTableCache(_controllerStarter.getHelixResourceManager().getTableCache(), "controller");

    // The broker requires the checks of a query on the table, and the DELETE permission granted explicitly
    String brokerUrl = "http://localhost:" + getRandomBrokerPort() + "/query/sql";
    assertForbidden(brokerUrl, request, AUTH_HEADER_USER);
    assertForbidden(brokerUrl, userTableRequest, AUTH_HEADER_USER);
    assertForbidden(brokerUrl, request, AUTH_HEADER);
    assertTrue(brokerExecutor.getStatements().isEmpty(), "a denied DELETE must not reach the executor");
    // An authorized DELETE on an unknown table fails before it reaches the executor
    assertTableDoesNotExist(sendPostRequest(brokerUrl, unknownTableRequest, AUTH_HEADER_DELETER));
    assertTrue(brokerExecutor.getStatements().isEmpty(), "a DELETE on an unknown table must not reach the executor");
    // An authorized DELETE reaches the executor the broker was started with, which deletes the rows
    assertDeleted(sendPostRequest(brokerUrl, request, AUTH_HEADER_DELETER));
    assertRecorded(brokerExecutor, AUTH_HEADER_DELETER);

    // The controller requires the READ and DELETE access types on the table
    String controllerUrl = "http://localhost:" + getControllerPort() + "/sql";
    assertAccessDenied(sendPostRequest(controllerUrl, request, AUTH_HEADER_USER));
    assertAccessDenied(sendPostRequest(controllerUrl, userTableRequest, AUTH_HEADER_USER));
    assertTrue(controllerExecutor.getStatements().isEmpty(), "a denied DELETE must not reach the executor");
    assertTableDoesNotExist(sendPostRequest(controllerUrl, unknownTableRequest, AUTH_HEADER));
    assertTrue(controllerExecutor.getStatements().isEmpty(),
        "a DELETE on an unknown table must not reach the executor");
    assertDeleted(sendPostRequest(controllerUrl, request, AUTH_HEADER));
    assertRecorded(controllerExecutor, AUTH_HEADER);
  }

  private static void waitForTableCache(TableCache tableCache, String role) {
    TestUtils.waitForCondition(aVoid -> tableCache.getActualTableName(DELETE_TABLE) != null, 30_000L,
        "the table cache of the " + role + " did not learn the table " + DELETE_TABLE);
  }

  private static void assertForbidden(String url, String request, Map<String, String> headers) {
    IOException e = expectThrows(IOException.class, () -> sendPostRequest(url, request, headers));
    assertEquals(((HttpErrorStatusException) e.getCause()).getStatusCode(), 403, e.getMessage());
  }

  private static void assertAccessDenied(String response)
      throws IOException {
    assertException(response, QueryErrorCode.ACCESS_DENIED);
  }

  private static void assertTableDoesNotExist(String response)
      throws IOException {
    JsonNode exception = assertException(response, QueryErrorCode.TABLE_DOES_NOT_EXIST);
    assertTrue(exception.get("message").asText().contains("Table does not exist: unknownTable"), response);
  }

  private static JsonNode assertException(String response, QueryErrorCode errorCode)
      throws IOException {
    JsonNode exception = JsonUtils.stringToJsonNode(response).get("exceptions").get(0);
    assertEquals(exception.get("errorCode").asInt(), errorCode.getId(), response);
    return exception;
  }

  /// The response carries the result table of [RecordingSqlQueryExecutor#executeDelete]
  private static void assertDeleted(String response)
      throws IOException {
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    assertTrue(responseJson.get("exceptions").isEmpty(), response);
    JsonNode resultTable = responseJson.get("resultTable");
    assertEquals(resultTable.get("dataSchema").get("columnNames").get(0).asText(), "table", response);
    assertEquals(resultTable.get("dataSchema").get("columnNames").get(1).asText(), "predicate", response);
    assertEquals(resultTable.get("rows").size(), 1, response);
    assertEquals(resultTable.get("rows").get(0).get(0).asText(), DELETE_TABLE, response);
    assertEquals(resultTable.get("rows").get(0).get(1).asText(), DELETE_PREDICATE, response);
  }

  /// The executor was handed the resolved statement once, with the headers of the request
  private static void assertRecorded(RecordingSqlQueryExecutor executor, Map<String, String> authHeader) {
    assertEquals(executor.getStatements().size(), 1, "the DELETE must reach the executor exactly once");
    DeleteStatement statement = executor.getStatements().get(0);
    assertTrue(statement.isResolved(), "the executor is handed a resolved statement");
    assertTrue(statement.tableExists(), "the executor is handed an existing table");
    assertEquals(statement.getTableName(), DELETE_TABLE);
    assertEquals(statement.getPredicate(), DELETE_PREDICATE);
    Map<String, String> headers = executor.getHeaders().get(0);
    String authorization = null;
    for (Map.Entry<String, String> header : headers.entrySet()) {
      if (header.getKey().equalsIgnoreCase("Authorization")) {
        authorization = header.getValue();
      }
    }
    assertEquals(authorization, authHeader.get("Authorization"), "the executor is handed the headers: " + headers);
  }

  /// Controller started with a [RecordingSqlQueryExecutor], through the `createSqlQueryExecutor()` hook
  private static class RecordingControllerStarter extends ControllerStarter {
    private RecordingSqlQueryExecutor _recordingSqlQueryExecutor;

    @Override
    protected SqlQueryExecutor createSqlQueryExecutor() {
      // Constructed as the default executor is
      _recordingSqlQueryExecutor = new RecordingSqlQueryExecutor(_config.generateVipUrl());
      return _recordingSqlQueryExecutor;
    }

    RecordingSqlQueryExecutor getSqlQueryExecutor() {
      return _recordingSqlQueryExecutor;
    }
  }

  /// Broker started with a [RecordingSqlQueryExecutor], through the `createSqlQueryExecutor()` hook
  private static class RecordingBrokerStarter extends HelixBrokerStarter {
    private RecordingSqlQueryExecutor _recordingSqlQueryExecutor;

    @Override
    protected SqlQueryExecutor createSqlQueryExecutor() {
      // Constructed as the default executor is
      String controllerUrl = _brokerConf.getProperty(Broker.CONTROLLER_URL);
      _recordingSqlQueryExecutor = controllerUrl != null ? new RecordingSqlQueryExecutor(controllerUrl)
          : new RecordingSqlQueryExecutor(_spectatorHelixManager);
      return _recordingSqlQueryExecutor;
    }

    RecordingSqlQueryExecutor getSqlQueryExecutor() {
      return _recordingSqlQueryExecutor;
    }

    TableCache getTableCache() {
      return _tableCache;
    }
  }

  /// Executor that records the `DELETE` statements it is handed, with the headers of their request, and answers each
  /// with a one-row result table of its table and predicate, as an executor that implements row deletion would
  private static class RecordingSqlQueryExecutor extends SqlQueryExecutor {
    private final List<DeleteStatement> _statements = new CopyOnWriteArrayList<>();
    private final List<Map<String, String>> _headers = new CopyOnWriteArrayList<>();

    RecordingSqlQueryExecutor(String controllerUrl) {
      super(controllerUrl);
    }

    RecordingSqlQueryExecutor(HelixManager helixManager) {
      super(helixManager);
    }

    @Override
    protected BrokerResponse executeDelete(DeleteStatement statement, @Nullable Map<String, String> headers) {
      _statements.add(statement);
      _headers.add(headers != null ? headers : Map.of());
      DataSchema dataSchema = new DataSchema(new String[]{"table", "predicate"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
      List<Object[]> rows = List.<Object[]>of(new Object[]{statement.getTableName(), statement.getPredicate()});
      BrokerResponseNative response = new BrokerResponseNative();
      response.setResultTable(new ResultTable(dataSchema, rows));
      return response;
    }

    List<DeleteStatement> getStatements() {
      return _statements;
    }

    List<Map<String, String>> getHeaders() {
      return _headers;
    }
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
    String jobFileContents = IOUtils.toString(new FileInputStream(jobFile), StandardCharsets.UTF_8);
    IOUtils.write(jobFileContents.replaceAll("9000", String.valueOf(getControllerPort())),
        new FileOutputStream(jobFile), StandardCharsets.UTF_8);

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

    // The Controller fans this request out to the protected Server admin API with its configured service identity.
    JsonNode tableSizeResponse = JsonUtils.stringToJsonNode(
        sendGetRequest("http://localhost:" + getControllerPort() + "/tables/baseballStats/size", AUTH_HEADER));
    Assert.assertTrue(tableSizeResponse.get("reportedSizeInBytes").asLong() > 0,
        "must return the size reported by the Server");

    // user with valid auth but no table access - must return 403
    try {
      sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql",
          "{\"sql\":\"SELECT count(*) FROM baseballStats\"}", AUTH_HEADER_USER);
    } catch (IOException e) {
      HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(httpErrorStatusException.getStatusCode(), 403, "must return 403");
    }
  }
}
