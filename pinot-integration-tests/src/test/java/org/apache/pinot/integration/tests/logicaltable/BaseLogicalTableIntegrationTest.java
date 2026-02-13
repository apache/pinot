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
package org.apache.pinot.integration.tests.logicaltable;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTestSet;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.QueryAssert;
import org.apache.pinot.integration.tests.QueryGenerator;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public abstract class BaseLogicalTableIntegrationTest extends BaseClusterIntegrationTestSet {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseLogicalTableIntegrationTest.class);
  private static final String DEFAULT_TENANT = "DefaultTenant";
  private static final String DEFAULT_LOGICAL_TABLE_NAME = "mytable";
  protected static final String DEFAULT_TABLE_NAME = "physicalTable";
  protected static final String EMPTY_OFFLINE_TABLE_NAME = "empty_o";
  protected static BaseLogicalTableIntegrationTest _sharedClusterTestSuite = null;
  protected List<File> _avroFiles;

  @BeforeSuite
  public void setUpSuite()
      throws Exception {
    LOGGER.info("Setting up integration test suite");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _sharedClusterTestSuite = this;

    // Start the Pinot cluster
    startZk();
    LOGGER.info("Start Kafka in the integration test suite");
    startKafka();
    startController();
    startBroker();
    startServers(2);
    LOGGER.info("Finished setting up integration test suite");
  }

  @AfterSuite
  public void tearDownSuite()
      throws Exception {
    LOGGER.info("Tearing down integration test suite");
    // Stop Kafka
    LOGGER.info("Stop Kafka in the integration test suite");
    stopKafka();
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.info("Finished tearing down integration test suite");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    if (_sharedClusterTestSuite != this) {
      _controllerRequestURLBuilder = _sharedClusterTestSuite._controllerRequestURLBuilder;
      _helixResourceManager = _sharedClusterTestSuite._helixResourceManager;
      _kafkaStarters = _sharedClusterTestSuite._kafkaStarters;
      _controllerBaseApiUrl = _sharedClusterTestSuite._controllerBaseApiUrl;
    }

    _avroFiles = getAllAvroFiles();
    uploadDataToOfflineTables(getOfflineTableNames(), _avroFiles);

    // create realtime table
    Map<String, List<File>> realtimeTableDataFiles = getRealtimeTableDataFiles();
    for (Map.Entry<String, List<File>> entry : realtimeTableDataFiles.entrySet()) {
      String tableName = entry.getKey();
      List<File> avroFilesForTable = entry.getValue();
      // create and upload the schema and table config
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(tableName);
      addSchema(schema);

      TableConfig realtimeTableConfig = createRealtimeTableConfig(avroFilesForTable.get(0));
      realtimeTableConfig.setTableName(tableName);
      addTableConfig(realtimeTableConfig);

      // push avro files into kafka
      pushAvroIntoKafka(avroFilesForTable);
    }

    createLogicalTableSchema();
    createLogicalTable();

    // Set up the H2 connection
    setUpH2Connection(_avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(_avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    createLogicalTableWithEmptyOfflineTable();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    cleanup();
  }

  protected List<String> getOfflineTableNames() {
    return List.of();
  }

  protected List<String> getRealtimeTableNames() {
    return List.of();
  }

  protected Map<String, List<File>> getRealtimeTableDataFiles() {
    List<String> realtimeTableNames = getRealtimeTableNames();
    return !realtimeTableNames.isEmpty() ? distributeFilesToTables(realtimeTableNames, _avroFiles) : Map.of();
  }

  protected List<String> getPhysicalTableNames() {
    List<String> offlineTableNames = getOfflineTableNames().stream().map(TableNameBuilder.OFFLINE::tableNameWithType)
        .collect(Collectors.toList());
    List<String> realtimeTableNames = getRealtimeTableNames().stream()
        .map(TableNameBuilder.REALTIME::tableNameWithType).collect(Collectors.toList());
    return Stream.concat(offlineTableNames.stream(), realtimeTableNames.stream()).collect(Collectors.toList());
  }

  protected String getLogicalTableName() {
    return DEFAULT_LOGICAL_TABLE_NAME;
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }

  protected String getBrokerTenant() {
    return DEFAULT_TENANT;
  }

  // Setup H2 table with the same name as the logical table.
  protected void setUpH2Connection(List<File> avroFiles)
      throws Exception {
    setUpH2Connection();
    ClusterIntegrationTestUtils.setUpH2TableWithAvro(avroFiles, getLogicalTableName(), _h2Connection);
  }

  private void createLogicalTableSchema()
      throws IOException {
    Schema logicalTableSchema = createSchema(getSchemaFileName());
    logicalTableSchema.setSchemaName(getLogicalTableName());
    addSchema(logicalTableSchema);
  }

  protected void createLogicalTable()
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    List<String> offlineTableNames = getOfflineTableNames().stream()
        .map(TableNameBuilder.OFFLINE::tableNameWithType).collect(Collectors.toList());
    List<String> realtimeTableNames = getRealtimeTableNames().stream()
        .map(TableNameBuilder.REALTIME::tableNameWithType).collect(Collectors.toList());
    LogicalTableConfig logicalTable = createLogicalTableConfig(offlineTableNames, realtimeTableNames);
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + getLogicalTableName()
        + " logical table successfully added.\"}");
  }

  protected LogicalTableConfig getLogicalTableConfig(String logicalTableName)
      throws IOException {
    String getLogicalTableUrl =
        _controllerRequestURLBuilder.forLogicalTableGet(logicalTableName);
    String resp = ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders());
    return LogicalTableConfig.fromString(resp);
  }

  private void createLogicalTableWithEmptyOfflineTable()
      throws IOException {
    Schema schema = createSchema(getSchemaFileName());
    schema.setSchemaName(TableNameBuilder.extractRawTableName(EMPTY_OFFLINE_TABLE_NAME));
    addSchema(schema);

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    TableConfig offlineTableConfig = createOfflineTableConfig(EMPTY_OFFLINE_TABLE_NAME);
    addTableConfig(offlineTableConfig);
    physicalTableConfigMap.put(TableNameBuilder.OFFLINE.tableNameWithType(EMPTY_OFFLINE_TABLE_NAME),
        new PhysicalTableConfig());
    String refOfflineTableName = TableNameBuilder.OFFLINE.tableNameWithType(EMPTY_OFFLINE_TABLE_NAME);

    String logicalTableName = EMPTY_OFFLINE_TABLE_NAME + "_logical";

    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    Schema logicalTableSchema = createSchema(getSchemaFileName());
    logicalTableSchema.setSchemaName(logicalTableName);
    addSchema(logicalTableSchema);
    LogicalTableConfigBuilder builder =
        new LogicalTableConfigBuilder().setTableName(logicalTableName)
            .setBrokerTenant(DEFAULT_TENANT)
            .setRefOfflineTableName(refOfflineTableName)
            .setPhysicalTableConfigMap(physicalTableConfigMap);

    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, builder.build().toSingleLineJsonString(), getHeaders());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName
        + " logical table successfully added.\"}");
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles,
        "localhost:" + _sharedClusterTestSuite._kafkaStarters.get(0).getPort(), getKafkaTopic(),
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), injectTombstones());
  }

  @Override
  public String getZkUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getZkUrl();
    }
    return super.getZkUrl();
  }

  @Override
  public ControllerRequestClient getControllerRequestClient() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerRequestClient();
    }
    return super.getControllerRequestClient();
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerBaseApiUrl();
    }
    return super.getBrokerBaseApiUrl();
  }

  @Override
  protected String getBrokerGrpcEndpoint() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerGrpcEndpoint();
    }
    return super.getBrokerGrpcEndpoint();
  }

  @Override
  public int getControllerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerPort();
    }
    return super.getControllerPort();
  }

  @Override
  public int getRandomBrokerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getRandomBrokerPort();
    }
    return super.getRandomBrokerPort();
  }

  @Override
  public String getHelixClusterName() {
    return "BaseLogicalTableIntegrationTest";
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForDocsLoaded(timeoutMs, true, getLogicalTableName());
  }

  @Override
  protected void setUpQueryGenerator(List<File> avroFiles) {
    Assert.assertNull(_queryGenerator);
    String tableName = getLogicalTableName();
    _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
  }

  @Test
  public void verifyLogicalTableConfig()
      throws IOException {
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    assertEquals(logicalTableConfig.getPhysicalTableConfigMap().size(), getPhysicalTableNames().size());
    assertEquals(new HashSet<>(getPhysicalTableNames()), logicalTableConfig.getPhysicalTableConfigMap().keySet());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardcodedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testHardcodedQueries();
  }

  public void testQueriesFromQueryFile()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    super.testQueriesFromQueryFile();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testGeneratedQueries(true, useMultiStageQueryEngine);
  }

  @Test
  public void testDisableGroovyQueryTableConfigOverride()
      throws Exception {
    QueryConfig queryConfig = new QueryConfig(null, false, null, null, null, null);
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);

    String groovyQuery = "SELECT GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
        + "'arg0 + arg1', FlightNum, Origin) FROM mytable";

    // Query should not throw exception
    postQuery(groovyQuery);

    // Disable groovy explicitly
    queryConfig = new QueryConfig(null, true, null, null, null, null);

    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);

    // grpc and http throw different exceptions. So only check error message.
    Exception athrows = expectThrows(Exception.class, () -> postQuery(groovyQuery));
    assertTrue(athrows.getMessage().contains("Groovy transform functions are disabled for queries"));

    // Remove query config
    logicalTableConfig.setQueryConfig(null);
    updateLogicalTableConfig(logicalTableConfig);

    athrows = expectThrows(Exception.class, () -> postQuery(groovyQuery));
    assertTrue(athrows.getMessage().contains("Groovy transform functions are disabled for queries"));
  }

  @Test
  public void testMaxQueryResponseSizeTableConfig()
      throws Exception {
    String starQuery = "SELECT * from mytable";

    QueryConfig queryConfig = new QueryConfig(null, null, null, null, 100L, null);
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);

    JsonNode response = postQuery(starQuery);
    JsonNode exceptions = response.get("exceptions");
    assertTrue(!exceptions.isEmpty()
        && exceptions.get(0).get("errorCode").asInt() == QueryErrorCode.QUERY_CANCELLATION.getId());

    // Query Succeeds with a high limit.
    queryConfig = new QueryConfig(null, null, null, null, 1000000L, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");

    //Reset to null.
    queryConfig = new QueryConfig(null, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");
  }

  @Test
  public void testMaxServerResponseSizeTableConfig()
      throws Exception {
    String starQuery = "SELECT * from mytable";

    QueryConfig queryConfig = new QueryConfig(null, null, null, null, null, 1000L);
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    JsonNode response = postQuery(starQuery);
    JsonNode exceptions = response.get("exceptions");
    assertTrue(!exceptions.isEmpty()
        && exceptions.get(0).get("errorCode").asInt() == QueryErrorCode.QUERY_CANCELLATION.getId());

    // Query Succeeds with a high limit.
    queryConfig = new QueryConfig(null, null, null, null, null, 1000000L);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");

    //Reset to null.
    queryConfig = new QueryConfig(null, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");
  }

  @Test
  public void testQueryTimeOut()
      throws Exception {
    String starQuery = "SELECT * from mytable";
    QueryConfig queryConfig = new QueryConfig(1L, null, null, null, null, null);
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    JsonNode response = postQuery(starQuery);
    JsonNode exceptions = response.get("exceptions");
    assertFalse(exceptions.isEmpty());
    int exceptionCode = exceptions.get(0).get("errorCode").asInt();
    assertTrue(exceptionCode == QueryErrorCode.BROKER_TIMEOUT.getId()
        // Timeout may occur just before submitting the request. Then this error code is thrown.
        || exceptionCode == QueryErrorCode.SERVER_NOT_RESPONDING.getId()
        || exceptionCode == QueryErrorCode.EXECUTION_TIMEOUT.getId()
    );

    // Query Succeeds with a high limit.
    queryConfig = new QueryConfig(1000000L, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");

    //Reset to null.
    queryConfig = new QueryConfig(null, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLogicalTableWithEmptyOfflineTable(boolean useMultiStageQueryEngine)
      throws Exception {

    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String logicalTableName = EMPTY_OFFLINE_TABLE_NAME + "_logical";
    // Query should return empty result
    JsonNode queryResponse = postQuery("SELECT count(*) FROM " + logicalTableName);
    assertEquals(queryResponse.get("numDocsScanned").asInt(), 0);
    assertEquals(queryResponse.get("numServersQueried").asInt(), useMultiStageQueryEngine ? 1 : 0);
    assertTrue(queryResponse.get("exceptions").isEmpty());
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testControllerQuerySubmit(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    @Language("sql")
    String query = "SELECT count(*) FROM " + getLogicalTableName();
    JsonNode response = postQueryToController(query);
    assertNoError(response);

    String tableName =
        getOfflineTableNames().isEmpty() ? getRealtimeTableNames().get(0) : getOfflineTableNames().get(0);
    query = "SELECT count(*) FROM " + tableName;
    response = postQueryToController(query);
    assertNoError(response);

    query = "SELECT count(*) FROM unknown";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");
  }

  @Test
  void testControllerJoinQuerySubmit()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    @Language("sql")
    String query = "SELECT count(*) FROM " + getLogicalTableName() + " JOIN " + getPhysicalTableNames().get(0)
        + " ON " + getLogicalTableName() + ".FlightNum = " + getPhysicalTableNames().get(0) + ".FlightNum";
    JsonNode response = postQueryToController(query);
    assertNoError(response);

    query = "SELECT count(*) FROM unknown JOIN " + getPhysicalTableNames().get(0)
        + " ON unknown.FlightNum = " + getPhysicalTableNames().get(0) + ".FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");

    query = "SELECT count(*) FROM " + getLogicalTableName() + " JOIN known  ON "
        + getLogicalTableName() + ".FlightNum = unknown.FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");
  }

  @Test
  void testPhysicalOptimizerWithLogicalTable()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    setUsePhysicalOptimizer(true);

    // Test simple count query with physical optimizer
    @Language("sql")
    String query = "SET usePhysicalOptimizer=true; SELECT count(*) FROM " + getLogicalTableName();
    JsonNode response = postQueryToController(query);
    assertNoError(response);
    assertTrue(response.get("numDocsScanned").asLong() > 0,
        "Expected some documents to be scanned");

    // Test query with filter
    query = "SET usePhysicalOptimizer=true; SELECT count(*) FROM " + getLogicalTableName()
        + " WHERE FlightNum > 0";
    response = postQueryToController(query);
    assertNoError(response);

    // Test query with aggregation
    query = "SET usePhysicalOptimizer=true; SELECT Carrier, count(*) FROM " + getLogicalTableName()
        + " GROUP BY Carrier LIMIT 10";
    response = postQueryToController(query);
    assertNoError(response);

    // Test query with order by
    query = "SET usePhysicalOptimizer=true; SELECT FlightNum, Carrier FROM " + getLogicalTableName()
        + " ORDER BY FlightNum LIMIT 10";
    response = postQueryToController(query);
    assertNoError(response);

    // Test query with join
    query = "SET usePhysicalOptimizer=true; SELECT count(*) FROM " + getLogicalTableName() + " JOIN "
        + getPhysicalTableNames().get(0) + " ON " + getLogicalTableName() + ".FlightNum = "
        + getPhysicalTableNames().get(0) + ".FlightNum";
    response = postQueryToController(query);
    assertNoError(response);

    // Test error case: unknown table in join
    query = "SET usePhysicalOptimizer=true; SELECT count(*) FROM unknown JOIN " + getPhysicalTableNames().get(0)
        + " ON unknown.FlightNum = " + getPhysicalTableNames().get(0) + ".FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");

    // Test error case: unknown table alias
    query = "SET usePhysicalOptimizer=true; SELECT count(*) FROM " + getLogicalTableName() + " JOIN known ON "
        + getLogicalTableName() + ".FlightNum = unknown.FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");
  }
}
