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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandlerDelegate;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTestSet;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.QueryAssert;
import org.apache.pinot.integration.tests.QueryGenerator;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import static org.testng.Assert.assertTrue;


public abstract class BaseLogicalTableIntegrationTest extends BaseClusterIntegrationTestSet {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseLogicalTableIntegrationTest.class);
  private static final String DEFAULT_TENANT = "DefaultTenant";
  private static final String DEFAULT_LOGICAL_TABLE_NAME = "mytable";
  protected static final String DEFAULT_TABLE_NAME = "physicalTable";
  protected static final String EMPTY_OFFLINE_TABLE_NAME = "empty_o";
  private static final String GROOVY_DISABLED_MESSAGE = "Groovy transform functions are disabled for queries";
  private static final long CONFIG_PROPAGATION_CHECK_INTERVAL_MS = 100L;
  private static final long CONFIG_PROPAGATION_TIMEOUT_MS = 60_000L;
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
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    // Stop Kafka
    LOGGER.info("Stop Kafka in the integration test suite");
    stopKafka();
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
      // tearDown() purges the shared cluster through cleanup() so the next class starts from an empty cluster,
      // and cleanup() reads Helix and the property store directly. Only the instance that ran @BeforeSuite has
      // those handles, so without copying them here cleanup() throws a NullPointerException that replaces
      // whatever it was about to report.
      _helixManager = _sharedClusterTestSuite._helixManager;
      _helixDataAccessor = _sharedClusterTestSuite._helixDataAccessor;
      _helixAdmin = _sharedClusterTestSuite._helixAdmin;
      _propertyStore = _sharedClusterTestSuite._propertyStore;
    }

    _avroFiles = getAllAvroFiles();
    Map<String, List<File>> offlineTableDataFiles = getOfflineTableDataFiles();
    for (Map.Entry<String, List<File>> entry : offlineTableDataFiles.entrySet()) {
      String tableName = entry.getKey();
      List<File> avroFilesForTable = entry.getValue();

      File tarDir = new File(_tarDir, tableName);

      TestUtils.ensureDirectoriesExistAndEmpty(tarDir);

      // Create and upload the schema and table config
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(tableName);
      addSchema(schema);
      TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
      addTableConfig(offlineTableConfig);

      // Create and upload segments
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFilesForTable, offlineTableConfig, schema, 0, _segmentDir,
          tarDir);
      uploadSegments(tableName, tarDir);
    }

    // create realtime table
    Map<String, List<File>> realtimeTableDataFiles = getRealtimeTableDataFiles();
    if (!realtimeTableDataFiles.isEmpty()) {
      // getKafkaTopic() defaults to the class simple name, so every subclass has its own topic, but the shared
      // cluster's @BeforeSuite runs on a single instance and therefore only creates that one instance's topic.
      // Create this class's topic explicitly - a no-op when it already exists - so that a table config pinning
      // stream.kafka.partition.ids is validated against the expected partition count rather than against a
      // single-partition topic auto-created by the broker on first access.
      createKafkaTopic(getKafkaTopic(), getNumKafkaPartitions());
    }
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

  protected Map<String, List<File>> getOfflineTableDataFiles() {
    List<String> offlineTableNames = getOfflineTableNames();
    return !offlineTableNames.isEmpty() ? distributeFilesToTables(offlineTableNames, _avroFiles) : Map.of();
  }

  protected Map<String, List<File>> getRealtimeTableDataFiles() {
    List<String> realtimeTableNames = getRealtimeTableNames();
    return !realtimeTableNames.isEmpty() ? distributeFilesToTables(realtimeTableNames, _avroFiles) : Map.of();
  }

  protected Map<String, List<File>> distributeFilesToTables(List<String> tableNames, List<File> avroFiles) {
    Map<String, List<File>> tableNameToFilesMap = new HashMap<>();

    // Initialize the map with empty lists for each table name
    tableNames.forEach(table -> tableNameToFilesMap.put(table, new ArrayList<>()));

    // Round-robin distribution of files to table names
    for (int i = 0; i < avroFiles.size(); i++) {
      String tableName = tableNames.get(i % tableNames.size());
      tableNameToFilesMap.get(tableName).add(avroFiles.get(i));
    }
    return tableNameToFilesMap;
  }

  private List<String> getTimeBoundaryTable() {
    String timeBoundaryTable = null;
    long maxEndTimeMillis = Long.MIN_VALUE;
    try {
      for (String tableName : getOfflineTableNames()) {
        String response = getOrCreateAdminClient().getSegmentClient()
            .getSegmentsMetadata(tableName, TableType.OFFLINE.toString());
        JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
        Iterator<String> stringIterator = jsonNode.fieldNames();
        while (stringIterator.hasNext()) {
          String segmentName = stringIterator.next();
          JsonNode segmentJsonNode = jsonNode.get(segmentName);
          long endTimeMillis = segmentJsonNode.get("endTimeMillis").asLong();
          if (endTimeMillis > maxEndTimeMillis) {
            maxEndTimeMillis = endTimeMillis;
            timeBoundaryTable = tableName;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get the time boundary table", e);
    }
    return timeBoundaryTable != null ? List.of(TableNameBuilder.OFFLINE.tableNameWithType(timeBoundaryTable))
        : List.of();
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

  /// Creates a new OFFLINE table config.
  protected TableConfig createOfflineTableConfig(String tableName) {
    // @formatter:off
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .build();
    // @formatter:on
  }

  public LogicalTableConfig getLogicalTableConfig(String tableName, List<String> physicalTableNames,
      String brokerTenant) {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (String physicalTableName : physicalTableNames) {
      physicalTableConfigMap.put(physicalTableName, new PhysicalTableConfig());
    }
    String offlineTableName =
        physicalTableNames.stream().filter(TableNameBuilder::isOfflineTableResource).findFirst().orElse(null);
    String realtimeTableName =
        physicalTableNames.stream().filter(TableNameBuilder::isRealtimeTableResource).findFirst().orElse(null);
    LogicalTableConfigBuilder builder =
        new LogicalTableConfigBuilder().setTableName(tableName)
            .setBrokerTenant(brokerTenant)
            .setRefOfflineTableName(offlineTableName)
            .setRefRealtimeTableName(realtimeTableName)
            .setPhysicalTableConfigMap(physicalTableConfigMap);
    if (!getOfflineTableNames().isEmpty() && !getRealtimeTableNames().isEmpty()) {
      builder.setTimeBoundaryConfig(
          new TimeBoundaryConfig("min", Map.of("includedTables", getTimeBoundaryTable()))
      );
    }
    return builder.build();
  }

  protected void createLogicalTable()
      throws Exception {
    Schema logicalTableSchema = createSchema(getSchemaFileName());
    logicalTableSchema.setSchemaName(getLogicalTableName());
    addSchema(logicalTableSchema);
    LogicalTableConfig logicalTable =
        getLogicalTableConfig(getLogicalTableName(), getPhysicalTableNames(), getBrokerTenant());
    String resp =
        getOrCreateAdminClient().getLogicalTableClient().createLogicalTable(logicalTable.toSingleLineJsonString());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + getLogicalTableName()
        + " logical table successfully added.\"}");
  }

  protected LogicalTableConfig getLogicalTableConfig(String logicalTableName)
      throws Exception {
    return getOrCreateAdminClient().getLogicalTableClient().getLogicalTableConfig(logicalTableName);
  }

  private void createLogicalTableWithEmptyOfflineTable()
      throws Exception {
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

    Schema logicalTableSchema = createSchema(getSchemaFileName());
    logicalTableSchema.setSchemaName(logicalTableName);
    addSchema(logicalTableSchema);
    LogicalTableConfigBuilder builder =
        new LogicalTableConfigBuilder().setTableName(logicalTableName)
            .setBrokerTenant(DEFAULT_TENANT)
        .setRefOfflineTableName(refOfflineTableName)
        .setPhysicalTableConfigMap(physicalTableConfigMap);

    String resp = getOrCreateAdminClient().getLogicalTableClient()
        .createLogicalTable(builder.build().toSingleLineJsonString());
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
    waitForAllDocsLoaded(getLogicalTableName(), timeoutMs);
  }

  @Override
  protected void setUpQueryGenerator(List<File> avroFiles) {
    Assert.assertNull(_queryGenerator);
    String tableName = getLogicalTableName();
    _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
  }

  @Test
  public void verifyLogicalTableConfig()
      throws Exception {
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

  /// End-to-end smoke test that enabling broker segment pruning on the MSE logical-planner path executes cleanly for a
  /// logical table and does not change results. It exercises the logical-table routing path where the leaf-stage filter
  /// is forwarded (via `WorkerManager.buildLogicalTableRoutingBrokerRequest`) into each physical table's routing
  /// request so the `LogicalTableRouteProvider` runs segment pruners over a filter-bearing request rather than a
  /// bare `SELECT *`. A malformed forwarded request would surface here as an exception or a changed result.
  ///
  /// Note: these physical tables carry no segment-pruner config, so this asserts correctness, not that a segment was
  /// actually pruned; the exact pruning behavior is unit-tested in `WorkerManagerTest`.
  @Test(dataProvider = "useBothQueryEngines")
  public void testBrokerPruningPreservesLogicalTableResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String logicalTableName = getLogicalTableName();
    String filteredQuery = "SELECT COUNT(*) FROM " + logicalTableName + " WHERE DaysSinceEpoch > 16312";

    // Broker pruning is on by default on the MSE logical planner path; disable it explicitly for the baseline so the
    // two runs exercise different routing paths.
    JsonNode withoutPruning = postQuery("SET useBrokerPruning=false; " + filteredQuery);
    assertTrue(withoutPruning.get("exceptions").isEmpty(), "Unexpected exceptions without broker pruning");

    JsonNode withPruning = postQuery(filteredQuery);
    assertTrue(withPruning.get("exceptions").isEmpty(), "Unexpected exceptions with broker pruning");

    // Broker pruning is a routing optimization only; the result must be identical to the unpruned run.
    assertEquals(withPruning.get("resultTable").get("rows").get(0).get(0).asLong(),
        withoutPruning.get("resultTable").get("rows").get(0).get(0).asLong(),
        "Broker pruning changed the result of a logical table query");
  }

  /// Both leaf-stage segment list encodings must return the same result for a logical table, whose leaf workers carry
  /// `logicalTableSegmentsMap`, keyed by physical table name, rather than the table-type keyed `tableSegmentsMap`.
  /// The encoding is turned on through cluster config, the way an operator would, and turned off again because the
  /// cluster is shared with the other logical-table test classes.
  @Test
  public void testProtoSegmentListPreservesLogicalTableResults()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "SELECT Carrier, COUNT(*) FROM " + getLogicalTableName() + " WHERE DaysSinceEpoch > 16312 "
        + "GROUP BY Carrier ORDER BY Carrier LIMIT 100";
    // The cluster was started by the shared suite instance, which is the one holding the broker starter.
    QueryDispatcher dispatcher =
        ((BrokerRequestHandlerDelegate) _sharedClusterTestSuite._brokerStarters.get(0).getBrokerRequestHandler())
            .getMultiStageBrokerRequestHandler().getQueryDispatcher();
    assertTrue(!dispatcher.isEnableProtoSegmentList(), "The proto segment list encoding must ship disabled");

    JsonNode legacy = postQuery(query);
    assertTrue(legacy.get("exceptions").isEmpty(), "Unexpected exceptions with the legacy encoding: " + legacy);

    try {
      setProtoSegmentList(true);
      TestUtils.waitForCondition(aVoid -> dispatcher.isEnableProtoSegmentList(), 10_000L,
          "Enabling the proto segment list encoding in cluster config did not reach the broker");

      JsonNode proto = postQuery(query);
      assertTrue(proto.get("exceptions").isEmpty(), "Unexpected exceptions with the proto encoding: " + proto);
      assertEquals(proto.get("resultTable").get("rows"), legacy.get("resultTable").get("rows"),
          "The segment list encoding changed the result of a logical table query");
    } finally {
      setProtoSegmentList(false);
      TestUtils.waitForCondition(aVoid -> !dispatcher.isEnableProtoSegmentList(), 10_000L,
          "Disabling the proto segment list encoding in cluster config did not reach the broker");
    }
  }

  private void setProtoSegmentList(boolean enabled)
      throws Exception {
    sendPostRequest(_controllerRequestURLBuilder.forClusterConfigs(),
        JsonUtils.objectToString(
            Map.of(CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_PROTO_SEGMENT_LIST, String.valueOf(enabled))));
  }

  @Test
  public void testDisableGroovyQueryTableConfigOverride()
      throws Exception {
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    String groovyQuery = "SELECT GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
        + "'arg0 + arg1', FlightNum, Origin) FROM " + getLogicalTableName();

    // Every step has to flip the outcome of the step before it. A wait whose condition already holds under the
    // previous config returns immediately and proves nothing, so the config being cleared is checked from the
    // enabled state: the cluster default disables Groovy exactly like the explicit override does.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, false, null, null, null, null),
        () -> succeeds(groovyQuery), "Groovy query kept failing after groovy was enabled");

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, true, null, null, null, null),
        () -> failsWithGroovyDisabled(groovyQuery), "Groovy query kept succeeding after groovy was disabled");

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, false, null, null, null, null),
        () -> succeeds(groovyQuery), "Groovy query kept failing after groovy was enabled again");

    // Removing the query config falls back to the cluster default, which disables groovy again.
    applyQueryConfigAndAwait(logicalTableConfig, null, () -> failsWithGroovyDisabled(groovyQuery),
        "Groovy query kept succeeding after the query config was removed");
  }

  /// Returns whether `query` fails because groovy is disabled, rather than succeeding or failing for another reason.
  ///
  /// Returns instead of asserting so that [#applyQueryConfigAndAwait] can retry: an assertion failure is an
  /// `AssertionError`, which the wait helper does not treat as a not-yet-satisfied condition.
  private boolean failsWithGroovyDisabled(String query) {
    try {
      postQuery(query);
      return false;
    } catch (Exception e) {
      // grpc and http throw different exceptions, so only check the error message.
      String message = e.getMessage();
      return message != null && message.contains(GROOVY_DISABLED_MESSAGE);
    }
  }

  @Test
  public void testMaxQueryResponseSizeTableConfig()
      throws Exception {
    String starQuery = "SELECT * from " + getLogicalTableName();
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, 100L, null),
        () -> failsWith(starQuery, QueryErrorCode.QUERY_CANCELLATION),
        "Query was not cancelled under a 100 byte response size limit");

    // Query succeeds with a high limit.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, 1000000L, null),
        () -> succeeds(starQuery), "Query kept failing under a high response size limit");

    // Restore the restrictive limit so that clearing it below is observable: waiting for success straight after
    // the high limit would be satisfied by the high limit itself.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, 100L, null),
        () -> failsWith(starQuery, QueryErrorCode.QUERY_CANCELLATION),
        "Query was not cancelled after the 100 byte response size limit was restored");

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, null),
        () -> succeeds(starQuery), "Query kept failing after the response size limit was cleared");
  }

  @Test
  public void testMaxServerResponseSizeTableConfig()
      throws Exception {
    String starQuery = "SELECT * from " + getLogicalTableName();
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, 1000L),
        () -> failsWith(starQuery, QueryErrorCode.QUERY_CANCELLATION),
        "Query was not cancelled under a 1000 byte server response size limit");

    // Query succeeds with a high limit.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, 1000000L),
        () -> succeeds(starQuery), "Query kept failing under a high server response size limit");

    // Restore the restrictive limit so that clearing it below is observable.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, 1000L),
        () -> failsWith(starQuery, QueryErrorCode.QUERY_CANCELLATION),
        "Query was not cancelled after the 1000 byte server response size limit was restored");

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, null),
        () -> succeeds(starQuery), "Query kept failing after the server response size limit was cleared");
  }

  @Test
  public void testQueryTimeOut()
      throws Exception {
    String starQuery = "SELECT * from " + getLogicalTableName();
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());

    // A 1 ms budget can expire at any stage, and each stage reports its own code: before the request is
    // submitted (SERVER_NOT_RESPONDING), while it waits to be scheduled (QUERY_SCHEDULING_TIMEOUT), while a
    // server runs it (EXECUTION_TIMEOUT) or while the broker waits for servers (BROKER_TIMEOUT). Which one wins
    // depends on how much work the table shape implies, so accept any of them.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(1L, null, null, null, null, null),
        () -> failsWith(starQuery, QueryErrorCode.BROKER_TIMEOUT, QueryErrorCode.SERVER_NOT_RESPONDING,
            QueryErrorCode.QUERY_SCHEDULING_TIMEOUT, QueryErrorCode.EXECUTION_TIMEOUT),
        "Query did not time out under a 1 ms timeout");

    // Query succeeds with a high timeout.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(1000000L, null, null, null, null, null),
        () -> succeeds(starQuery), "Query kept failing under a high timeout");

    // Restore the 1 ms timeout so that clearing the override below is observable.
    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(1L, null, null, null, null, null),
        () -> failsWith(starQuery, QueryErrorCode.BROKER_TIMEOUT, QueryErrorCode.SERVER_NOT_RESPONDING,
            QueryErrorCode.QUERY_SCHEDULING_TIMEOUT, QueryErrorCode.EXECUTION_TIMEOUT),
        "Query did not time out after the 1 ms timeout was restored");

    applyQueryConfigAndAwait(logicalTableConfig, new QueryConfig(null, null, null, null, null, null),
        () -> succeeds(starQuery), "Query kept failing after the timeout override was cleared");
  }

  /// Applies `queryConfig` to `logicalTableConfig` and waits until the brokers act on it.
  ///
  /// Updating a logical table config through the controller is asynchronous: brokers observe the change through
  /// a ZooKeeper property store listener, so a query issued right after the REST call can still be planned with
  /// the previous config. Waiting for the new behavior keeps these assertions from racing that propagation.
  private void applyQueryConfigAndAwait(LogicalTableConfig logicalTableConfig, @Nullable QueryConfig queryConfig,
      TestUtils.SupplierWithException<Boolean> brokerAppliedConfig, String message)
      throws Exception {
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    TestUtils.waitForCondition(brokerAppliedConfig, CONFIG_PROPAGATION_CHECK_INTERVAL_MS,
        CONFIG_PROPAGATION_TIMEOUT_MS, message, Duration.ofMillis(CONFIG_PROPAGATION_TIMEOUT_MS / 4));
  }

  /// Returns whether `query` completes without any exception.
  private boolean succeeds(String query)
      throws Exception {
    return postQuery(query).get("exceptions").isEmpty();
  }

  /// Returns whether `query` reports an exception whose error code is one of `expectedErrorCodes`.
  private boolean failsWith(String query, QueryErrorCode... expectedErrorCodes)
      throws Exception {
    JsonNode exceptions = postQuery(query).get("exceptions");
    if (exceptions.isEmpty()) {
      return false;
    }
    int errorCode = exceptions.get(0).get("errorCode").asInt();
    for (QueryErrorCode expectedErrorCode : expectedErrorCodes) {
      if (errorCode == expectedErrorCode.getId()) {
        return true;
      }
    }
    return false;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLogicalTableWithEmptyOfflineTable(boolean useMultiStageQueryEngine)
      throws Exception {

    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String logicalTableName = EMPTY_OFFLINE_TABLE_NAME + "_logical";
    // Query should return empty result
    JsonNode queryResponse = postQuery("SELECT count(*) FROM " + logicalTableName);
    assertEquals(queryResponse.get("numDocsScanned").asInt(), 0);
    // Neither engine dispatches to servers for an empty table: the multi-stage broker short-circuits when all leaf
    // stages are empty (#18538).
    assertEquals(queryResponse.get("numServersQueried").asInt(), 0, "Query should not dispatch to servers");
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
