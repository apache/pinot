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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory.ResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Single integration test class that tests all query killing modes (CPU broker, CPU server, memory server)
/// using one shared cluster. Tests run in priority order, each progressively enabling its kill mode via
/// cluster config updates and disabling the previous test's mode.
public class QueryKillingIntegrationTest extends BaseClusterIntegrationTest {
  private static final String STRING_DIM_SV1 = "stringDimSV1";
  private static final String STRING_DIM_SV2 = "stringDimSV2";
  private static final String INT_DIM_SV1 = "intDimSV1";
  private static final String LONG_DIM_SV1 = "longDimSV1";
  private static final String DOUBLE_DIM_SV1 = "doubleDimSV1";
  private static final String BOOLEAN_DIM_SV1 = "booleanDimSV1";
  private static final int NUM_SEGMENTS = 3;
  private static final int NUM_DOCS_PER_SEGMENT = 3_000_000;

  private static final String LARGE_SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 9000000";

  private static final String LARGE_DISTINCT_QUERY =
      "SELECT DISTINCT stringDimSV2 FROM mytable ORDER BY stringDimSV2 LIMIT 3000000";

  private static final String LARGE_GROUP_BY_QUERY =
      "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 1 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, pair-wise combine
  private static final String LARGE_GROUP_BY_PAIRWISE_COMBINE_QUERY =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, sequential combine
  private static final String LARGE_GROUP_BY_SEQUENTIAL_COMBINE_QUERY =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  private static final String AGGREGATE_QUERY = "SELECT MIN(intDimSV1) FROM mytable";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 5";

  private static final String[] EXPENSIVE_QUERIES = {
      LARGE_SELECT_STAR_QUERY, LARGE_DISTINCT_QUERY, LARGE_GROUP_BY_QUERY, LARGE_GROUP_BY_PAIRWISE_COMBINE_QUERY,
      LARGE_GROUP_BY_SEQUENTIAL_COMBINE_QUERY
  };

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_TABLE_NAME)
        .addSingleValueDimension(STRING_DIM_SV1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_DIM_SV2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(INT_DIM_SV1, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_DIM_SV1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_DIM_SV1, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(BOOLEAN_DIM_SV1, FieldSpec.DataType.BOOLEAN)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    // Enable sampling and factory at startup. Kill modes are toggled dynamically per test.
    String prefix = Accounting.BROKER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.Keys.QUERY_KILLED_METRIC_ENABLED, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, Integer.MAX_VALUE);

    String prefix = Accounting.SERVER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(prefix + Accounting.Keys.QUERY_KILLED_METRIC_ENABLED, true);
  }

  @Override
  protected long getCountStarResult() {
    return NUM_SEGMENTS * NUM_DOCS_PER_SEGMENT;
  }

  // ---------------------------------------------------------------------------
  // Test: default accountant values after startup
  // ---------------------------------------------------------------------------

  @Test
  public void testDefaultValues() {
    assertTrue(ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled());

    ThreadAccountant brokerAccountant = _brokerStarters.get(0).getThreadAccountant();
    assertTrue(brokerAccountant instanceof ResourceUsageAccountant);
    QueryMonitorConfig brokerConfig = ((ResourceUsageAccountant) brokerAccountant).getQueryMonitorConfig();
    assertFalse(brokerConfig.isOomKillQueryEnabled());
    assertTrue(brokerConfig.isQueryKilledMetricEnabled());

    ThreadAccountant serverAccountant = _serverStarters.get(0).getServerInstance().getThreadAccountant();
    assertTrue(serverAccountant instanceof ResourceUsageAccountant);
    QueryMonitorConfig serverConfig = ((ResourceUsageAccountant) serverAccountant).getQueryMonitorConfig();
    assertTrue(serverConfig.isOomKillQueryEnabled());
    assertTrue(serverConfig.isQueryKilledMetricEnabled());
  }

  // ---------------------------------------------------------------------------
  // Test: resource usage stats (no kill mode needed, just verifies sampling works)
  // ---------------------------------------------------------------------------

  @Test
  public void testResourceUsageStats()
      throws Exception {
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    long offlineThreadMemAllocatedBytes = queryResponse.get("offlineThreadMemAllocatedBytes").asLong();
    long offlineResponseSerMemAllocatedBytes = queryResponse.get("offlineResponseSerMemAllocatedBytes").asLong();
    long offlineTotalMemAllocatedBytes = queryResponse.get("offlineTotalMemAllocatedBytes").asLong();

    assertTrue(offlineThreadMemAllocatedBytes > 0);
    assertTrue(offlineResponseSerMemAllocatedBytes > 0);
    assertEquals(offlineThreadMemAllocatedBytes + offlineResponseSerMemAllocatedBytes, offlineTotalMemAllocatedBytes);

    long offlineThreadCpuTimeNs = queryResponse.get("offlineThreadCpuTimeNs").asLong();
    long offlineSystemActivitiesCpuTimeNs = queryResponse.get("offlineSystemActivitiesCpuTimeNs").asLong();
    long offlineResponseSerializationCpuTimeNs = queryResponse.get("offlineResponseSerializationCpuTimeNs").asLong();
    long offlineTotalCpuTimeNs = queryResponse.get("offlineTotalCpuTimeNs").asLong();
    assertTrue(offlineThreadCpuTimeNs > 0);
    assertTrue(offlineSystemActivitiesCpuTimeNs > 0);
    assertTrue(offlineResponseSerializationCpuTimeNs > 0);
    assertEquals(offlineThreadCpuTimeNs + offlineSystemActivitiesCpuTimeNs + offlineResponseSerializationCpuTimeNs,
        offlineTotalCpuTimeNs);
  }

  // ---------------------------------------------------------------------------
  // Test: CPU-based broker query killing
  // ---------------------------------------------------------------------------

  @Test(dependsOnMethods = {"testDefaultValues", "testResourceUsageStats"})
  public void testCpuBasedBrokerQueryKilling()
      throws Exception {
    // Set server heap ratios high to prevent server OOM from killing the query first
    updateClusterConfig(Map.of(
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED, "true",
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS, "500",
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, "1.1",
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, "1.1",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, "1.1",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, "1.1"
    ));
    TestUtils.waitForCondition(
        aVoid -> getBrokerResourceUsageAccountant().getQueryMonitorConfig().isCpuTimeBasedKillingEnabled(), 10_000L,
        "Failed to enable CPU killing on broker");

    // Single query kill — use MSE only (other engines can cause OOM on server first)
    setUseMultiStageQueryEngine(true);
    verifyCpuTimeKill(LARGE_SELECT_STAR_QUERY, postQuery(LARGE_SELECT_STAR_QUERY), "BROKER");

    // Multiple concurrent queries kill
    JsonNode[] responses = runConcurrentQueries(LARGE_SELECT_STAR_QUERY, AGGREGATE_QUERY, SELECT_STAR_QUERY);
    verifyCpuTimeKill(LARGE_SELECT_STAR_QUERY, responses[0], "BROKER");
    verifyNoExceptions(AGGREGATE_QUERY, responses[1]);
    verifyNoExceptions(SELECT_STAR_QUERY, responses[2]);
  }

  // ---------------------------------------------------------------------------
  // Test: CPU-based server query killing
  // ---------------------------------------------------------------------------

  @Test(dependsOnMethods = "testCpuBasedBrokerQueryKilling")
  public void testCpuBasedServerQueryKilling()
      throws Exception {
    // Disable broker CPU killing from previous test, enable server CPU killing
    setUseMultiStageQueryEngine(false);
    updateClusterConfig(Map.of(
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED, "false",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED, "true",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS, "500",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, "1.1",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, "1.1"
    ));
    TestUtils.waitForCondition(aVoid -> {
      QueryMonitorConfig brokerConfig = getBrokerResourceUsageAccountant().getQueryMonitorConfig();
      QueryMonitorConfig serverConfig = getServerResourceUsageAccountant().getQueryMonitorConfig();
      return !brokerConfig.isCpuTimeBasedKillingEnabled() && serverConfig.isCpuTimeBasedKillingEnabled();
    }, 10_000L, "Failed to switch from broker to server CPU killing");

    // Single query kill with each expensive query, both SSE and MSE
    for (String query : EXPENSIVE_QUERIES) {
      verifyCpuTimeKill(query, postQuery(query), "SERVER");
      setUseMultiStageQueryEngine(true);
      verifyCpuTimeKill(query, postQuery(query), "SERVER");
      setUseMultiStageQueryEngine(false);
    }

    // Multiple concurrent queries kill, both SSE and MSE
    for (boolean useMSE : new boolean[]{false, true}) {
      setUseMultiStageQueryEngine(useMSE);
      JsonNode[] responses = runConcurrentQueries(LARGE_DISTINCT_QUERY, AGGREGATE_QUERY, SELECT_STAR_QUERY);
      verifyCpuTimeKill(LARGE_DISTINCT_QUERY, responses[0], "SERVER");
      verifyNoExceptions(AGGREGATE_QUERY, responses[1]);
      verifyNoExceptions(SELECT_STAR_QUERY, responses[2]);
    }
  }

  // ---------------------------------------------------------------------------
  // Test: memory-based (OOM) server query killing
  // ---------------------------------------------------------------------------

  @Test(dependsOnMethods = "testCpuBasedServerQueryKilling")
  public void testMemoryBasedServerQueryKilling()
      throws Exception {
    // Disable server CPU killing from previous test, enable server OOM killing
    setUseMultiStageQueryEngine(false);
    updateClusterConfig(Map.of(
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED, "false",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "true",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO, "0",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, "0.15"
    ));
    TestUtils.waitForCondition(aVoid -> {
      QueryMonitorConfig serverConfig = getServerResourceUsageAccountant().getQueryMonitorConfig();
      return !serverConfig.isCpuTimeBasedKillingEnabled() && serverConfig.isOomKillQueryEnabled();
    }, 10_000L, "Failed to switch from CPU to OOM killing on server");

    // Single query OOM kill with each expensive query, both SSE and MSE
    for (String query : EXPENSIVE_QUERIES) {
      verifyOomKill(query, postQuery(query));
      setUseMultiStageQueryEngine(true);
      verifyOomKill(query, postQuery(query));
      setUseMultiStageQueryEngine(false);
    }

    // Multiple concurrent queries OOM kill, both SSE and MSE
    for (boolean useMSE : new boolean[]{false, true}) {
      setUseMultiStageQueryEngine(useMSE);
      JsonNode[] responses = runConcurrentQueries(LARGE_DISTINCT_QUERY, AGGREGATE_QUERY, SELECT_STAR_QUERY);
      verifyOomKill(LARGE_DISTINCT_QUERY, responses[0]);
      verifyNoExceptions(AGGREGATE_QUERY, responses[1]);
      verifyNoExceptions(SELECT_STAR_QUERY, responses[2]);
    }
  }

  // ---------------------------------------------------------------------------
  // Test: resource usage stats after all killing tests (verifies accountant is not degraded)
  // ---------------------------------------------------------------------------

  @Test(dependsOnMethods = "testMemoryBasedServerQueryKilling")
  public void testResourceUsageStatsAfterKilling()
      throws Exception {
    // Disable OOM killing from previous test
    setUseMultiStageQueryEngine(false);
    updateClusterConfig(Map.of(
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "false",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO, "0.75",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, "0.96"
    ));
    TestUtils.waitForCondition(
        aVoid -> !getServerResourceUsageAccountant().getQueryMonitorConfig().isOomKillQueryEnabled(), 10_000L,
        "Failed to disable OOM killing on server");

    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    long offlineThreadMemAllocatedBytes = queryResponse.get("offlineThreadMemAllocatedBytes").asLong();
    long offlineResponseSerMemAllocatedBytes = queryResponse.get("offlineResponseSerMemAllocatedBytes").asLong();
    long offlineTotalMemAllocatedBytes = queryResponse.get("offlineTotalMemAllocatedBytes").asLong();

    assertTrue(offlineThreadMemAllocatedBytes > 0,
        "offlineThreadMemAllocatedBytes should be > 0 after killing tests");
    assertTrue(offlineResponseSerMemAllocatedBytes > 0,
        "offlineResponseSerMemAllocatedBytes should be > 0 after killing tests");
    assertEquals(offlineThreadMemAllocatedBytes + offlineResponseSerMemAllocatedBytes, offlineTotalMemAllocatedBytes);

    long offlineThreadCpuTimeNs = queryResponse.get("offlineThreadCpuTimeNs").asLong();
    long offlineSystemActivitiesCpuTimeNs = queryResponse.get("offlineSystemActivitiesCpuTimeNs").asLong();
    long offlineResponseSerializationCpuTimeNs = queryResponse.get("offlineResponseSerializationCpuTimeNs").asLong();
    long offlineTotalCpuTimeNs = queryResponse.get("offlineTotalCpuTimeNs").asLong();
    assertTrue(offlineThreadCpuTimeNs > 0,
        "offlineThreadCpuTimeNs should be > 0 after killing tests");
    assertTrue(offlineSystemActivitiesCpuTimeNs > 0,
        "offlineSystemActivitiesCpuTimeNs should be > 0 after killing tests");
    assertTrue(offlineResponseSerializationCpuTimeNs > 0,
        "offlineResponseSerializationCpuTimeNs should be > 0 after killing tests");
    assertEquals(offlineThreadCpuTimeNs + offlineSystemActivitiesCpuTimeNs + offlineResponseSerializationCpuTimeNs,
        offlineTotalCpuTimeNs);
  }

  // ---------------------------------------------------------------------------
  // Test: dynamic toggling of query kill via cluster config
  // ---------------------------------------------------------------------------

  @Test(dependsOnMethods = "testResourceUsageStatsAfterKilling")
  public void testDynamicallyToggleQueryKill()
      throws Exception {
    ResourceUsageAccountant brokerAccountant =
        (ResourceUsageAccountant) _brokerStarters.get(0).getThreadAccountant();
    ResourceUsageAccountant serverAccountant =
        (ResourceUsageAccountant) _serverStarters.get(0).getServerInstance().getThreadAccountant();

    // Ensure initial state: broker OOM disabled, server OOM enabled
    updateClusterConfig(Map.of(
        Accounting.BROKER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "false",
        Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "true"
    ));
    TestUtils.waitForCondition(aVoid -> !brokerAccountant.getQueryMonitorConfig().isOomKillQueryEnabled()
            && serverAccountant.getQueryMonitorConfig().isOomKillQueryEnabled(), 10_000L,
        "Failed to set initial OOM state");

    // Role-specific prefix should be dynamically applied
    updateClusterConfig(
        Map.of(Accounting.BROKER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "true"));
    TestUtils.waitForCondition(aVoid -> brokerAccountant.getQueryMonitorConfig().isOomKillQueryEnabled(), 10_000L,
        "Failed to enable broker side query kill dynamically");
    updateClusterConfig(
        Map.of(Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "false"));
    TestUtils.waitForCondition(aVoid -> !serverAccountant.getQueryMonitorConfig().isOomKillQueryEnabled(), 10_000L,
        "Failed to disable server side query kill dynamically");

    // Common prefix should not override role-specific prefix
    updateClusterConfig(
        Map.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, "false"));
    assertTrue(brokerAccountant.getQueryMonitorConfig().isOomKillQueryEnabled());
    assertFalse(serverAccountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private ResourceUsageAccountant getBrokerResourceUsageAccountant() {
    return (ResourceUsageAccountant) _brokerStarters.get(0).getThreadAccountant();
  }

  private ResourceUsageAccountant getServerResourceUsageAccountant() {
    return (ResourceUsageAccountant) _serverStarters.get(0).getServerInstance().getThreadAccountant();
  }

  private JsonNode[] runConcurrentQueries(String expensiveQuery, String cheapQuery1, String cheapQuery2)
      throws Exception {
    AtomicReference<JsonNode> response1 = new AtomicReference<>();
    AtomicReference<JsonNode> response2 = new AtomicReference<>();
    AtomicReference<JsonNode> response3 = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(3);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(() -> {
      response1.set(postQuery(expensiveQuery));
      latch.countDown();
      return null;
    });
    executor.submit(() -> {
      response2.set(postQuery(cheapQuery1));
      latch.countDown();
      return null;
    });
    executor.submit(() -> {
      response3.set(postQuery(cheapQuery2));
      latch.countDown();
      return null;
    });
    executor.shutdown();
    latch.await();
    return new JsonNode[]{response1.get(), response2.get(), response3.get()};
  }

  private void verifyCpuTimeKill(String query, JsonNode response, String killedOn) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertEquals(exceptionsNode.size(), 1, "Expected 1 exception for query: " + query + ", but got: " + exceptionsNode);
    JsonNode exceptionNode = exceptionsNode.get(0);
    JsonNode errorCodeNode = exceptionNode.get("errorCode");
    assertNotNull(errorCodeNode, "Missing errorCode from exception: " + exceptionNode);
    int errorCode = errorCodeNode.asInt();
    assertEquals(errorCode, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error code: " + errorCode + " from exception: " + exceptionNode);
    JsonNode messageNode = exceptionNode.get("message");
    assertNotNull(messageNode, "Missing message from exception: " + exceptionNode);
    String message = messageNode.asText();
    assertTrue(message.contains("CPU time based killed on " + killedOn),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
  }

  private void verifyOomKill(String query, JsonNode response) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertEquals(exceptionsNode.size(), 1, "Expected 1 exception for query: " + query + ", but got: " + exceptionsNode);
    JsonNode exceptionNode = exceptionsNode.get(0);
    JsonNode errorCodeNode = exceptionNode.get("errorCode");
    assertNotNull(errorCodeNode, "Missing errorCode from exception: " + exceptionNode);
    int errorCode = errorCodeNode.asInt();
    assertEquals(errorCode, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error code: " + errorCode + " from exception: " + exceptionNode);
    JsonNode messageNode = exceptionNode.get("message");
    assertNotNull(messageNode, "Missing message from exception: " + exceptionNode);
    String message = messageNode.asText();
    assertTrue(message.contains("OOM killed on SERVER"),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
  }

  private void verifyNoExceptions(String query, JsonNode response) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for query: " + query + ", but got: " + exceptionsNode);
  }

  private List<File> createAvroFiles()
      throws IOException {
    // Create Avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(new Field(STRING_DIM_SV1, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(STRING_DIM_SV2, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(INT_DIM_SV1, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(LONG_DIM_SV1, org.apache.avro.Schema.create(Type.LONG), null, null),
        new Field(DOUBLE_DIM_SV1, org.apache.avro.Schema.create(Type.DOUBLE), null, null),
        new Field(BOOLEAN_DIM_SV1, org.apache.avro.Schema.create(Type.BOOLEAN), null, null)));

    // Create Avro files
    List<File> ret = new ArrayList<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      File avroFile = new File(_tempDir, "data_" + segmentId + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        int randBound = NUM_DOCS_PER_SEGMENT / 2;
        Random random = new Random(0);
        for (int docId = 0; docId < NUM_DOCS_PER_SEGMENT; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(STRING_DIM_SV1, "test query killing");
          record.put(STRING_DIM_SV2, "test query killing" + docId);
          record.put(INT_DIM_SV1, random.nextInt(randBound));
          record.put(LONG_DIM_SV1, random.nextLong());
          record.put(DOUBLE_DIM_SV1, random.nextDouble());
          record.put(BOOLEAN_DIM_SV1, true);
          fileWriter.append(record);
        }
        ret.add(avroFile);
      }
    }
    return ret;
  }
}
