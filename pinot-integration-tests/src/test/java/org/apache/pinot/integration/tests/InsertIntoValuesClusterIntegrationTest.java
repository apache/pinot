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
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingest.InsertConsistencyMode;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Integration tests for push-based INSERT INTO functionality.
///
/// Tests the controller coordinator REST API for statement lifecycle management,
/// idempotency, hybrid table validation, abort, and list operations.
///
/// The ROW executor is registered with the coordinator at controller startup,
/// so submitted inserts are expected to succeed (state = VISIBLE).
///
/// **Base class choice:** this test extends {@link BaseClusterIntegrationTest} rather than
/// {@link org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest}
/// for three concrete reasons:
/// - **Per-test table topology** — each test method creates its own OFFLINE and/or REALTIME
///   table inside the test body and tears it down at the end. This is required because tests
///   exercise hybrid table validation (which needs both _OFFLINE and _REALTIME variants present),
///   table-not-found rejection (needs a table to NOT exist), and idempotency across distinct
///   tables. The `CustomDataQueryClusterIntegrationTest` pattern of one shared schema/table
///   per test class cannot express these topologies.
/// - **Coordinator state isolation** — each test asserts on coordinator-internal state
///   (active-statement counter, request-id reservation tombstones, manifest GC). Sharing tables
///   across tests would let one test's manifest leak into another's listStatements assertions.
/// - **Controller config override** — {@link #overrideControllerConf} sets
///   `controller.insert.enabled=true`. `BaseClusterIntegrationTest` runs setUp/tearDown
///   per class so the flag binding is stable; cluster restart per method is not needed.
public class InsertIntoValuesClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InsertIntoValuesClusterIntegrationTest.class);

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    /// Enable the push-based INSERT INTO feature flag. The flag defaults to false in production
    /// so operators must opt in; integration tests need it on to exercise the coordinator.
    properties.put(ControllerConf.INSERT_ENABLED, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    startZk();
    startController();
    startBroker();
    startServers(2);
    startKafkaWithoutTopic();
  }

  @AfterClass
  public void tearDown() {
    try {
      stopServer();
      stopBroker();
      stopController();
      stopKafka();
      stopZk();
    } finally {
      FileUtils.deleteQuietly(_tempDir);
    }
  }

  private void createOfflineTable(String tableName)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(tableName)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("name", DataType.STRING)
        .addMetric("score", DataType.FLOAT)
        .build();
    addSchema(schema);

    createOfflineTableVariant(tableName, null);
  }

  private void createOfflineTableVariant(String tableName, String timeColumnName)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(timeColumnName)
        .build();
    sendPostRequest(
        _controllerRequestURLBuilder.forTableCreate(),
        tableConfig.toString(),
        BasicAuthTestUtils.AUTH_HEADER);
  }

  private String buildInsertRequestJson(String tableName, TableType tableType, String requestId,
      String payloadHash)
      throws Exception {
    return buildInsertRequestJson(tableName, tableType, requestId, payloadHash,
        List.of(Map.of("id", 1, "name", "test", "score", 90.0)));
  }

  private String buildInsertRequestJson(String tableName, TableType tableType, String requestId,
      String payloadHash, List<Map<String, Object>> fields)
      throws Exception {
    Map<String, Object> request = new HashMap<>();
    request.put("tableName", tableName);
    request.put("insertType", InsertType.ROW.name());
    request.put("consistencyMode", InsertConsistencyMode.WAIT_FOR_ACCEPT.name());

    if (tableType != null) {
      request.put("tableType", tableType.name());
    }
    if (requestId != null) {
      request.put("requestId", requestId);
    }
    if (payloadHash != null) {
      request.put("payloadHash", payloadHash);
    }

    List<Map<String, Object>> rowList = new ArrayList<>();
    for (Map<String, Object> fieldMap : fields) {
      rowList.add(Map.of("fieldToValueMap", fieldMap));
    }
    request.put("rows", rowList);

    return JsonUtils.objectToString(request);
  }

  private void createRealtimeTable(String tableName, String kafkaTopic, boolean fullUpsert)
      throws Exception {
    createKafkaTopic(kafkaTopic);
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder()
        .setSchemaName(tableName)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("name", DataType.STRING)
        .addMetric("score", DataType.FLOAT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    if (fullUpsert) {
      schemaBuilder.setPrimaryKeyColumns(List.of("id"));
    }
    addSchema(schemaBuilder.build());

    TableConfig tableConfig;
    Map<String, String> csvDecoder = getCSVDecoderProperties(",", "id,name,score,ts");
    if (fullUpsert) {
      UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
      upsertConfig.setComparisonColumn("ts");
      tableConfig = createCSVUpsertTableConfig(tableName, kafkaTopic, getNumKafkaPartitions(), csvDecoder,
          upsertConfig, "id");
    } else {
      Map<String, String> streamConfigs = getStreamConfigMap();
      String topicProperty =
          StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME);
      streamConfigs.put(topicProperty, kafkaTopic);
      streamConfigs.putAll(csvDecoder);
      tableConfig = new TableConfigBuilder(TableType.REALTIME)
          .setTableName(tableName)
          .setTimeColumnName("ts")
          .setStreamConfigs(streamConfigs)
          .build();
    }
    addTableConfig(tableConfig);
  }

  @Override
  protected String getTimeColumnName() {
    return "ts";
  }

  private JsonNode postInsertRequest(String payload)
      throws Exception {
    /// The /insert/execute endpoint requires ?tableName=<table> as a query param to bind the
    /// table-scoped @Authorize check. Extract the value from the payload so callers don't need to
    /// pass it twice.
    String tableName = JsonUtils.stringToJsonNode(payload).get("tableName").asText();
    String url = getControllerBaseApiUrl() + "/insert/execute?tableName="
        + URLEncoder.encode(tableName, StandardCharsets.UTF_8);
    String response = sendPostRequest(url, payload,
        Collections.singletonMap("accept", "application/json"));
    return JsonUtils.stringToJsonNode(response);
  }

  private JsonNode getInsertStatus(String statementId, String tableNameWithType)
      throws Exception {
    String url = getControllerBaseApiUrl() + "/insert/status/" + statementId
        + "?tableName=" + tableNameWithType;
    String response = sendGetRequest(url);
    return JsonUtils.stringToJsonNode(response);
  }

  private JsonNode postInsertAbort(String statementId, String tableNameWithType)
      throws Exception {
    String url = getControllerBaseApiUrl() + "/insert/abort/" + statementId
        + "?tableName=" + tableNameWithType;
    String response = sendPostRequest(url, null,
        Collections.singletonMap("accept", "application/json"));
    return JsonUtils.stringToJsonNode(response);
  }

  private JsonNode getInsertList(String tableNameWithType)
      throws Exception {
    String url = getControllerBaseApiUrl() + "/insert/list?tableName=" + tableNameWithType;
    String response = sendGetRequest(url);
    return JsonUtils.stringToJsonNode(response);
  }

  /// ---- Test: Submit insert and verify coordinator processes it ----

  @Test
  public void testInsertIntoOfflineTableReturnsResult()
      throws Exception {
    String tableName = "insertOfflineResult";
    createOfflineTable(tableName);

    String payload = buildInsertRequestJson(tableName, null, null, null);

    JsonNode result = postInsertRequest(payload);
    LOGGER.info("Insert result: {}", result);

    assertNotNull(result.get("statementId"), "statementId should be present");
    String statementId = result.get("statementId").asText();
    assertFalse(statementId.isEmpty(), "statementId should not be empty");

    /// With ROW executor registered, the insert should reach VISIBLE synchronously.
    String state = result.get("state").asText();
    assertEquals(state, "VISIBLE", "ROW executor should return VISIBLE synchronously, got: " + state);
  }

  /// ---- Test: Explicit OFFLINE type targeting works for single-type table ----

  @Test
  public void testInsertWithExplicitOfflineType()
      throws Exception {
    String tableName = "insertExplicitOffline";
    createOfflineTable(tableName);

    /// Submit with explicit tableType=OFFLINE on a table that only has OFFLINE
    String payload = buildInsertRequestJson(tableName, TableType.OFFLINE, null, null);
    JsonNode result = postInsertRequest(payload);
    LOGGER.info("Explicit OFFLINE type result: {}", result);
    /// Assert the request resolves and reaches VISIBLE — pins the explicit-type resolution path.
    assertEquals(result.get("state").asText(), "VISIBLE",
        "Explicit OFFLINE type on an OFFLINE-only table should resolve and produce VISIBLE; got: " + result);
  }

  /// ---- Test: Table does not exist ----

  @Test
  public void testInsertNonExistentTable()
      throws Exception {
    String payload = buildInsertRequestJson("nonExistentTable99", null, null, null);

    JsonNode result = postInsertRequest(payload);
    LOGGER.info("Non-existent table result: {}", result);
    assertEquals(result.get("state").asText(), "REJECTED");
    assertEquals(result.get("errorCode").asText(), "TABLE_RESOLUTION_ERROR");
  }

  /// ---- Test: Insert with wrong explicit table type ----

  @Test
  public void testInsertWithWrongTableType()
      throws Exception {
    String tableName = "insertWrongType";
    createOfflineTable(tableName);

    /// Try to insert with REALTIME type into an OFFLINE-only table
    String payload = buildInsertRequestJson(tableName, TableType.REALTIME, null, null);
    JsonNode result = postInsertRequest(payload);
    LOGGER.info("Wrong type result: {}", result);
    assertEquals(result.get("state").asText(), "REJECTED");
    assertEquals(result.get("errorCode").asText(), "TABLE_RESOLUTION_ERROR");
  }

  /// ---- Test: Insert with explicit table type suffix in name ----

  @Test
  public void testInsertWithExplicitTableTypeSuffix()
      throws Exception {
    String tableName = "insertSuffix";
    createOfflineTable(tableName);

    /// Use explicit _OFFLINE suffix in table name
    String payload = buildInsertRequestJson(tableName + "_OFFLINE", null, null, null);
    JsonNode result = postInsertRequest(payload);
    LOGGER.info("Explicit suffix result: {}", result);
    String state = result.get("state").asText();
    assertEquals(state, "VISIBLE", "ROW executor should return VISIBLE synchronously, got: " + state);
  }

  /// ---- Test: Idempotency — same requestId + payloadHash should return the
  ///      same statementId and not re-execute ----

  @Test
  public void testInsertIdempotency()
      throws Exception {
    String tableName = "insertIdemp";
    createOfflineTable(tableName);

    String requestId = "idemp-001";
    String payloadHash = "hash-abc";

    String payload = buildInsertRequestJson(tableName, null, requestId, payloadHash);

    /// First submit — succeeds with executor registered
    JsonNode result1 = postInsertRequest(payload);
    LOGGER.info("First submit: {}", result1);
    String state1 = result1.get("state").asText();
    assertFalse("REJECTED".equals(state1) && result1.has("errorCode")
        && "NO_EXECUTOR".equals(result1.get("errorCode").asText()),
        "Executor should be registered; should not get NO_EXECUTOR");
    String statementId1 = result1.get("statementId").asText();

    /// Second submit with same requestId + payloadHash — should be idempotent
    JsonNode result2 = postInsertRequest(payload);
    LOGGER.info("Second submit: {}", result2);
    assertEquals(result2.get("statementId").asText(), statementId1,
        "Idempotent retry should return same statementId");
  }

  /// ---- Test: Status API returns NOT_FOUND for unknown statement ----

  @Test
  public void testInsertStatusNotFound()
      throws Exception {
    String tableName = "insertStatusNotFound";
    createOfflineTable(tableName);

    /// The resource maps any state=REJECTED coordinator response (including NOT_FOUND) to HTTP 404
    /// to honor the contract that REJECTED has no manifest persisted. ControllerTest.sendGetRequest
    /// wraps HttpErrorStatusException in an IOException on 4xx, so we catch IOException and unwrap
    /// to check the status code and error message.
    try {
      getInsertStatus("unknown-stmt-id", tableName + "_OFFLINE");
      fail("Expected HTTP 404 for unknown statementId");
    } catch (IOException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof HttpErrorStatusException,
          "Expected cause to be HttpErrorStatusException, got: " + cause);
      HttpErrorStatusException httpEx = (HttpErrorStatusException) cause;
      assertEquals(httpEx.getStatusCode(), 404);
      assertTrue(httpEx.getMessage().contains("errorCode=NOT_FOUND"),
          "Expected message to mention errorCode=NOT_FOUND, got: " + httpEx.getMessage());
    }
  }

  /// ---- Test: Abort returns NOT_FOUND for unknown statement ----

  @Test
  public void testInsertAbortNotFound()
      throws Exception {
    String tableName = "insertAbortNotFound";
    createOfflineTable(tableName);

    JsonNode abortResult = postInsertAbort("unknown-stmt-id", tableName + "_OFFLINE");
    LOGGER.info("Abort not found: {}", abortResult);
    assertEquals(abortResult.get("errorCode").asText(), "NOT_FOUND");
  }

  /// ---- Test: List returns empty array for table with no statements ----

  @Test
  public void testInsertListEmpty()
      throws Exception {
    String tableName = "insertListEmpty";
    createOfflineTable(tableName);

    JsonNode list = getInsertList(tableName + "_OFFLINE");
    LOGGER.info("Empty list: {}", list);
    assertTrue(list.isArray(), "List should return an array");
    assertEquals(list.size(), 0, "Should have 0 statements");
  }

  /// ---- Test: SQL parsing of INSERT INTO VALUES through broker ----

  @Test
  public void testInsertIntoValuesSqlParsing()
      throws Exception {
    String tableName = "insertSqlParse";
    createOfflineTable(tableName);

    /// Submit INSERT INTO VALUES SQL through the broker. Asserts the parser → DML dispatch →
    /// controller coordinator → row executor pipeline end-to-end. A regression in any layer
    /// (grammar, broker SqlQueryExecutor PUSH branch, InsertRequest builder, controller resource)
    /// must fail this test — do NOT swallow exceptions.
    String sql = "INSERT INTO " + tableName
        + " (id, name, score) VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3)";

    /// Use the HTTP endpoint explicitly: the base class's postQuery() randomly toggles between HTTP
    /// and gRPC endpoints, but BrokerGrpcServer routes all queries through the DQL handler (it does
    /// not dispatch on PinotSqlType). DML via gRPC is a known v1 limitation; until the gRPC server
    /// adds a DML branch (mirroring PinotClientRequest.executeSqlQuery), INSERT INTO must go over
    /// HTTP /query/sql.
    JsonNode response = queryBrokerHttpEndpoint(sql);
    LOGGER.info("SQL INSERT response: {}", response);
    assertNotNull(response, "Broker must return a response for INSERT INTO VALUES");
    /// Strict assertion: the result table must include exactly one row whose status column equals
    /// "VISIBLE". A loose `contains("statementId")` would silently accept REJECTED responses since
    /// those also surface a statementId — masking regressions where the executor failed but the
    /// parser succeeded.
    JsonNode resultRows = response.path("resultTable").path("rows");
    assertEquals(resultRows.size(), 1, "Expected exactly one result row; got: " + response);
    String state = resultRows.get(0).get(1).asText();
    assertEquals(state, "VISIBLE",
        "INSERT INTO VALUES must return state=VISIBLE; got state=" + state + " in response: " + response);

    /// Now query the table to confirm the data is actually queryable. A regression that returns
    /// VISIBLE in the JSON but never registers the segment with servers must fail here.
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode countResp = postQuery("SELECT COUNT(*) FROM " + tableName);
        return countResp.get("resultTable").get("rows").get(0).get(0).asInt() == 2;
      } catch (Exception e) {
        return false;
      }
    }, 30_000, "INSERT INTO VALUES rows did not become queryable within 30s");
  }

  /// ---- Test: value-type fidelity for negative numbers, BYTES, and BIG_DECIMAL through the
  ///      broker's JSON wire path ----

  @Test
  public void testInsertNegativeBytesAndBigDecimalThroughBroker()
      throws Exception {
    String tableName = "insertTypedValues";
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(tableName)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("data", DataType.BYTES)
        .addMetric("amount", DataType.BIG_DECIMAL)
        .addMetric("delta", DataType.DOUBLE)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toString(),
        BasicAuthTestUtils.AUTH_HEADER);

    /// The broker serializes rows to JSON for the controller. Without canonical wire encoding,
    /// byte[] becomes Base64 (the controller's STRING→BYTES coercion expects HEX) and BigDecimal
    /// loses precision through Jackson's default double parsing. Negative numbers additionally
    /// exercise the parser's MINUS_PREFIX unwrapping. This test pins all three end to end.
    String sql = "INSERT INTO " + tableName
        + " (id, data, amount, delta) VALUES (-7, X'0BCD', 123456789.123456789, -1.5)";
    JsonNode response = queryBrokerHttpEndpoint(sql);
    LOGGER.info("Typed-values INSERT response: {}", response);
    JsonNode resultRows = response.path("resultTable").path("rows");
    assertEquals(resultRows.size(), 1, "Expected exactly one result row; got: " + response);
    assertEquals(resultRows.get(0).get(1).asText(), "VISIBLE",
        "Typed-values INSERT must return VISIBLE; got: " + response);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode resp = postQuery("SELECT id, data, amount, delta FROM " + tableName + " WHERE id = -7");
        JsonNode rows = resp.path("resultTable").path("rows");
        if (rows.size() != 1) {
          return false;
        }
        JsonNode row = rows.get(0);
        return row.get(0).asInt() == -7
            && "0bcd".equalsIgnoreCase(row.get(1).asText())
            && "123456789.123456789".equals(row.get(2).asText())
            && row.get(3).asDouble() == -1.5;
      } catch (Exception e) {
        return false;
      }
    }, 30_000, "Typed INSERT values (negative INT, BYTES, BIG_DECIMAL, negative DOUBLE) did not round-trip");
  }

  @Test
  public void testRealtimeUploadBesideConsumingSegmentAndHybridTypeSelection()
      throws Exception {
    String tableName = "insertRealtimeHybrid";
    String kafkaTopic = tableName + "-stream";
    String realtimeTable = tableName + "_REALTIME";
    long timestamp = System.currentTimeMillis();
    createRealtimeTable(tableName, kafkaTopic, false);
    pushCsvIntoKafka(List.of("1,stream,10.0," + timestamp), kafkaTopic, null);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT COUNT(*) FROM " + realtimeTable);
        return response.path("resultTable").path("rows").path(0).path(0).asInt() == 1
            && response.path("numConsumingSegmentsQueried").asInt() > 0;
      } catch (Exception e) {
        return false;
      }
    }, 60_000, "The stream row did not become queryable from a consuming segment");

    createOfflineTableVariant(tableName, "ts");
    List<Map<String, Object>> realtimeRows = List.of(
        Map.of("id", 2, "name", "uploaded", "score", 20.0, "ts", timestamp + 1));
    JsonNode ambiguous = postInsertRequest(buildInsertRequestJson(tableName, null, null, null, realtimeRows));
    assertEquals(ambiguous.path("state").asText(), "REJECTED");
    assertEquals(ambiguous.path("errorCode").asText(), "TABLE_RESOLUTION_ERROR");

    JsonNode realtimeInsert = postInsertRequest(
        buildInsertRequestJson(tableName, TableType.REALTIME, null, null, realtimeRows));
    assertEquals(realtimeInsert.path("state").asText(), "VISIBLE", realtimeInsert.toString());
    assertEquals(realtimeInsert.path("segmentNames").size(), 1, realtimeInsert.toString());
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT COUNT(*) FROM " + realtimeTable);
        return response.path("resultTable").path("rows").path(0).path(0).asInt() == 2
            && response.path("numConsumingSegmentsQueried").asInt() > 0
            && response.path("numSegmentsQueried").asInt()
                > response.path("numConsumingSegmentsQueried").asInt();
      } catch (Exception e) {
        return false;
      }
    }, 60_000, "Uploaded REALTIME row was not queryable alongside the consuming row");
    JsonNode uploaded = postQuery("SELECT name FROM " + realtimeTable + " WHERE id = 2");
    assertEquals(uploaded.path("resultTable").path("rows").path(0).path(0).asText(), "uploaded");

    JsonNode offlineInsert = postInsertRequest(buildInsertRequestJson(tableName, TableType.OFFLINE, null, null,
        List.of(Map.of("id", 3, "name", "offline", "score", 30.0, "ts", timestamp + 2))));
    assertEquals(offlineInsert.path("state").asText(), "VISIBLE", offlineInsert.toString());
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT COUNT(*) FROM " + tableName + "_OFFLINE");
        return response.path("resultTable").path("rows").path(0).path(0).asInt() == 1;
      } catch (Exception e) {
        return false;
      }
    }, 60_000, "Explicit OFFLINE insert into the hybrid table was not queryable");
  }

  @Test
  public void testPartitionedFullUpsertUploadedRowWinsOverConsumingRow()
      throws Exception {
    String tableName = "insertFullUpsert";
    String kafkaTopic = tableName + "-stream";
    String realtimeTable = tableName + "_REALTIME";
    long timestamp = System.currentTimeMillis();
    createRealtimeTable(tableName, kafkaTopic, true);
    pushCsvIntoKafka(List.of("1,old,10.0," + timestamp), kafkaTopic, 0);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT name FROM " + realtimeTable + " WHERE id = 1");
        return "old".equals(response.path("resultTable").path("rows").path(0).path(0).asText())
            && response.path("numConsumingSegmentsQueried").asInt() > 0;
      } catch (Exception e) {
        return false;
      }
    }, 60_000, "The old primary-key row did not become queryable from a consuming segment");

    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction("Murmur", getNumKafkaPartitions(), null);
    int otherId = 2;
    while (partitionFunction.getPartition(Integer.toString(otherId)) == partitionFunction.getPartition("1")) {
      otherId++;
    }
    int secondId = otherId;
    List<Map<String, Object>> rows = List.of(
        Map.of("id", 1, "name", "new", "score", 20.0, "ts", timestamp + 1),
        Map.of("id", secondId, "name", "other", "score", 30.0, "ts", timestamp + 1));
    JsonNode insert = postInsertRequest(buildInsertRequestJson(tableName, TableType.REALTIME, null, null, rows));
    assertEquals(insert.path("state").asText(), "VISIBLE", insert.toString());
    assertEquals(insert.path("segmentNames").size(), 2, "Each partition should produce one segment: " + insert);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT id, name, score FROM " + realtimeTable + " ORDER BY id");
        JsonNode actual = response.path("resultTable").path("rows");
        return actual.size() == 2
            && actual.get(0).get(0).asInt() == 1
            && "new".equals(actual.get(0).get(1).asText())
            && actual.get(0).get(2).asDouble() == 20.0
            && actual.get(1).get(0).asInt() == secondId
            && "other".equals(actual.get(1).get(1).asText())
            && response.path("numConsumingSegmentsQueried").asInt() > 0;
      } catch (Exception e) {
        return false;
      }
    }, 60_000, "The newer uploaded primary-key row did not win across both partitions");
  }
}
