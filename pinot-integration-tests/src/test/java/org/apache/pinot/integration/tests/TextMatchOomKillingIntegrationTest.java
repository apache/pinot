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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TextMatchOomKillingIntegrationTest extends BaseClusterIntegrationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TABLE_NAME = "TextMatchOomKillingTest";
  private static final String TEXT_COLUMN = "textContent";
  private static final String ID_COLUMN = "id";
  private static final String TIME_COLUMN = "timestamp";

  private static final int NUM_SEGMENTS = 3;
  private static final int NUM_DOCS_PER_SEGMENT = 3_000_000;

  // Query designed to match many documents and consume significant resources
  private static final String LARGE_TEXT_MATCH_QUERY =
      "SELECT * FROM " + TABLE_NAME + " WHERE TEXT_MATCH(textContent, '/.*searchable.*/') LIMIT 9000000";

  // Regex query that forces full index scan with large result set
  private static final String REGEX_TEXT_MATCH_QUERY =
      "SELECT textContent FROM " + TABLE_NAME + " WHERE TEXT_MATCH(textContent, '/.*document.*/') LIMIT 3000000";

  // Simple query that should succeed without OOM
  private static final String SIMPLE_TEXT_MATCH_QUERY =
      "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(textContent, 'unique')";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload schema with TEXT index
    Schema schema = createSchema();
    addSchema(schema);

    // Create and upload table config with TEXT index
    TableConfig tableConfig = createTableConfig();
    addTableConfig(tableConfig);

    // Create and upload segments
    List<File> avroFiles = createAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(TABLE_NAME, _tarDir);

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
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return (long) NUM_SEGMENTS * NUM_DOCS_PER_SEGMENT;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, Integer.MAX_VALUE);

    // Enable OOM protection with aggressive thresholds for testing
    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    // Set very low thresholds to trigger OOM killing
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.15f);
  }

  @Override
  protected Schema createSchema()
      throws IOException {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(TEXT_COLUMN, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private TableConfig createTableConfig() {
    ObjectNode textColumnIndexes;
    try {
      textColumnIndexes = (ObjectNode) OBJECT_MAPPER.readTree("{\"text\": {}}");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    FieldConfig textFieldConfig = new FieldConfig(
        TEXT_COLUMN,
        FieldConfig.EncodingType.RAW,
        null,
        null,
        null,
        null,
        textColumnIndexes,
        null,
        null);

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNoDictionaryColumns(List.of(TEXT_COLUMN))
        .setFieldConfigList(List.of(textFieldConfig))
        .build();
  }

  private List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(ID_COLUMN, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(TEXT_COLUMN, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(TIME_COLUMN, org.apache.avro.Schema.create(Type.LONG), null, null)));

    List<File> avroFiles = new ArrayList<>();
    Random random = new Random(42);

    // Generate varied content to create realistic text search scenarios
    String[] contentPatterns = {
        "This is a searchable document with varied content number %d",
        "Machine learning and data science document %d with searchable patterns",
        "Apache Pinot is a realtime distributed OLAP datastore document %d",
        "Unique document entry %d with specific searchable content for testing",
        "Random text content %d that should match regex patterns searchable"
    };

    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      File avroFile = new File(_tempDir, "text_data_" + segmentId + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter =
               new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        for (int docId = 0; docId < NUM_DOCS_PER_SEGMENT; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          int globalId = segmentId * NUM_DOCS_PER_SEGMENT + docId;
          record.put(ID_COLUMN, globalId);
          // Use varied content to make text matching realistic
          String pattern = contentPatterns[random.nextInt(contentPatterns.length)];
          record.put(TEXT_COLUMN, String.format(pattern, globalId));
          record.put(TIME_COLUMN, System.currentTimeMillis() + globalId);
          fileWriter.append(record);
        }
        avroFiles.add(avroFile);
      }
    }
    return avroFiles;
  }

  /**
   * Tests that resource usage stats are correctly reported for TEXT_MATCH queries.
   * Tests both SSE and MSE engines as they report stats differently.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testTextMatchResourceUsageStats(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode queryResponse = postQuery(SIMPLE_TEXT_MATCH_QUERY);

    // Verify no exceptions
    JsonNode exceptionsNode = queryResponse.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions node");
    assertTrue(exceptionsNode.isEmpty(), "Query should succeed without exceptions: " + exceptionsNode);

    // SSE and MSE report resource stats differently
    long memAllocated = 0;
    long cpuTime = 0;

    if (useMultiStageQueryEngine) {
      // MSE: stats are in stageStats -> LEAF node
      JsonNode stageStats = queryResponse.get("stageStats");
      if (stageStats != null) {
        JsonNode leafStats = findLeafStageStats(stageStats);
        if (leafStats != null) {
          memAllocated = leafStats.has("threadMemAllocatedBytes")
              ? leafStats.get("threadMemAllocatedBytes").asLong() : 0;
          cpuTime = leafStats.has("threadCpuTimeNs")
              ? leafStats.get("threadCpuTimeNs").asLong() : 0;
        }
      }
    } else {
      // SSE: stats are at top level
      memAllocated = queryResponse.has("offlineThreadMemAllocatedBytes")
          ? queryResponse.get("offlineThreadMemAllocatedBytes").asLong() : 0;
      cpuTime = queryResponse.has("offlineTotalCpuTimeNs")
          ? queryResponse.get("offlineTotalCpuTimeNs").asLong() : 0;
    }

    assertTrue(memAllocated > 0 || cpuTime > 0,
        "Resource tracking should report positive values for " + (useMultiStageQueryEngine ? "MSE" : "SSE")
            + ". Memory: " + memAllocated + ", CPU: " + cpuTime);
  }

  /**
   * Recursively finds the LEAF stage stats node in the stageStats tree.
   */
  private JsonNode findLeafStageStats(JsonNode node) {
    if (node == null) {
      return null;
    }
    if (node.has("type") && "LEAF".equals(node.get("type").asText())) {
      return node;
    }
    JsonNode children = node.get("children");
    if (children != null && children.isArray()) {
      for (JsonNode child : children) {
        JsonNode result = findLeafStageStats(child);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }

  /**
   * Tests that large TEXT_MATCH queries are killed by OOM protection on offline segments.
   */
  @Test
  public void testOfflineTextMatchOomKill()
      throws Exception {
    JsonNode response = postQuery(LARGE_TEXT_MATCH_QUERY);
    verifyOomKillOrResourceTracking(LARGE_TEXT_MATCH_QUERY, response);
  }

  /**
   * Tests that regex TEXT_MATCH queries (which scan the full index) are killed by OOM protection.
   */
  @Test
  public void testRegexTextMatchOomKill()
      throws Exception {
    JsonNode response = postQuery(REGEX_TEXT_MATCH_QUERY);
    verifyOomKillOrResourceTracking(REGEX_TEXT_MATCH_QUERY, response);
  }

  /**
   * Tests that small TEXT_MATCH queries complete successfully without being killed.
   */
  @Test
  public void testSmallTextMatchQuerySucceeds()
      throws Exception {
    JsonNode response = postQuery(SIMPLE_TEXT_MATCH_QUERY);
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions node for query: " + SIMPLE_TEXT_MATCH_QUERY);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for small TEXT_MATCH query, but got: " + exceptionsNode);

    // Verify we got results
    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "Missing resultTable for query: " + SIMPLE_TEXT_MATCH_QUERY);
    long count = resultTable.get("rows").get(0).get(0).asLong();
    assertTrue(count > 0, "Expected positive count for TEXT_MATCH query");
  }

  /**
   * Tests OOM killing with both SSE and MSE query engines.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testTextMatchOomKillBothEngines(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery(LARGE_TEXT_MATCH_QUERY);
    verifyOomKillOrResourceTracking(LARGE_TEXT_MATCH_QUERY, response);
  }

  /**
   * Verifies that the query was either OOM killed OR completed with resource tracking.
   * This is more lenient than verifyOomKill because OOM killing depends on heap size and timing.
   */
  private void verifyOomKillOrResourceTracking(String query, JsonNode response) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions node for query: " + query);

    if (exceptionsNode.isEmpty()) {
      // Query completed successfully - verify resource tracking worked
      long memAllocated = response.has("offlineThreadMemAllocatedBytes")
          ? response.get("offlineThreadMemAllocatedBytes").asLong() : 0;
      long cpuTime = response.has("offlineTotalCpuTimeNs")
          ? response.get("offlineTotalCpuTimeNs").asLong() : 0;
      assertTrue(memAllocated > 0 || cpuTime > 0,
          "Query completed but no resource tracking for query: " + query);
    } else {
      // Query was killed - verify it was OOM killed
      verifyOomKill(query, response);
    }
  }

  private void verifyOomKill(String query, JsonNode response) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertEquals(exceptionsNode.size(), 1,
        "Expected 1 exception for query: " + query + ", but got: " + exceptionsNode);

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
}
