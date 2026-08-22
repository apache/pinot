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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Verifies that FULL-upsert visibility is applied before vector top-K candidate selection for both consuming and
/// immutable realtime segments.
@Test(suiteName = "CustomClusterIntegrationTest")
public class VectorUpsertTableTest extends CustomDataQueryClusterIntegrationTest {
  private static final String CONSUMING_TABLE_NAME = "VectorUpsertConsumingTest";
  private static final String SEALED_TABLE_NAME = "VectorUpsertSealedTest";
  private static final String CONSUMING_TOPIC = CONSUMING_TABLE_NAME + "-kafka";
  private static final String SEALED_TOPIC = SEALED_TABLE_NAME + "-kafka";

  private static final String ENTITY_ID_COLUMN = "entityId";
  private static final String TIMESTAMP_COLUMN = "ts";
  private static final String VECTOR_COLUMN = "embedding";
  private static final int NUM_PARTITIONS = 1;
  private static final int NUM_REPLICAS = 1;
  private static final int NUM_CURRENT_RECORDS = 4;
  private static final int NUM_PHYSICAL_RECORDS = 6;
  private static final int TOP_K = 2;
  private static final int REALTIME_TABLE_CONFIG_RETRY_COUNT = 5;
  private static final long REALTIME_TABLE_CONFIG_RETRY_WAIT_MS = 1_000L;
  private static final long KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS = 30_000L;
  private static final long STATE_TIMEOUT_MS = 120_000L;
  private static final long OLD_TIMESTAMP = 1_700_000_000_000L;
  private static final long NEW_TIMESTAMP = OLD_TIMESTAMP + 1_000L;

  private static final TestRecord A1_V1 = new TestRecord("A1", OLD_TIMESTAMP, 1.0f, 0.0f);
  private static final TestRecord A2_V1 = new TestRecord("A2", OLD_TIMESTAMP, 1.0f, 0.0f);
  private static final TestRecord B1 = new TestRecord("B1", OLD_TIMESTAMP, 0.0f, 1.0f);
  private static final TestRecord B2 = new TestRecord("B2", OLD_TIMESTAMP, 0.0f, -1.0f);
  private static final TestRecord A1_V2 = new TestRecord("A1", NEW_TIMESTAMP, -1.0f, 0.0f);
  private static final TestRecord A2_V2 = new TestRecord("A2", NEW_TIMESTAMP, -1.0f, 0.0f);
  private static final List<TestRecord> OLD_A_AND_CURRENT_B = List.of(A1_V1, A2_V1, B1, B2);
  private static final List<TestRecord> NEW_A = List.of(A1_V2, A2_V2);
  private static final List<TestRecord> ALL_RECORDS = List.of(A1_V1, A2_V1, B1, B2, A1_V2, A2_V2);

  private boolean _sealedTableCreated;

  @Override
  public String getTableName() {
    return CONSUMING_TABLE_NAME;
  }

  @Override
  public String getKafkaTopic() {
    return CONSUMING_TOPIC;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_PARTITIONS;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 100;
  }

  @Override
  protected String getPartitionColumn() {
    return ENTITY_ID_COLUMN;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_CURRENT_RECORDS;
  }

  @Override
  public Schema createSchema() {
    return createPinotSchema(CONSUMING_TABLE_NAME);
  }

  @Override
  public List<File> createAvroFiles() {
    // setUpTable() builds three purpose-specific Avro files instead of using the default fixture path.
    return List.of();
  }

  @Override
  protected void setUpTable()
      throws Exception {
    disableMultiStageQueryEngine();

    File allRecordsFile = writeAvroFile("all-records.avro", ALL_RECORDS);
    File oldAndCurrentRecordsFile = writeAvroFile("old-a-and-current-b.avro", OLD_A_AND_CURRENT_B);
    File newARecordsFile = writeAvroFile("new-a.avro", NEW_A);

    addSchema(createPinotSchema(CONSUMING_TABLE_NAME));
    addSchema(createPinotSchema(SEALED_TABLE_NAME));
    createSharedKafkaTopic(CONSUMING_TOPIC, NUM_PARTITIONS);
    createSharedKafkaTopic(SEALED_TOPIC, NUM_PARTITIONS);
    waitForKafkaTopicMetadataReadyForConsumer(CONSUMING_TOPIC);
    waitForKafkaTopicMetadataReadyForConsumer(SEALED_TOPIC);

    // Both tables use the same Avro field schema, so one sample file is sufficient for both decoder instances.
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = allRecordsFile;
    addRealtimeTableConfigWithRetry(createRealtimeUpsertTableConfig(CONSUMING_TABLE_NAME, CONSUMING_TOPIC),
        CONSUMING_TOPIC);
    addRealtimeTableConfigWithRetry(createRealtimeUpsertTableConfig(SEALED_TABLE_NAME, SEALED_TOPIC), SEALED_TOPIC);
    _sealedTableCreated = true;

    pushAvroFiles(CONSUMING_TOPIC, List.of(allRecordsFile));
    pushAvroFiles(SEALED_TOPIC, List.of(oldAndCurrentRecordsFile));

    // The old A versions and current B records must be fully queryable before sealing S0.
    waitForTableState(SEALED_TABLE_NAME, OLD_A_AND_CURRENT_B.size(), OLD_A_AND_CURRENT_B.size(), 1, 1,
        STATE_TIMEOUT_MS);
    forceCommitAndWait(SEALED_TABLE_NAME);
    pushAvroFiles(SEALED_TOPIC, List.of(newARecordsFile));
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    disableMultiStageQueryEngine();
    long effectiveTimeoutMs = Math.max(timeoutMs, STATE_TIMEOUT_MS);
    waitForTableState(CONSUMING_TABLE_NAME, NUM_PHYSICAL_RECORDS, NUM_CURRENT_RECORDS, 1, 1,
        effectiveTimeoutMs);
    waitForTableState(SEALED_TABLE_NAME, NUM_PHYSICAL_RECORDS, NUM_CURRENT_RECORDS, 2, 1, effectiveTimeoutMs);
  }

  @AfterClass
  @Override
  public void tearDown()
      throws IOException {
    try {
      if (_sealedTableCreated) {
        dropRealtimeTable(SEALED_TABLE_NAME);
      }
    } finally {
      super.tearDown();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFullUpsertVectorCandidates(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertVectorResultMatchesCurrentCorpus(CONSUMING_TABLE_NAME, useMultiStageQueryEngine);
    assertVectorResultMatchesCurrentCorpus(SEALED_TABLE_NAME, useMultiStageQueryEngine);
  }

  private void assertVectorResultMatchesCurrentCorpus(String tableName, boolean useMultiStageQueryEngine)
      throws Exception {
    String queryVector = "ARRAY[1.0, 0.0]";
    String vectorQuery = String.format(
        "SELECT %s, cosineDistance(%s, %s) AS distance FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY distance ASC, %s ASC LIMIT %d",
        ENTITY_ID_COLUMN, VECTOR_COLUMN, queryVector, tableName,
        VECTOR_COLUMN, queryVector, TOP_K, ENTITY_ID_COLUMN, TOP_K);
    String exactQuery = String.format(
        "SELECT %s, cosineDistance(%s, %s) AS distance FROM %s "
            + "ORDER BY distance ASC, %s ASC LIMIT %d",
        ENTITY_ID_COLUMN, VECTOR_COLUMN, queryVector, tableName, ENTITY_ID_COLUMN, TOP_K);
    String physicalControlQuery = String.format(
        "SELECT %s, %s, cosineDistance(%s, %s) AS distance FROM %s "
            + "ORDER BY distance ASC, %s ASC LIMIT %d OPTION(skipUpsert=true)",
        ENTITY_ID_COLUMN, TIMESTAMP_COLUMN, VECTOR_COLUMN, queryVector, tableName, ENTITY_ID_COLUMN, TOP_K);
    String explainVectorQuery = tableName.equals(SEALED_TABLE_NAME) ? String.format(
        "SELECT %s, cosineDistance(%s, %s) AS distance FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = %d "
            + "ORDER BY distance ASC, %s ASC LIMIT %d",
        ENTITY_ID_COLUMN, VECTOR_COLUMN, queryVector, tableName,
        VECTOR_COLUMN, queryVector, TOP_K, TIMESTAMP_COLUMN, OLD_TIMESTAMP, ENTITY_ID_COLUMN, TOP_K) : vectorQuery;

    JsonNode vectorResponse = postQuery(vectorQuery);
    JsonNode exactResponse = postQuery(exactQuery);
    JsonNode physicalControlResponse = postQuery(physicalControlQuery);
    JsonNode explainResponse = postQuery("SET explainAskingServers=true; EXPLAIN PLAN FOR " + explainVectorQuery);
    assertNoError(vectorResponse);
    assertNoError(exactResponse);
    assertNoError(physicalControlResponse);
    assertNoError(explainResponse);
    List<String> vectorEntityIds = extractEntityIds(vectorResponse);
    List<String> exactEntityIds = extractEntityIds(exactResponse);

    String engine = useMultiStageQueryEngine ? "multi-stage" : "single-stage";
    assertEquals(vectorEntityIds.size(), TOP_K,
        "Vector query must return exactly K rows for " + tableName + " on the " + engine + " engine");
    assertEquals(vectorEntityIds, List.of("B1", "B2"),
        "Obsolete A versions must not consume vector candidates for " + tableName + " on the " + engine
            + " engine");
    assertEquals(vectorEntityIds, exactEntityIds,
        "Vector and exact scalar ranking must agree for " + tableName + " on the " + engine + " engine");

    JsonNode physicalRows = getRows(physicalControlResponse);
    assertEquals(physicalRows.size(), TOP_K,
        "skipUpsert control must return exactly K physical rows for " + tableName + " on the " + engine + " engine");
    assertEquals(extractEntityIds(physicalControlResponse), List.of("A1", "A2"),
        "The obsolete A versions should be the physically nearest rows for " + tableName + " on the " + engine
            + " engine");
    for (JsonNode row : physicalRows) {
      assertEquals(row.get(1).asLong(), OLD_TIMESTAMP, "skipUpsert must expose the obsolete A version");
      assertEquals(row.get(2).asDouble(), 0.0, 1e-6, "The obsolete A vector must be an exact query match");
    }

    String explain = GroupByOptionsTest.toExplainStr(explainResponse, useMultiStageQueryEngine);
    assertExplainContains(explain, "requiredDocIdFilterApplied", true);
    if (tableName.equals(CONSUMING_TABLE_NAME)) {
      // The mutable vector index cannot restrict its search, so the planner selects the exact scan operator.
      assertTrue(explain.contains("VECTOR_SIMILARITY_EXACT_SCAN") || explain.contains("VectorSimilarityExactScan"),
          "Consuming segments must fall back to the exact scan operator: " + explain);
      assertExplainContains(explain, "fallbackReason", "mutable_vector_index_not_filter_aware");
    } else {
      assertTrue(explain.contains("VECTOR_SIMILARITY_INDEX") || explain.contains("VectorSimilarityIndex"),
          "Sealed segments must use their vector index: " + explain);
      assertExplainContains(explain, "searchMode", "FILTER_THEN_ANN");
    }
  }

  private static void assertExplainContains(String explain, String key, Object value) {
    String legacyStyle = key + ":" + value;
    String multiStageStyle = key + "=[" + value + "]";
    assertTrue(explain.contains(legacyStyle) || explain.contains(multiStageStyle),
        "Explain should contain " + key + '=' + value + ": " + explain);
  }

  private TableConfig createRealtimeUpsertTableConfig(String tableName, String topicName) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setComparisonColumns(List.of(TIMESTAMP_COLUMN));

    Map<String, String> streamConfigs = getStreamConfigMap();
    streamConfigs.put(StreamConfigProperties.constructStreamProperty("kafka",
        StreamConfigProperties.STREAM_TOPIC_NAME), topicName);

    FieldConfig vectorFieldConfig = new FieldConfig.Builder(VECTOR_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
        .withProperties(Map.of(
            "vectorIndexType", "HNSW",
            "vectorDimension", "2",
            "vectorDistanceFunction", "COSINE",
            "version", "1",
            "commitDocs", "1"))
        .build();

    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(tableName)
        .setTimeColumnName(TIMESTAMP_COLUMN)
        .setNumReplicas(NUM_REPLICAS)
        .setFieldConfigList(List.of(vectorFieldConfig))
        .setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null,
            RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(ENTITY_ID_COLUMN, new ColumnPartitionConfig("Murmur", NUM_PARTITIONS))))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(ENTITY_ID_COLUMN, 1))
        .build();
  }

  private void addRealtimeTableConfigWithRetry(TableConfig tableConfig, String topicName)
      throws Exception {
    for (int attempt = 1; attempt <= REALTIME_TABLE_CONFIG_RETRY_COUNT; attempt++) {
      try {
        addTableConfig(tableConfig);
        return;
      } catch (IOException e) {
        if (!isRetryableRealtimePartitionMetadataError(e, topicName)
            || attempt == REALTIME_TABLE_CONFIG_RETRY_COUNT) {
          throw e;
        }
        LOGGER.warn("Retrying realtime table creation for topic {} after metadata propagation failure "
                + "(attempt {}/{})", topicName, attempt, REALTIME_TABLE_CONFIG_RETRY_COUNT, e);
        waitForKafkaTopicMetadataReadyForConsumer(topicName);
        Thread.sleep(REALTIME_TABLE_CONFIG_RETRY_WAIT_MS);
      }
    }
    throw new IllegalStateException("Failed to create realtime table after retries for topic: " + topicName);
  }

  private static boolean isRetryableRealtimePartitionMetadataError(Throwable throwable, String topicName) {
    String errorToken = "Failed to fetch partition information for topic: " + topicName;
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains(errorToken)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void waitForKafkaTopicMetadataReadyForConsumer(String topicName) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(topicName), 200L,
        KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka topic '" + topicName + "' metadata is not visible to consumers in custom cluster suite");
  }

  private boolean isKafkaTopicMetadataReadyForConsumer(String topicName) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getSharedKafkaBrokerList());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pinot-vector-upsert-topic-ready-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName, Duration.ofSeconds(5));
      return partitionInfos != null && partitionInfos.size() >= NUM_PARTITIONS;
    } catch (Exception e) {
      return false;
    }
  }

  private void pushAvroFiles(String topicName, List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles, getSharedKafkaBrokerList(), topicName,
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), ENTITY_ID_COLUMN, false);
  }

  private void forceCommitAndWait(String tableName)
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();

    TestUtils.waitForCondition(aVoid -> {
      try {
        String status = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
        return JsonUtils.stringToJsonNode(status)
            .path(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1) == 0;
      } catch (Exception e) {
        return false;
      }
    }, 200L, STATE_TIMEOUT_MS, "Timed out waiting for force-commit job: " + jobId);
  }

  private void waitForTableState(String tableName, int expectedPhysicalDocs, int expectedCurrentDocs,
      int expectedSegments, int expectedConsumingSegments, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        if (!areReplicasReady(tableName, expectedSegments)) {
          return false;
        }
        // Query repeatedly after verifying the table data manager so document counts and segment state must remain
        // stable across broker requests.
        for (int i = 0; i < 10; i++) {
          ResultSetGroup physicalResult = getPinotConnection().execute(
              "SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)");
          ResultSetGroup currentResult = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName);
          if (physicalResult.getResultSet(0).getLong(0) != expectedPhysicalDocs
              || currentResult.getResultSet(0).getLong(0) != expectedCurrentDocs) {
            return false;
          }
          ExecutionStats executionStats = physicalResult.getExecutionStats();
          if (executionStats.getNumSegmentsQueried() != expectedSegments
              || executionStats.getNumConsumingSegmentsQueried() != expectedConsumingSegments) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 200L, timeoutMs,
        String.format("Timed out waiting for %s: physicalDocs=%d, currentDocs=%d, segments=%d, consumingSegments=%d",
            tableName, expectedPhysicalDocs, expectedCurrentDocs, expectedSegments, expectedConsumingSegments));
  }

  private boolean areReplicasReady(String tableName, int expectedSegments) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    int readyReplicas = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        continue;
      }
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        if (segmentDataManagers.size() == expectedSegments) {
          readyReplicas++;
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return readyReplicas == NUM_REPLICAS;
  }

  private File writeAvroFile(String fileName, List<TestRecord> records)
      throws IOException {
    org.apache.avro.Schema avroSchema = createAvroSchema();
    File avroFile = new File(_tempDir, fileName);
    try (DataFileWriter<GenericData.Record> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (TestRecord testRecord : records) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ENTITY_ID_COLUMN, testRecord._entityId);
        record.put(TIMESTAMP_COLUMN, testRecord._timestamp);
        record.put(VECTOR_COLUMN, List.of(testRecord._vectorX, testRecord._vectorY));
        writer.append(record);
      }
    }
    return avroFile;
  }

  private static Schema createPinotSchema(String tableName) {
    return new Schema.SchemaBuilder()
        .setSchemaName(tableName)
        .addSingleValueDimension(ENTITY_ID_COLUMN, FieldSpec.DataType.STRING)
        .addDateTime(TIMESTAMP_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addMultiValueDimension(VECTOR_COLUMN, FieldSpec.DataType.FLOAT)
        .setPrimaryKeyColumns(List.of(ENTITY_ID_COLUMN))
        .build();
  }

  private static org.apache.avro.Schema createAvroSchema() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord("VectorUpsertRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ENTITY_ID_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(VECTOR_COLUMN,
            org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT)),
            null, null)));
    return avroSchema;
  }

  private static List<String> extractEntityIds(JsonNode response) {
    JsonNode rows = getRows(response);
    List<String> entityIds = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
      entityIds.add(row.get(0).asText());
    }
    return entityIds;
  }

  private static JsonNode getRows(JsonNode response) {
    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "Query failed: " + response.toPrettyString());
    return resultTable.get("rows");
  }

  private static final class TestRecord {
    private final String _entityId;
    private final long _timestamp;
    private final float _vectorX;
    private final float _vectorY;

    private TestRecord(String entityId, long timestamp, float vectorX, float vectorY) {
      _entityId = entityId;
      _timestamp = timestamp;
      _vectorX = vectorX;
      _vectorY = vectorY;
    }
  }
}
