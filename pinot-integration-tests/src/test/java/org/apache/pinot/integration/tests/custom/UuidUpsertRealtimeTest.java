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
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.integration.tests.ClusterTest.AvroFileSchemaKafkaAvroMessageDecoder;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;

/**
 * Realtime upsert coverage for UUID primary keys.
 *
 * <p>This test uses a single Kafka partition because mixed-case UUID spellings normalize only after Pinot ingests the
 * row. Keeping the stream single-partitioned ensures logically equivalent UUID keys remain colocated before that
 * canonicalization step.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidUpsertRealtimeTest extends CustomDataQueryClusterIntegrationTest {
  private static final long DEDUPLICATED_RECORDS_READY_TIMEOUT_MS = 120_000L;
  private static final String TABLE_NAME = "UuidUpsertRealtimeTest";
  private static final String UUID_PK_COLUMN = "uuidPk";
  private static final String PAYLOAD_COLUMN = "payload";
  private static final String TIME_COLUMN = "ts";
  private static final List<String> UUID_INPUT_VALUES = List.of(
      "550E8400-E29B-41D4-A716-446655440000",
      "550E8400-E29B-41D4-A716-446655440001",
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440002",
      "550e8400-e29b-41d4-a716-446655440001");
  private static final List<String> PAYLOAD_VALUES = List.of("alpha-v1", "beta-v1", "alpha-v2", "gamma-v1",
      "beta-v2");
  private static final List<String> EXPECTED_UUID_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002");
  private static final List<String> EXPECTED_PAYLOAD_VALUES = List.of("alpha-v2", "beta-v2", "gamma-v1");
  private static final int TOTAL_RAW_RECORDS = UUID_INPUT_VALUES.size();
  private static final long BASE_TIMESTAMP_MILLIS = 1_700_100_000_000L;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Override
  protected UpsertConfig getUpsertConfig() {
    return new UpsertConfig(UpsertConfig.Mode.FULL);
  }

  @Override
  protected RoutingConfig getRoutingConfig() {
    return new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
  }

  @Override
  protected long getCountStarResult() {
    return EXPECTED_UUID_VALUES.size();
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Map.of(UUID_PK_COLUMN, new ColumnPartitionConfig("Murmur", getNumKafkaPartitions())));
    return getTableConfigBuilder(TableType.REALTIME)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(UUID_PK_COLUMN, 1))
        .build();
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return hasConsistentCount(String.format("SELECT COUNT(*) FROM %s OPTION(skipUpsert=true)", getTableName()),
            TOTAL_RAW_RECORDS);
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load raw UUID upsert records");
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(UUID_PK_COLUMN, FieldSpec.DataType.UUID)
        .addSingleValueDimension(PAYLOAD_COLUMN, FieldSpec.DataType.STRING)
        .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(UUID_PK_COLUMN))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidUpsertRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(UUID_PK_COLUMN, create(Type.STRING), null, null),
        new Field(PAYLOAD_COLUMN, create(Type.STRING), null, null),
        new Field(TIME_COLUMN, create(Type.LONG), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < UUID_INPUT_VALUES.size(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(UUID_PK_COLUMN, UUID_INPUT_VALUES.get(i));
        record.put(PAYLOAD_COLUMN, PAYLOAD_VALUES.get(i));
        record.put(TIME_COLUMN, BASE_TIMESTAMP_MILLIS + i);
        writers.get(0).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUuidPrimaryKeyUpsertDedup(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    waitForDeduplicatedDocsLoaded(DEDUPLICATED_RECORDS_READY_TIMEOUT_MS);

    JsonNode response = postQuery(String.format("SELECT COUNT(*) FROM %s", getTableName()));
    assertNoExceptions(response);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), EXPECTED_UUID_VALUES.size());

    response = postQuery(String.format("SELECT COUNT(*) FROM %s OPTION(skipUpsert=true)", getTableName()));
    assertNoExceptions(response);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), TOTAL_RAW_RECORDS);

    response = postQuery(String.format("SELECT %s, %s FROM %s ORDER BY %s", UUID_PK_COLUMN, PAYLOAD_COLUMN,
        getTableName(), UUID_PK_COLUMN));
    assertNoExceptions(response);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), EXPECTED_UUID_VALUES.size());
    for (int i = 0; i < EXPECTED_UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), EXPECTED_UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(1).asText(), EXPECTED_PAYLOAD_VALUES.get(i));
    }

    response = postQuery(String.format("SELECT %s FROM %s WHERE %s = TO_UUID('%s')", PAYLOAD_COLUMN, getTableName(),
        UUID_PK_COLUMN, UUID_INPUT_VALUES.get(0)));
    assertNoExceptions(response);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asText(), "alpha-v2");
  }

  private static void assertNoExceptions(JsonNode response) {
    assertEquals(response.get("exceptions").size(), 0, response.toPrettyString());
  }

  private void waitForDeduplicatedDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return hasConsistentCount(String.format("SELECT COUNT(*) FROM %s", getTableName()), getCountStarResult());
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to converge deduplicated UUID upsert record counts");
  }

  private boolean hasConsistentCount(String query, long expectedCount) {
    for (int i = 0; i < 10; i++) {
      long count = Long.parseLong(getPinotConnection().execute(query).getResultSet(0).getString(0));
      if (count != expectedCount) {
        return false;
      }
    }
    return true;
  }
}
