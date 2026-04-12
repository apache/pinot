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
import java.util.concurrent.atomic.AtomicReference;
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
import static org.testng.Assert.assertNotNull;

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
  private static final String COUNT_QUERY = String.format("SELECT COUNT(*) FROM %s", TABLE_NAME);
  private static final String RAW_COUNT_QUERY =
      String.format("SELECT COUNT(*) FROM %s OPTION(skipUpsert=true)", TABLE_NAME);
  private static final String ORDERED_ROWS_QUERY =
      String.format("SELECT %s, %s FROM %s ORDER BY %s", UUID_PK_COLUMN, PAYLOAD_COLUMN, TABLE_NAME, UUID_PK_COLUMN);
  private static final String FILTER_QUERY = String.format("SELECT %s FROM %s WHERE %s = TO_UUID('%s')",
      PAYLOAD_COLUMN, TABLE_NAME, UUID_PK_COLUMN, UUID_INPUT_VALUES.get(0));
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
        return queryCountStarWithoutUpsert() == TOTAL_RAW_RECORDS;
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
    UuidQueryResults queryResults = waitForExpectedQueryResults(DEDUPLICATED_RECORDS_READY_TIMEOUT_MS);

    assertExpectedCountResponse(queryResults._countResponse, EXPECTED_UUID_VALUES.size());
    assertExpectedCountResponse(queryResults._rawCountResponse, TOTAL_RAW_RECORDS);
    assertExpectedOrderedRowsResponse(queryResults._orderedRowsResponse);
    assertExpectedFilterResponse(queryResults._filterResponse);
  }

  private static void assertNoExceptions(JsonNode response) {
    assertEquals(getExceptionCount(response), 0, response.toPrettyString());
  }

  private void assertExpectedCountResponse(JsonNode response, long expectedCount) {
    assertNoExceptions(response);
    JsonNode firstRowFirstColumn = getFirstRowFirstColumn(response);
    assertNotNull(firstRowFirstColumn, response.toPrettyString());
    assertEquals(firstRowFirstColumn.asLong(), expectedCount);
  }

  private void assertExpectedOrderedRowsResponse(JsonNode response) {
    assertNoExceptions(response);
    JsonNode rows = getRows(response);
    assertNotNull(rows, response.toPrettyString());
    assertEquals(rows.size(), EXPECTED_UUID_VALUES.size());
    for (int i = 0; i < EXPECTED_UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), EXPECTED_UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(1).asText(), EXPECTED_PAYLOAD_VALUES.get(i));
    }
  }

  private void assertExpectedFilterResponse(JsonNode response) {
    assertNoExceptions(response);
    JsonNode firstRowFirstColumn = getFirstRowFirstColumn(response);
    assertNotNull(firstRowFirstColumn, response.toPrettyString());
    assertEquals(firstRowFirstColumn.asText(), "alpha-v2");
  }

  private UuidQueryResults waitForExpectedQueryResults(long timeoutMs)
      throws Exception {
    AtomicReference<UuidQueryResults> queryResultsRef = new AtomicReference<>();
    TestUtils.waitForCondition(aVoid -> {
      try {
        UuidQueryResults queryResults = fetchQueryResults();
        if (matchesExpectedQueryResults(queryResults)) {
          queryResultsRef.set(queryResults);
          return true;
        }
        return false;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to observe expected UUID upsert query results");
    return queryResultsRef.get();
  }

  private UuidQueryResults fetchQueryResults()
      throws Exception {
    return new UuidQueryResults(postQuery(COUNT_QUERY), postQuery(RAW_COUNT_QUERY), postQuery(ORDERED_ROWS_QUERY),
        postQuery(FILTER_QUERY));
  }

  private boolean matchesExpectedQueryResults(UuidQueryResults queryResults) {
    return hasExpectedCount(queryResults._countResponse, EXPECTED_UUID_VALUES.size())
        && hasExpectedCount(queryResults._rawCountResponse, TOTAL_RAW_RECORDS)
        && hasExpectedOrderedRows(queryResults._orderedRowsResponse)
        && hasExpectedFilterValue(queryResults._filterResponse, "alpha-v2");
  }

  private boolean hasExpectedCount(JsonNode response, long expectedCount) {
    JsonNode firstRowFirstColumn = getFirstRowFirstColumn(response);
    return hasNoExceptions(response) && firstRowFirstColumn != null
        && firstRowFirstColumn.asLong(Long.MIN_VALUE) == expectedCount;
  }

  private boolean hasExpectedOrderedRows(JsonNode response) {
    if (!hasNoExceptions(response)) {
      return false;
    }
    JsonNode rows = getRows(response);
    if (rows == null) {
      return false;
    }
    if (rows.size() != EXPECTED_UUID_VALUES.size()) {
      return false;
    }
    for (int i = 0; i < EXPECTED_UUID_VALUES.size(); i++) {
      JsonNode row = rows.get(i);
      if (row == null || row.size() < 2 || !EXPECTED_UUID_VALUES.get(i).equals(row.get(0).asText())
          || !EXPECTED_PAYLOAD_VALUES.get(i).equals(row.get(1).asText())) {
        return false;
      }
    }
    return true;
  }

  private boolean hasExpectedFilterValue(JsonNode response, String expectedValue) {
    JsonNode firstRowFirstColumn = getFirstRowFirstColumn(response);
    return hasNoExceptions(response) && firstRowFirstColumn != null
        && expectedValue.equals(firstRowFirstColumn.asText());
  }

  private static boolean hasNoExceptions(JsonNode response) {
    return getExceptionCount(response) == 0;
  }

  private static int getExceptionCount(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    return exceptions != null ? exceptions.size() : 0;
  }

  private static JsonNode getRows(JsonNode response) {
    JsonNode resultTable = response.get("resultTable");
    if (resultTable == null) {
      return null;
    }
    return resultTable.get("rows");
  }

  private static JsonNode getFirstRowFirstColumn(JsonNode response) {
    JsonNode rows = getRows(response);
    if (rows == null || rows.isEmpty()) {
      return null;
    }
    JsonNode firstRow = rows.get(0);
    if (firstRow == null || firstRow.isEmpty()) {
      return null;
    }
    return firstRow.get(0);
  }

  private long queryCountStarWithoutUpsert() {
    return getPinotConnection().execute(RAW_COUNT_QUERY).getResultSet(0).getLong(0);
  }

  private static final class UuidQueryResults {
    private final JsonNode _countResponse;
    private final JsonNode _rawCountResponse;
    private final JsonNode _orderedRowsResponse;
    private final JsonNode _filterResponse;

    private UuidQueryResults(JsonNode countResponse, JsonNode rawCountResponse, JsonNode orderedRowsResponse,
        JsonNode filterResponse) {
      _countResponse = countResponse;
      _rawCountResponse = rawCountResponse;
      _orderedRowsResponse = orderedRowsResponse;
      _filterResponse = filterResponse;
    }
  }
}
