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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "UuidTypeTest";
  private static final String ID_COLUMN = "id";
  private static final String UUID_FROM_STRING_COLUMN = "uuidFromString";
  private static final String UUID_FROM_BYTES_COLUMN = "uuidFromBytes";
  private static final String UUID_AS_BYTES_COLUMN = "uuidAsBytes";
  private static final String UUID_ARRAY_FROM_STRING_COLUMN = "uuidArrayFromString";
  private static final String UUID_ARRAY_FROM_BYTES_COLUMN = "uuidArrayFromBytes";
  private static final String TIME_COLUMN = "ts";
  private static final List<String> UUID_INPUT_VALUES = List.of(
      "550E8400-E29B-41D4-A716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-E29B-41D4-A716-446655440000",
      "550e8400-e29b-41d4-a716-446655440002",
      "550E8400-E29B-41D4-A716-446655440001",
      "550e8400-e29b-41d4-a716-446655440003");
  private static final List<String> UUID_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440002",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440003");
  private static final List<String> DISTINCT_UUID_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002",
      "550e8400-e29b-41d4-a716-446655440003");
  private static final List<Integer> SORTED_UUID_IDS = List.of(0, 2, 1, 4, 3, 5);
  private static final long BASE_TIMESTAMP_MILLIS = 1_700_000_000_000L;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Override
  protected long getCountStarResult() {
    return UUID_VALUES.size();
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
  protected List<String> getInvertedIndexColumns() {
    return List.of(UUID_FROM_STRING_COLUMN);
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of(UUID_FROM_BYTES_COLUMN, UUID_AS_BYTES_COLUMN, UUID_ARRAY_FROM_BYTES_COLUMN);
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return List.of(UUID_FROM_STRING_COLUMN, UUID_FROM_BYTES_COLUMN, UUID_AS_BYTES_COLUMN);
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(UUID_FROM_STRING_COLUMN, FieldSpec.DataType.UUID)
        .addSingleValueDimension(UUID_FROM_BYTES_COLUMN, FieldSpec.DataType.UUID)
        .addSingleValueDimension(UUID_AS_BYTES_COLUMN, FieldSpec.DataType.BYTES)
        .addMultiValueDimension(UUID_ARRAY_FROM_STRING_COLUMN, FieldSpec.DataType.UUID)
        .addMultiValueDimension(UUID_ARRAY_FROM_BYTES_COLUMN, FieldSpec.DataType.UUID)
        .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    org.apache.avro.Schema uuidStringArraySchema = org.apache.avro.Schema.createArray(create(Type.STRING));
    org.apache.avro.Schema uuidBytesArraySchema = org.apache.avro.Schema.createArray(create(Type.BYTES));
    avroSchema.setFields(List.of(
        new Field(ID_COLUMN, create(Type.INT), null, null),
        new Field(UUID_FROM_STRING_COLUMN, create(Type.STRING), null, null),
        new Field(UUID_FROM_BYTES_COLUMN, create(Type.BYTES), null, null),
        new Field(UUID_AS_BYTES_COLUMN, create(Type.BYTES), null, null),
        new Field(UUID_ARRAY_FROM_STRING_COLUMN, uuidStringArraySchema, null, null),
        new Field(UUID_ARRAY_FROM_BYTES_COLUMN, uuidBytesArraySchema, null, null),
        new Field(TIME_COLUMN, create(Type.LONG), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < UUID_VALUES.size(); i++) {
        String uuidInputValue = UUID_INPUT_VALUES.get(i);
        byte[] uuidBytes = UuidUtils.toBytes(uuidInputValue);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, i);
        record.put(UUID_FROM_STRING_COLUMN, uuidInputValue);
        record.put(UUID_FROM_BYTES_COLUMN, ByteBuffer.wrap(uuidBytes));
        record.put(UUID_AS_BYTES_COLUMN, ByteBuffer.wrap(uuidBytes));
        record.put(UUID_ARRAY_FROM_STRING_COLUMN, uuidStringArray(i, uuidStringArraySchema));
        record.put(UUID_ARRAY_FROM_BYTES_COLUMN, uuidBytesArray(i, uuidBytesArraySchema));
        record.put(TIME_COLUMN, BASE_TIMESTAMP_MILLIS + i);
        writers.get(0).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectAndPredicateQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, %s, %s, %s FROM %s ORDER BY %s", ID_COLUMN, UUID_FROM_STRING_COLUMN, UUID_FROM_BYTES_COLUMN,
        UUID_AS_BYTES_COLUMN, getTableName(), ID_COLUMN));

    assertEquals(rows.size(), UUID_VALUES.size());
    for (int i = 0; i < UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asInt(), i);
      assertEquals(rows.get(i).get(1).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(2).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(3).asText(), uuidHex(UUID_VALUES.get(i)));
    }

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE %s = CAST('%s' AS UUID) ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, UUID_INPUT_VALUES.get(1).toUpperCase(), ID_COLUMN));
    assertIdRows(rows, 1, 4);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE %s IN (CAST('%s' AS UUID), CAST('%s' AS UUID)) ORDER BY %s", ID_COLUMN,
        getTableName(), UUID_FROM_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(0).toUpperCase(), DISTINCT_UUID_VALUES.get(2),
        ID_COLUMN));
    assertIdRows(rows, 0, 2, 3);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE %s = TO_UUID('%s') ORDER BY %s", ID_COLUMN, getTableName(), UUID_FROM_STRING_COLUMN,
        DISTINCT_UUID_VALUES.get(3).toUpperCase(), ID_COLUMN));
    assertIdRows(rows, 5);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE %s = UUID_TO_BYTES(CAST('%s' AS UUID)) ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_AS_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(1), ID_COLUMN));
    assertIdRows(rows, 1, 4);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE CAST(%s AS UUID) = TO_UUID('%s') ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_AS_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(2).toUpperCase(), ID_COLUMN));
    assertIdRows(rows, 3);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s FROM %s WHERE %s NOT IN (CAST('%s' AS UUID), CAST('%s' AS UUID)) ORDER BY %s", ID_COLUMN,
        getTableName(), UUID_FROM_STRING_COLUMN, DISTINCT_UUID_VALUES.get(0), DISTINCT_UUID_VALUES.get(3), ID_COLUMN));
    assertIdRows(rows, 1, 3, 4);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUuidArrayProjection(boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, %s, %s, arrayLength(%s), arrayLength(%s) FROM %s ORDER BY %s", ID_COLUMN,
        UUID_ARRAY_FROM_STRING_COLUMN, UUID_ARRAY_FROM_BYTES_COLUMN, UUID_ARRAY_FROM_STRING_COLUMN,
        UUID_ARRAY_FROM_BYTES_COLUMN, getTableName(), ID_COLUMN));

    assertEquals(rows.size(), UUID_VALUES.size());
    for (int i = 0; i < UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asInt(), i);
      assertUuidArray(rows.get(i).get(1), i);
      assertUuidArray(rows.get(i).get(2), i);
      assertEquals(rows.get(i).get(3).asInt(), 2);
      assertEquals(rows.get(i).get(4).asInt(), 2);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUuidFunctionsAndCaseQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT UUID_TO_STRING(TO_UUID(UUID_TO_STRING(%s))), UUID_TO_STRING(TO_UUID(%s)), "
            + "UUID_TO_STRING(BYTES_TO_UUID(%s)), UUID_TO_BYTES(%s), "
            + "IS_UUID(UUID_TO_STRING(%s)), IS_UUID(%s), IS_UUID('not-a-uuid') "
            + "FROM %s ORDER BY %s", UUID_FROM_STRING_COLUMN, UUID_AS_BYTES_COLUMN, UUID_FROM_BYTES_COLUMN,
        UUID_FROM_BYTES_COLUMN, UUID_FROM_STRING_COLUMN, UUID_AS_BYTES_COLUMN, getTableName(), ID_COLUMN));

    assertEquals(rows.size(), UUID_VALUES.size());
    for (int i = 0; i < UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(1).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(2).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(3).asText(), uuidHex(UUID_VALUES.get(i)));
      assertEquals(rows.get(i).get(4).asBoolean(), true);
      assertEquals(rows.get(i).get(5).asBoolean(), true);
      assertEquals(rows.get(i).get(6).asBoolean(), false);
    }

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT CAST('%s' AS UUID) FROM %s ORDER BY %s LIMIT 1", DISTINCT_UUID_VALUES.get(0).toUpperCase(),
        getTableName(), ID_COLUMN));
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), DISTINCT_UUID_VALUES.get(0));

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT CASE WHEN %s < 2 THEN CAST('%s' AS UUID) ELSE BYTES_TO_UUID(%s) END "
            + "FROM %s ORDER BY %s", ID_COLUMN, DISTINCT_UUID_VALUES.get(3).toUpperCase(), UUID_AS_BYTES_COLUMN,
        getTableName(), ID_COLUMN));
    assertEquals(rows.size(), UUID_VALUES.size());
    assertEquals(rows.get(0).get(0).asText(), DISTINCT_UUID_VALUES.get(3));
    assertEquals(rows.get(1).get(0).asText(), DISTINCT_UUID_VALUES.get(3));
    for (int i = 2; i < UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), UUID_VALUES.get(i));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByDistinctOrderByAndParity(boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", UUID_FROM_STRING_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, UUID_FROM_STRING_COLUMN));
    assertGroupedCounts(rows);

    JsonNode rawRows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", UUID_FROM_BYTES_COLUMN, getTableName(),
        UUID_FROM_BYTES_COLUMN, UUID_FROM_BYTES_COLUMN));
    assertEquals(rawRows, rows);

    rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT DISTINCT BYTES_TO_UUID(%s) FROM %s ORDER BY BYTES_TO_UUID(%s)", UUID_AS_BYTES_COLUMN, getTableName(),
        UUID_AS_BYTES_COLUMN));
    assertDistinctRows(rows);

    JsonNode dictRows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, %s FROM %s ORDER BY %s, %s", UUID_FROM_STRING_COLUMN, ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, ID_COLUMN));
    JsonNode rawOrderRows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT %s, %s FROM %s ORDER BY %s, %s", UUID_FROM_BYTES_COLUMN, ID_COLUMN, getTableName(),
        UUID_FROM_BYTES_COLUMN, ID_COLUMN));
    assertEquals(rawOrderRows, dictRows);

    for (int i = 0; i < SORTED_UUID_IDS.size(); i++) {
      int expectedId = SORTED_UUID_IDS.get(i);
      assertEquals(dictRows.get(i).get(0).asText(), UUID_VALUES.get(expectedId));
      assertEquals(dictRows.get(i).get(1).asInt(), expectedId);
    }
  }

  /**
   * Tests range predicates on UUID columns (both dictionary-backed and no-dictionary).
   *
   * <p>UUID byte order determines sort order: the last segment of each test UUID differs only in the final byte
   * (00–03), so the four distinct values have a well-defined byte-ordered range.
   *
   * <p>Only the single-stage engine is tested here; MSQE range pushdown for UUID columns is verified separately
   * through the SSE/MSE parity checks in {@link #testSseMseParity}.
   */
  @Test
  public void testRangePredicates()
      throws Exception {
    // uuidFromString is dictionary-backed; uuidFromBytes is no-dictionary (raw bytes)
    // Sorted distinct values: 440000 < 440001 < 440002 < 440003

    // GT on dictionary-backed UUID column: > 440001 → values 440002 (id=3), 440003 (id=5)
    JsonNode rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s > '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, DISTINCT_UUID_VALUES.get(1), ID_COLUMN));
    assertIdRows(rows, 3, 5);

    // LT on dictionary-backed UUID column: < 440002 → values 440000 (ids=0,2), 440001 (ids=1,4)
    rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s < '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, DISTINCT_UUID_VALUES.get(2), ID_COLUMN));
    assertIdRows(rows, 0, 1, 2, 4);

    // BETWEEN on no-dictionary UUID column: 440001–440002 inclusive → ids 1, 3, 4
    rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s BETWEEN '%s' AND '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(1), DISTINCT_UUID_VALUES.get(2), ID_COLUMN));
    assertIdRows(rows, 1, 3, 4);

    // GTE on no-dictionary UUID column: >= 440002 → values 440002 (id=3), 440003 (id=5)
    rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s >= '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(2), ID_COLUMN));
    assertIdRows(rows, 3, 5);

    // LTE on no-dictionary UUID column: <= 440001 → values 440000 (ids=0,2), 440001 (ids=1,4)
    rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s <= '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(1), ID_COLUMN));
    assertIdRows(rows, 0, 1, 2, 4);

    // Mixed-case UUID bounds are normalized: upper-case variant of 440001
    rows = postRows(false, String.format(
        "SELECT %s FROM %s WHERE %s > '%s' ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, DISTINCT_UUID_VALUES.get(1).toUpperCase(), ID_COLUMN));
    assertIdRows(rows, 3, 5);
  }

  @Test
  public void testSseMseParity()
      throws Exception {
    List<String> queries = List.of(
        String.format("SELECT %s, UUID_TO_STRING(BYTES_TO_UUID(%s)) AS uuidText FROM %s ORDER BY %s",
            UUID_FROM_STRING_COLUMN, UUID_AS_BYTES_COLUMN, getTableName(), ID_COLUMN),
        String.format("SELECT COUNT(*) AS cnt FROM %s WHERE %s = TO_UUID('%s')", getTableName(),
            UUID_FROM_STRING_COLUMN, DISTINCT_UUID_VALUES.get(1).toUpperCase()),
        String.format("SELECT BYTES_TO_UUID(%s) AS uuidValue, COUNT(*) AS cnt FROM %s "
                + "GROUP BY BYTES_TO_UUID(%s) ORDER BY uuidValue",
            UUID_AS_BYTES_COLUMN, getTableName(), UUID_AS_BYTES_COLUMN));

    for (String query : queries) {
      JsonNode sseResult = postResultTable(false, query);
      JsonNode mseResult = postResultTable(true, query);
      assertEquals(mseResult, sseResult, query);
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testUuidEqualityJoinWithExpressionKey(boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode rows = postRows(useMultiStageQueryEngine, String.format(
        "SELECT a.%s, b.%s, a.%s "
            + "FROM (SELECT %s, %s FROM %s WHERE %s < 5) a "
            + "JOIN (SELECT %s, %s, %s FROM %s WHERE %s > 0) b "
            + "ON a.%s = BYTES_TO_UUID(UUID_TO_BYTES(b.%s)) "
            + "WHERE a.%s < b.%s "
            + "ORDER BY a.%s, b.%s",
        ID_COLUMN, ID_COLUMN, UUID_FROM_STRING_COLUMN, ID_COLUMN, UUID_FROM_STRING_COLUMN, getTableName(), ID_COLUMN,
        ID_COLUMN, UUID_FROM_BYTES_COLUMN, UUID_AS_BYTES_COLUMN, getTableName(), ID_COLUMN, UUID_FROM_STRING_COLUMN,
        UUID_FROM_BYTES_COLUMN, ID_COLUMN, ID_COLUMN, ID_COLUMN, ID_COLUMN));

    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).get(0).asInt(), 0);
    assertEquals(rows.get(0).get(1).asInt(), 2);
    assertEquals(rows.get(0).get(2).asText(), DISTINCT_UUID_VALUES.get(0));
    assertEquals(rows.get(1).get(0).asInt(), 1);
    assertEquals(rows.get(1).get(1).asInt(), 4);
    assertEquals(rows.get(1).get(2).asText(), DISTINCT_UUID_VALUES.get(1));
  }

  private JsonNode postRows(boolean useMultiStageQueryEngine, String query)
      throws Exception {
    return postResultTable(useMultiStageQueryEngine, query).get("rows");
  }

  private JsonNode postResultTable(boolean useMultiStageQueryEngine, String query)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery(query);
    assertNoExceptions(response);
    return response.get("resultTable");
  }

  private static void assertGroupedCounts(JsonNode rows) {
    assertEquals(rows.size(), DISTINCT_UUID_VALUES.size());
    assertEquals(rows.get(0).get(0).asText(), DISTINCT_UUID_VALUES.get(0));
    assertEquals(rows.get(0).get(1).asInt(), 2);
    assertEquals(rows.get(1).get(0).asText(), DISTINCT_UUID_VALUES.get(1));
    assertEquals(rows.get(1).get(1).asInt(), 2);
    assertEquals(rows.get(2).get(0).asText(), DISTINCT_UUID_VALUES.get(2));
    assertEquals(rows.get(2).get(1).asInt(), 1);
    assertEquals(rows.get(3).get(0).asText(), DISTINCT_UUID_VALUES.get(3));
    assertEquals(rows.get(3).get(1).asInt(), 1);
  }

  private static void assertDistinctRows(JsonNode rows) {
    assertEquals(rows.size(), DISTINCT_UUID_VALUES.size());
    for (int i = 0; i < DISTINCT_UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), DISTINCT_UUID_VALUES.get(i));
    }
  }

  private static void assertIdRows(JsonNode rows, int... expectedIds) {
    assertEquals(rows.size(), expectedIds.length);
    for (int i = 0; i < expectedIds.length; i++) {
      assertEquals(rows.get(i).get(0).asInt(), expectedIds[i]);
    }
  }

  private static GenericData.Array<String> uuidStringArray(int rowIndex, org.apache.avro.Schema schema) {
    GenericData.Array<String> array = new GenericData.Array<>(2, schema);
    array.add(UUID_INPUT_VALUES.get(rowIndex));
    array.add(UUID_INPUT_VALUES.get((rowIndex + 1) % UUID_INPUT_VALUES.size()));
    return array;
  }

  private static GenericData.Array<ByteBuffer> uuidBytesArray(int rowIndex, org.apache.avro.Schema schema) {
    GenericData.Array<ByteBuffer> array = new GenericData.Array<>(2, schema);
    array.add(ByteBuffer.wrap(UuidUtils.toBytes(UUID_INPUT_VALUES.get(rowIndex))));
    array.add(ByteBuffer.wrap(UuidUtils.toBytes(UUID_INPUT_VALUES.get((rowIndex + 1) % UUID_INPUT_VALUES.size()))));
    return array;
  }

  private static void assertUuidArray(JsonNode array, int rowIndex) {
    assertEquals(array.size(), 2);
    assertEquals(array.get(0).asText(), UUID_VALUES.get(rowIndex));
    assertEquals(array.get(1).asText(), UUID_VALUES.get((rowIndex + 1) % UUID_VALUES.size()));
  }

  private static String uuidHex(String uuidValue) {
    return BytesUtils.toHexString(UuidUtils.toBytes(uuidValue));
  }

  private static void assertNoExceptions(JsonNode response) {
    assertEquals(response.get("exceptions").size(), 0, response.toPrettyString());
  }
}
