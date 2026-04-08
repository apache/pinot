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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * End-to-end integration tests for bitwise scalar functions over a custom Pinot cluster.
 *
 * <p>This test exercises projection and predicate evaluation for INT and LONG bitwise functions.
 * Test methods mutate the query-engine mode and are not thread-safe.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class BitwiseFunctionsIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "BitwiseFunctionsIntegrationTest";
  private static final String ID_COLUMN = "id";
  private static final String INT_COLUMN = "intCol";
  private static final String INT_OTHER_COLUMN = "intOtherCol";
  private static final String LONG_COLUMN = "longCol";
  private static final String LONG_OTHER_COLUMN = "longOtherCol";
  private static final String INT_SHIFT_COLUMN = "intShiftCol";
  private static final String LONG_SHIFT_COLUMN = "longShiftCol";

  private static final List<RowData> ROWS = List.of(
      new RowData(0, 6, 10, 10L, 6L, 1, 1L),
      new RowData(1, -8, 3, -8L, 3L, 2, 2L)
  );

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return ROWS.size();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_OTHER_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_OTHER_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(INT_SHIFT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_SHIFT_COLUMN, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("bitwiseRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(INT_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(INT_OTHER_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(LONG_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(LONG_OTHER_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(INT_SHIFT_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(LONG_SHIFT_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < ROWS.size(); i++) {
        RowData rowData = ROWS.get(i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, rowData._id);
        record.put(INT_COLUMN, rowData._intValue);
        record.put(INT_OTHER_COLUMN, rowData._intOtherValue);
        record.put(LONG_COLUMN, rowData._longValue);
        record.put(LONG_OTHER_COLUMN, rowData._longOtherValue);
        record.put(INT_SHIFT_COLUMN, rowData._intShift);
        record.put(LONG_SHIFT_COLUMN, rowData._longShift);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBinaryBitwiseFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT "
            + "bitAnd(%s, %s), bitOr(%s, %s), bitXor(%s, %s), "
            + "bitAnd(%s, %s), bitOr(%s, %s), bitXor(%s, %s), "
            + "bitAnd(%s, %s), bit_and(%s, %s), bit_or(%s, %s), bit_xor(%s, %s) "
            + "FROM %s WHERE %s = 0",
        INT_COLUMN, INT_OTHER_COLUMN, INT_COLUMN, INT_OTHER_COLUMN, INT_COLUMN, INT_OTHER_COLUMN, INT_COLUMN,
        LONG_COLUMN, INT_COLUMN, LONG_COLUMN, INT_COLUMN, LONG_COLUMN, LONG_COLUMN, LONG_OTHER_COLUMN, INT_COLUMN,
        LONG_COLUMN, INT_COLUMN, LONG_COLUMN, INT_COLUMN, LONG_COLUMN, getTableName(), ID_COLUMN);
    JsonNode response = postQuery(query);
    JsonNode row = getOnlyRow(response, 10);

    assertColumnTypes(response, "INT", "INT", "INT", "LONG", "LONG", "LONG", "LONG", "LONG", "LONG", "LONG");
    assertEquals(row.get(0).asInt(), 2);
    assertEquals(row.get(1).asInt(), 14);
    assertEquals(row.get(2).asInt(), 12);
    assertEquals(row.get(3).asLong(), 2L);
    assertEquals(row.get(4).asLong(), 14L);
    assertEquals(row.get(5).asLong(), 12L);
    assertEquals(row.get(6).asLong(), 2L);
    assertEquals(row.get(7).asLong(), 2L);
    assertEquals(row.get(8).asLong(), 14L);
    assertEquals(row.get(9).asLong(), 12L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUnaryAndShiftBitwiseFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT "
            + "bitNot(%s), bitNot(%s), bitMask(%s), bitMask(%s), "
            + "bitShiftLeft(%s, %s), bitShiftLeft(%s, %s), "
            + "bitShiftRight(%s, %s), bitShiftRight(%s, %s), "
            + "bitShiftRightUnsigned(%s, %s), bitShiftRightLogical(%s, %s), "
            + "bitExtract(%s, %s), extractBit(%s, %s) "
            + "FROM %s WHERE %s = 1",
        INT_COLUMN, LONG_COLUMN, INT_SHIFT_COLUMN, LONG_SHIFT_COLUMN, INT_COLUMN, INT_SHIFT_COLUMN, LONG_COLUMN,
        INT_SHIFT_COLUMN, INT_COLUMN, INT_SHIFT_COLUMN, LONG_COLUMN, INT_SHIFT_COLUMN, INT_COLUMN, INT_SHIFT_COLUMN,
        LONG_COLUMN, INT_SHIFT_COLUMN, INT_COLUMN, INT_SHIFT_COLUMN, LONG_COLUMN, LONG_SHIFT_COLUMN, getTableName(),
        ID_COLUMN);
    JsonNode response = postQuery(query);
    JsonNode row = getOnlyRow(response, 12);

    assertColumnTypes(response, "INT", "LONG", "LONG", "LONG", "INT", "LONG", "INT", "LONG", "INT", "LONG", "INT",
        "INT");
    assertEquals(row.get(0).asInt(), 7);
    assertEquals(row.get(1).asLong(), 7L);
    assertEquals(row.get(2).asLong(), 4L);
    assertEquals(row.get(3).asLong(), 4L);
    assertEquals(row.get(4).asInt(), -32);
    assertEquals(row.get(5).asLong(), -32L);
    assertEquals(row.get(6).asInt(), -2);
    assertEquals(row.get(7).asLong(), -2L);
    assertEquals(row.get(8).asInt(), 1073741822);
    assertEquals(row.get(9).asLong(), 4611686018427387902L);
    assertEquals(row.get(10).asInt(), 0);
    assertEquals(row.get(11).asInt(), 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBitwiseFunctionsInPredicates(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT %s, bitMask(%s), bitMask(%s) "
            + "FROM %s WHERE bitExtract(%s, %s) = 1",
        ID_COLUMN, INT_SHIFT_COLUMN, LONG_SHIFT_COLUMN, getTableName(), LONG_COLUMN, LONG_SHIFT_COLUMN);
    JsonNode response = postQuery(query);
    JsonNode row = getOnlyRow(response, 3);

    assertColumnTypes(response, "INT", "LONG", "LONG");
    assertEquals(row.get(0).asInt(), 0);
    assertEquals(row.get(1).asLong(), 2L);
    assertEquals(row.get(2).asLong(), 2L);
  }

  private JsonNode getOnlyRow(JsonNode response, int expectedColumnCount) {
    assertEquals(response.path("exceptions").size(), 0);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), expectedColumnCount);
    return row;
  }

  private void assertColumnTypes(JsonNode response, String... expectedTypes) {
    for (int i = 0; i < expectedTypes.length; i++) {
      assertEquals(getType(response, i), expectedTypes[i]);
    }
  }

  private static final class RowData {
    private final int _id;
    private final int _intValue;
    private final int _intOtherValue;
    private final long _longValue;
    private final long _longOtherValue;
    private final int _intShift;
    private final long _longShift;

    private RowData(int id, int intValue, int intOtherValue, long longValue, long longOtherValue, int intShift,
        long longShift) {
      _id = id;
      _intValue = intValue;
      _intOtherValue = intOtherValue;
      _longValue = longValue;
      _longOtherValue = longOtherValue;
      _intShift = intShift;
      _longShift = longShift;
    }
  }
}
