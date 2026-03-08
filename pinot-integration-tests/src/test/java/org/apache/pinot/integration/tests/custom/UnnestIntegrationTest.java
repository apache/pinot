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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class UnnestIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "UnnestIntegrationTest";
  private static final String INT_COLUMN = "intCol";
  private static final String LONG_COLUMN = "longCol";
  private static final String FLOAT_COLUMN = "floatCol";
  private static final String DOUBLE_COLUMN = "doubleCol";
  private static final String STRING_COLUMN = "stringCol";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String GROUP_BY_COLUMN = "groupKey";
  private static final String LONG_ARRAY_COLUMN = "longArrayCol";
  private static final String DOUBLE_ARRAY_COLUMN = "doubleArrayCol";
  private static final String STRING_ARRAY_COLUMN = "stringArrayCol";

  @Test(dataProvider = "useV2QueryEngine")
  public void testCountWithCrossJoinUnnest(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s CROSS JOIN UNNEST(longArrayCol) AS u(elem)", getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    long count = rows.get(0).get(0).asLong();
    assertEquals(count, 4 * getCountStarResult());
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSelectWithCrossJoinUnnest(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT intCol, u.elem FROM %s CROSS JOIN UNNEST(stringArrayCol) AS u(elem)"
        + " ORDER BY intCol", getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.size(), 3 * getCountStarResult());
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.get(0).asInt(), i / 3);
      switch (i % 3) {
        case 0:
          assertEquals(row.get(1).asText(), "a");
          break;
        case 1:
          assertEquals(row.get(1).asText(), "b");
          break;
        case 2:
          assertEquals(row.get(1).asText(), "c");
          break;
        default:
          break;
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSelectWithCrossJoinUnnestOnMultiColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT intCol, u.longValue, u.stringValue FROM %s CROSS JOIN UNNEST(longArrayCol, stringArrayCol) AS u"
            + "(longValue, stringValue)"
            + " ORDER BY intCol", getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.size(), 4 * getCountStarResult());
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.get(0).asInt(), i / 4);
      switch (i % 4) {
        case 0:
          assertEquals(row.get(1).asLong(), 0L);
          assertEquals(row.get(2).asText(), "a");
          break;
        case 1:
          assertEquals(row.get(1).asLong(), 1L);
          assertEquals(row.get(2).asText(), "b");
          break;
        case 2:
          assertEquals(row.get(1).asLong(), 2L);
          assertEquals(row.get(2).asText(), "c");
          break;
        case 3:
          assertEquals(row.get(1).asLong(), 3L);
          assertEquals(row.get(2).asText(), "null");
          break;
        default:
          break;
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSelectWithCrossJoinUnnestWithOrdinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT intCol, u.elem, u.idx FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS u(elem, idx)"
            + " ORDER BY intCol, u.idx", getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.size(), 3 * getCountStarResult());
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      int expectedBase = i / 3;
      int mod = i % 3;
      int expectedIdx = mod + 1; // 1-based ordinality
      assertEquals(row.get(0).asInt(), expectedBase);
      switch (mod) {
        case 0:
          assertEquals(row.get(1).asText(), "a");
          break;
        case 1:
          assertEquals(row.get(1).asText(), "b");
          break;
        case 2:
          assertEquals(row.get(1).asText(), "c");
          break;
        default:
          break;
      }
      assertEquals(row.get(2).asInt(), expectedIdx);
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testCountFilterOnOrdinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT COUNT(u.elem), sum(u.idx) FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS u(elem, idx) "
            + "WHERE idx = 2",
        getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    // One match per base row when idx = 2
    assertEquals(rows.get(0).get(0).asLong(), getCountStarResult());
    assertEquals(rows.get(0).get(1).asLong(), 2 * getCountStarResult());

    query = String.format(
        "SELECT COUNT(u.elem) FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS u(elem, idx) "
            + "WHERE idx = 2",
        getTableName());
    json = postQuery(query);
    rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.get(0).get(0).asLong(), getCountStarResult());

    query = String.format(
        "SELECT  sum(u.idx) FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS u(elem, idx) "
            + "WHERE idx = 2",
        getTableName());
    json = postQuery(query);
    rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.get(0).get(0).asLong(), 2 * getCountStarResult());
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSelectWithMultiArrayUnnestWithOrdinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT intCol, u.longValue, u.stringValue, u.ord FROM %s "
            + "CROSS JOIN UNNEST(longArrayCol, stringArrayCol) WITH ORDINALITY AS u(longValue, stringValue, ord) "
            + "ORDER BY intCol, u.ord",
        getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    // longArrayCol has 4 entries per row, so we expect 4 rows per base row after zipping with stringArrayCol
    assertEquals(rows.size(), 4 * getCountStarResult());
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      int baseRow = i / 4;
      int ordinality = (i % 4) + 1;
      assertEquals(row.get(0).asInt(), baseRow);
      assertEquals(row.get(3).asInt(), ordinality);
      switch (ordinality) {
        case 1:
          assertEquals(row.get(1).asLong(), 0L);
          assertEquals(row.get(2).asText(), "a");
          break;
        case 2:
          assertEquals(row.get(1).asLong(), 1L);
          assertEquals(row.get(2).asText(), "b");
          break;
        case 3:
          assertEquals(row.get(1).asLong(), 2L);
          assertEquals(row.get(2).asText(), "c");
          break;
        case 4:
          assertEquals(row.get(1).asLong(), 3L);
          assertTrue(row.get(2).isNull(),
              "Shorter stringArrayCol should yield null padding for ordinality 4");
          break;
        default:
          throw new AssertionError("Unexpected ordinality " + ordinality);
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testAggregateAndFilterWithMultiArrayOrdinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT COUNT(u.longValue), SUM(u.ord) FROM %s "
            + "CROSS JOIN UNNEST(longArrayCol, stringArrayCol) WITH ORDINALITY AS u(longValue, stringValue, ord) "
            + "WHERE ord = 3",
        getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    // Each base row contributes exactly one match at ordinality = 3
    assertEquals(rows.get(0).get(0).asLong(), getCountStarResult());
    assertEquals(rows.get(0).get(1).asLong(), 3L * getCountStarResult());
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSelectFilterOnOrdinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT u.elem, u.idx FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS u(elem, idx) WHERE idx = 2",
        getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    for (JsonNode row : rows) {
      assertEquals(row.get(0).asText(), "b");
      assertEquals(row.get(1).asInt(), 2);
    }
    assertNotNull(rows);
    assertEquals(rows.size(), getCountStarResult());
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testSumOfOrdinalityOnLongArray(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT SUM(u.idx), SUM(u.val) FROM %s CROSS JOIN UNNEST(longArrayCol) WITH ORDINALITY AS u(val, idx)",
        getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    // Each row contributes 1+2+3+4 = 10; there are getCountStarResult() base rows
    assertEquals(rows.get(0).get(0).asLong(), 10 * getCountStarResult());
    // Each row contributes 1+2+3 = 6; there are getCountStarResult() base rows
    assertEquals(rows.get(0).get(1).asLong(), 6 * getCountStarResult());
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testOrdinalityAliasVariation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT SUM(w.ord) FROM %s CROSS JOIN UNNEST(stringArrayCol) WITH ORDINALITY AS w(s, ord)", getTableName());
    JsonNode json = postQuery(query);
    JsonNode rows = json.get("resultTable").get("rows");
    assertNotNull(rows);
    long sum = rows.get(0).get(0).asLong();
    // Each row contributes 1+2+3 = 6; there are getCountStarResult() base rows
    assertEquals(sum, 6 * getCountStarResult());
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(GROUP_BY_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(LONG_ARRAY_COLUMN, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DOUBLE_ARRAY_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension(STRING_ARRAY_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  protected long getCountStarResult() {
    return 10;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(INT_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(LONG_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(FLOAT_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
            null, null),
        new org.apache.avro.Schema.Field(DOUBLE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(STRING_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(GROUP_BY_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(LONG_ARRAY_COLUMN,
            org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)),
            null, null),
        new org.apache.avro.Schema.Field(DOUBLE_ARRAY_COLUMN,
            org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE)),
            null, null),
        new org.apache.avro.Schema.Field(STRING_ARRAY_COLUMN,
            org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)),
            null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COLUMN, i);
        record.put(LONG_COLUMN, i);
        record.put(FLOAT_COLUMN, i + RANDOM.nextFloat());
        record.put(DOUBLE_COLUMN, i + RANDOM.nextDouble());
        record.put(STRING_COLUMN, RandomStringUtils.insecure().next(i));
        record.put(TIMESTAMP_COLUMN, i);
        record.put(GROUP_BY_COLUMN, String.valueOf(i % 10));
        record.put(LONG_ARRAY_COLUMN, List.of(0, 1, 2, 3));
        record.put(DOUBLE_ARRAY_COLUMN, List.of(0.0, 0.1, 0.2, 0.3));
        record.put(STRING_ARRAY_COLUMN, List.of("a", "b", "c"));
        // add avro record to file
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }
}
