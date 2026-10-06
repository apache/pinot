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
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// IDSET and IN_ID_SET in both query engines, on a cluster with 2 servers. A set built by either engine must filter
/// the same rows in the other one.
@Test(suiteName = "CustomClusterIntegrationTest")
public class IdSetIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "IdSetIntegrationTest";
  private static final String ID = "id";
  private static final String INT_COL = "intCol";
  private static final String LONG_COL = "longCol";
  private static final String STR_COL = "strCol";
  private static final String BYTES_COL = "bytesCol";
  private static final int NUM_ROWS = 1000;
  // Row i holds value i % NUM_VALUES, so each value appears in NUM_ROWS / NUM_VALUES rows, and the sets built from the
  // rows with id < NUM_SET_VALUES hold values 0 to NUM_SET_VALUES - 1
  private static final int NUM_VALUES = 200;
  private static final int NUM_SET_VALUES = 60;
  private static final String SET_FILTER = ID + " < " + NUM_SET_VALUES;
  // STRING and BYTES sets are Bloom filters; these params keep their literals short
  private static final String BLOOM_FILTER_PARAMS = "expectedInsertions=1000;fpp=0.0001";

  @Override
  protected long getCountStarResult() {
    return NUM_ROWS;
  }

  @Test
  public void testIdSetIsTheSameInBothEngines()
      throws Exception {
    for (String column : List.of(INT_COL, LONG_COL, STR_COL, BYTES_COL)) {
      String query = "SELECT " + idSetExpression(column) + " FROM " + getTableName() + " WHERE " + SET_FILTER;
      setUseMultiStageQueryEngine(false);
      String singleStageIdSet = getSingleValue(postQuery(query)).asText();
      setUseMultiStageQueryEngine(true);
      String multiStageIdSet = getSingleValue(postQuery(query)).asText();
      assertEquals(multiStageIdSet, singleStageIdSet, column);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilterBySetFromEitherEngine(boolean buildSetWithMultiStageEngine)
      throws Exception {
    for (String column : List.of(INT_COL, LONG_COL, STR_COL, BYTES_COL)) {
      setUseMultiStageQueryEngine(buildSetWithMultiStageEngine);
      String idSet = getSingleValue(
          postQuery("SELECT " + idSetExpression(column) + " FROM " + getTableName() + " WHERE " + SET_FILTER)).asText();
      String inQuery =
          "SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET(" + column + ", '" + idSet + "') = 1";
      String notInQuery =
          "SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET(" + column + ", '" + idSet + "') = 0";
      setUseMultiStageQueryEngine(false);
      long singleStageCount = getSingleValue(postQuery(inQuery)).asLong();
      assertEquals(getSingleValue(postQuery(notInQuery)).asLong(), NUM_ROWS - singleStageCount, column);
      setUseMultiStageQueryEngine(true);
      long multiStageCount = getSingleValue(postQuery(inQuery)).asLong();
      assertEquals(getSingleValue(postQuery(notInQuery)).asLong(), NUM_ROWS - multiStageCount, column);
      assertEquals(multiStageCount, singleStageCount, column);
      assertEquals(multiStageCount, getExpectedCount(column, IdSets.fromBase64String(idSet)), column);
    }
  }

  @Test
  public void testScalarSubquery()
      throws Exception {
    // The set argument is a scalar subquery, so IN_ID_SET runs as a scalar function above the join that supplies it
    setUseMultiStageQueryEngine(true);
    for (String column : List.of(INT_COL, LONG_COL, STR_COL, BYTES_COL)) {
      String idSetQuery = "SELECT " + idSetExpression(column) + " FROM " + getTableName() + " WHERE " + SET_FILTER;
      String query =
          "SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET(" + column + ", (" + idSetQuery + ")) = 1";
      IdSet idSet = IdSets.fromBase64String(getSingleValue(postQuery(idSetQuery)).asText());
      assertEquals(getSingleValue(postQuery(query)).asLong(), getExpectedCount(column, idSet), column);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMismatchedValueType(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    IdSet intIdSet = IdSets.create(DataType.INT);
    intIdSet.add(1);
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET(" + LONG_COL + ", '" + intIdSet.toBase64String()
            + "') = 1");
    String exceptions = response.get("exceptions").toString();
    assertTrue(exceptions.contains("Cannot look up LONG values in an IdSet built from INT values"), exceptions);
  }

  private static String idSetExpression(String column) {
    if (column.equals(STR_COL) || column.equals(BYTES_COL)) {
      return "IDSET(" + column + ", '" + BLOOM_FILTER_PARAMS + "')";
    }
    return "IDSET(" + column + ")";
  }

  /// Returns the number of rows whose value of the column the IdSet contains, which includes any Bloom filter false
  /// positives. INT and LONG sets are exact, so they hold exactly the values of the rows with id < NUM_SET_VALUES.
  private static long getExpectedCount(String column, IdSet idSet) {
    long numValuesInSet = 0;
    for (int value = 0; value < NUM_VALUES; value++) {
      boolean contains;
      switch (column) {
        case INT_COL:
          contains = idSet.contains(value);
          break;
        case LONG_COL:
          contains = idSet.contains(10_000_000_000L + value);
          break;
        case STR_COL:
          contains = idSet.contains(stringValue(value));
          break;
        default:
          contains = idSet.contains(stringValue(value).getBytes(StandardCharsets.UTF_8));
          break;
      }
      if (contains) {
        numValuesInSet++;
      } else if (value < NUM_SET_VALUES) {
        throw new AssertionError(column + " set misses value " + value);
      }
    }
    if (column.equals(INT_COL) || column.equals(LONG_COL)) {
      assertEquals(numValuesInSet, NUM_SET_VALUES, column);
    }
    return numValuesInSet * (NUM_ROWS / NUM_VALUES);
  }

  private static String stringValue(int value) {
    return "value_" + value;
  }

  private static JsonNode getSingleValue(JsonNode response) {
    assertTrue(response.get("exceptions").isEmpty(), response.get("exceptions").toString());
    return response.get("resultTable").get("rows").get(0).get(0);
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID, DataType.INT)
        .addSingleValueDimension(INT_COL, DataType.INT)
        .addSingleValueDimension(LONG_COL, DataType.LONG)
        .addSingleValueDimension(STR_COL, DataType.STRING)
        .addSingleValueDimension(BYTES_COL, DataType.BYTES)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null),
        new org.apache.avro.Schema.Field(INT_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null),
        new org.apache.avro.Schema.Field(LONG_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(STR_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(BYTES_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null)));
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_ROWS; i++) {
        int value = i % NUM_VALUES;
        String stringValue = stringValue(value);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID, i);
        record.put(INT_COL, value);
        record.put(LONG_COL, 10_000_000_000L + value);
        record.put(STR_COL, stringValue);
        record.put(BYTES_COL, ByteBuffer.wrap(stringValue.getBytes(StandardCharsets.UTF_8)));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }
}
