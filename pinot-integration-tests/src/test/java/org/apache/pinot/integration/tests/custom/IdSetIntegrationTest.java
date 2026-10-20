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
import static org.testng.Assert.assertFalse;
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
  // A Bloom filter sized for 10M ids, which serializes to a literal of about 16 MB
  private static final String LARGE_BLOOM_FILTER_PARAMS = "expectedInsertions=10000000;fpp=0.01";
  // A Bloom filter sized for 20M ids, which serializes to a literal of about 32 MB
  private static final String HUGE_BLOOM_FILTER_PARAMS = "expectedInsertions=20000000;fpp=0.01";
  // The number of rows whose value is in the sets built from the rows with id < NUM_SET_VALUES, without false positives
  private static final long NUM_ROWS_IN_SET = (long) NUM_SET_VALUES * (NUM_ROWS / NUM_VALUES);

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
    // The subquery is not correlated, so it runs before the query, like the subquery of an IN_SUBQUERY
    setUseMultiStageQueryEngine(true);
    for (String column : List.of(INT_COL, LONG_COL, STR_COL, BYTES_COL)) {
      String idSetQuery = "SELECT " + idSetExpression(column) + " FROM " + getTableName() + " WHERE " + SET_FILTER;
      IdSet idSet = IdSets.fromBase64String(getSingleValue(postQuery(idSetQuery)).asText());
      assertEquals(getCount("IN_ID_SET(" + column + ", (" + idSetQuery + ")) = 1"), getExpectedCount(column, idSet),
          column);
    }

    // So the filter runs in the leaf stage, without a join. EXPLAIN does not run the subquery, and says so.
    JsonNode response = postQuery("EXPLAIN PLAN FOR SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET("
        + INT_COL + ", (SELECT IDSET(" + INT_COL + ") FROM " + getTableName() + " WHERE " + SET_FILTER + ")) = 1");
    assertTrue(response.get("exceptions").isEmpty(), response.get("exceptions").toString());
    JsonNode resultTable = response.get("resultTable");
    String plan = resultTable.get("rows").get(0).get(1).asText();
    assertFalse(plan.contains("Join"), plan);
    assertTrue(resultTable.get("dataSchema").get("columnNames").toString().contains("SUBQUERIES"),
        resultTable.toString());

    // A correlated subquery runs as part of the query
    assertEquals(getSingleValue(postQuery("SELECT COUNT(*) FROM " + getTableName() + " t1 WHERE IN_ID_SET(t1."
        + INT_COL + ", (SELECT IDSET(t2." + INT_COL + ") FROM " + getTableName() + " t2 WHERE t2." + SET_FILTER
        + " AND t2." + LONG_COL + " = t1." + LONG_COL + ")) = 1")).asLong(), NUM_ROWS_IN_SET);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInSubquery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String column : List.of(INT_COL, LONG_COL, STR_COL, BYTES_COL)) {
      String idSetQuery = "SELECT " + idSetExpression(column) + " FROM " + getTableName() + " WHERE " + SET_FILTER;
      long expectedCount =
          getExpectedCount(column, IdSets.fromBase64String(getSingleValue(postQuery(idSetQuery)).asText()));
      String inSubquery = "IN_SUBQUERY(" + column + ", " + quote(idSetQuery) + ")";
      assertEquals(getCount(inSubquery + " = 1"), expectedCount, column);
      assertEquals(getCount(inSubquery + " = 0"), NUM_ROWS - expectedCount, column);
    }
  }

  @Test
  public void testInSubqueryOutsideFilters()
      throws Exception {
    // Only the multi-stage engine runs IN_SUBQUERY outside the WHERE clause
    setUseMultiStageQueryEngine(true);
    String subquery = quote("SELECT IDSET(" + INT_COL + ") FROM " + getTableName() + " WHERE " + SET_FILTER);
    String inSubquery = "IN_SUBQUERY(" + INT_COL + ", " + subquery + ") = 1";
    assertEquals(getSingleValue(postQuery(
            "SELECT SUM(CASE WHEN " + inSubquery + " THEN 1 ELSE 0 END) FROM " + getTableName())).asLong(),
        NUM_ROWS_IN_SET);
    JsonNode response = postQuery("SELECT " + INT_COL + ", COUNT(*) FROM " + getTableName() + " GROUP BY " + INT_COL
        + " HAVING " + inSubquery + " LIMIT " + NUM_VALUES);
    assertTrue(response.get("exceptions").isEmpty(), response.get("exceptions").toString());
    assertEquals(response.get("resultTable").get("rows").size(), NUM_SET_VALUES);
    assertEquals(getSingleValue(postQuery("SELECT COUNT(*) FROM " + getTableName() + " t1 JOIN " + getTableName()
        + " t2 ON t1." + ID + " = t2." + ID + " WHERE IN_SUBQUERY(t2." + INT_COL + ", " + subquery + ") = 1"))
        .asLong(), NUM_ROWS_IN_SET);

    // A subquery can have IN_SUBQUERY too
    String nestedInSubquery = "IN_SUBQUERY(" + ID + ", " + quote(
        "SELECT IDSET(" + ID + ") FROM " + getTableName() + " WHERE " + SET_FILTER) + ") = 1";
    assertEquals(getCount("IN_SUBQUERY(" + INT_COL + ", " + quote(
        "SELECT IDSET(" + INT_COL + ") FROM " + getTableName() + " WHERE " + nestedInSubquery) + ") = 1"),
        NUM_ROWS_IN_SET);
  }

  @Test
  public void testFailsOnPartialSubqueryResult()
      throws Exception {
    // The subquery stops at 10 groups, so its IdSet would miss ids
    setUseMultiStageQueryEngine(true);
    String subquery = "SET numGroupsLimit = 10; SELECT IDSET(" + INT_COL + ") FROM (SELECT " + INT_COL + " FROM "
        + getTableName() + " GROUP BY " + INT_COL + ")";
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_SUBQUERY(" + INT_COL + ", "
        + quote(subquery) + ") = 1");
    String exceptions = response.get("exceptions").toString();
    assertTrue(exceptions.contains("Subquery returned a partial result [numGroupsLimitReached]"), exceptions);
  }

  @Test
  public void testHugeIdSetInSubquery()
      throws Exception {
    // The set never reaches the client or the parser, so it can be larger than the 20M chars a JSON request can hold
    setUseMultiStageQueryEngine(true);
    String idSetQuery = "SELECT IDSET(" + STR_COL + ", '" + HUGE_BLOOM_FILTER_PARAMS + "') FROM " + getTableName()
        + " WHERE " + SET_FILTER;
    // With 60 of the 20M ids it is sized for, the Bloom filter has practically no false positives
    assertEquals(getCount("IN_SUBQUERY(" + STR_COL + ", " + quote(idSetQuery) + ") = 1"), NUM_ROWS_IN_SET);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLargeIdSet(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String idSet = getSingleValue(postQuery(
        "SELECT IDSET(" + STR_COL + ", '" + LARGE_BLOOM_FILTER_PARAMS + "') FROM " + getTableName() + " WHERE "
            + SET_FILTER)).asText();
    assertTrue(idSet.length() > 15_000_000, "IdSet length: " + idSet.length());
    String inQuery =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE IN_ID_SET(" + STR_COL + ", '" + idSet + "') = 1";
    // Over HTTP, because the broker's gRPC endpoint accepts requests of up to 4 MB
    assertEquals(getSingleValue(queryBrokerHttpEndpoint(inQuery)).asLong(),
        getExpectedCount(STR_COL, IdSets.fromBase64String(idSet)));
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

  /// Returns the SQL string literal for the given string.
  private static String quote(String string) {
    return "'" + string.replace("'", "''") + "'";
  }

  /// Returns the number of rows that pass the filter.
  private long getCount(String filter)
      throws Exception {
    return getSingleValue(postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + filter)).asLong();
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
