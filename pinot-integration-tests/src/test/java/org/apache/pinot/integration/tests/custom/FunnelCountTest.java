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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Integration test for FUNNEL_COUNT aggregation function across all strategy combinations
 * (bitmap, set, theta_sketch, partitioned, partitioned+sorted, partitioned+theta_sketch).
 *
 * <p>Uses an e-commerce funnel with 4 steps: view → cart → checkout → purchase.
 * Data is split across 2 segments with no user overlap (properly partitioned),
 * so all strategies produce identical results.
 *
 * <p>Each segment contains multiple category values (electronics, clothing, home) to
 * exercise GROUP BY across categories within a single segment. Users 3 and 9 are
 * "cross-category" — their funnel actions span two categories, which is specifically
 * designed to support future "hold constant columns" feature testing.
 *
 * <h3>Test data layout (2 segments)</h3>
 * <pre>
 * Segment 1 — users 1–6 (mixed categories):
 *   user_id | action   | category
 *   --------|----------|-------------
 *       1   | view     | electronics       user 1: 4 steps, all electronics
 *       1   | cart     | electronics
 *       1   | checkout | electronics
 *       1   | purchase | electronics
 *       2   | view     | clothing          user 2: 4 steps, all clothing
 *       2   | cart     | clothing
 *       2   | checkout | clothing
 *       2   | purchase | clothing
 *       3   | view     | electronics       user 3: 3 steps, MIXED (elec → cloth)
 *       3   | cart     | electronics
 *       3   | checkout | clothing
 *       4   | view     | clothing          user 4: 2 steps, all clothing
 *       4   | cart     | clothing
 *       5   | view     | home              user 5: 2 steps, all home
 *       5   | cart     | home
 *       6   | view     | electronics       user 6: 1 step, electronics
 *
 * Segment 2 — users 7–12 (mixed categories):
 *   user_id | action   | category
 *   --------|----------|-------------
 *       7   | view     | clothing          user 7: 4 steps, all clothing
 *       7   | cart     | clothing
 *       7   | checkout | clothing
 *       7   | purchase | clothing
 *       8   | view     | electronics       user 8: 3 steps, all electronics
 *       8   | cart     | electronics
 *       8   | checkout | electronics
 *       9   | view     | home              user 9: 3 steps, MIXED (home → elec)
 *       9   | cart     | home
 *       9   | checkout | electronics
 *      10   | view     | electronics       user 10: 2 steps, all electronics
 *      10   | cart     | electronics
 *      11   | view     | clothing          user 11: 2 steps, all clothing
 *      11   | cart     | clothing
 *      12   | view     | home              user 12: 1 step, home
 * </pre>
 *
 * <h3>Expected funnel counts</h3>
 * <pre>
 * Overall:       [12, 10, 6, 3]
 * clothing:      [ 4,  4, 2, 2]
 * electronics:   [ 5,  4, 2, 1]
 * home:          [ 3,  2, 0, 0]
 * </pre>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class FunnelCountTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "FunnelCountTest";
  private static final String USER_ID_COL = "user_id";
  private static final String ACTION_COL = "action";
  private static final String CATEGORY_COL = "category";

  private static final String VIEW = "view";
  private static final String CART = "cart";
  private static final String CHECKOUT = "checkout";
  private static final String PURCHASE = "purchase";

  private static final long COUNT_STAR = 31L;
  private static final long[] EXPECTED_OVERALL = {12, 10, 6, 3};
  private static final long[] EXPECTED_ELECTRONICS = {5, 4, 2, 1};
  private static final long[] EXPECTED_CLOTHING = {4, 4, 2, 2};
  private static final long[] EXPECTED_HOME = {3, 2, 0, 0};

  @Override
  protected long getCountStarResult() {
    return COUNT_STAR;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(USER_ID_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(ACTION_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CATEGORY_COL, FieldSpec.DataType.STRING)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setSortedColumn(USER_ID_COL)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(USER_ID_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(ACTION_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(CATEGORY_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)));

    File avroFile1 = new File(_tempDir, "data-0.avro");
    try (DataFileWriter<GenericData.Record> w = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      w.create(avroSchema, avroFile1);
      appendRecord(w, avroSchema, 1, VIEW, "electronics");
      appendRecord(w, avroSchema, 1, CART, "electronics");
      appendRecord(w, avroSchema, 1, CHECKOUT, "electronics");
      appendRecord(w, avroSchema, 1, PURCHASE, "electronics");
      appendRecord(w, avroSchema, 2, VIEW, "clothing");
      appendRecord(w, avroSchema, 2, CART, "clothing");
      appendRecord(w, avroSchema, 2, CHECKOUT, "clothing");
      appendRecord(w, avroSchema, 2, PURCHASE, "clothing");
      appendRecord(w, avroSchema, 3, VIEW, "electronics");
      appendRecord(w, avroSchema, 3, CART, "electronics");
      appendRecord(w, avroSchema, 3, CHECKOUT, "clothing");
      appendRecord(w, avroSchema, 4, VIEW, "clothing");
      appendRecord(w, avroSchema, 4, CART, "clothing");
      appendRecord(w, avroSchema, 5, VIEW, "home");
      appendRecord(w, avroSchema, 5, CART, "home");
      appendRecord(w, avroSchema, 6, VIEW, "electronics");
    }

    File avroFile2 = new File(_tempDir, "data-1.avro");
    try (DataFileWriter<GenericData.Record> w = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      w.create(avroSchema, avroFile2);
      appendRecord(w, avroSchema, 7, VIEW, "clothing");
      appendRecord(w, avroSchema, 7, CART, "clothing");
      appendRecord(w, avroSchema, 7, CHECKOUT, "clothing");
      appendRecord(w, avroSchema, 7, PURCHASE, "clothing");
      appendRecord(w, avroSchema, 8, VIEW, "electronics");
      appendRecord(w, avroSchema, 8, CART, "electronics");
      appendRecord(w, avroSchema, 8, CHECKOUT, "electronics");
      appendRecord(w, avroSchema, 9, VIEW, "home");
      appendRecord(w, avroSchema, 9, CART, "home");
      appendRecord(w, avroSchema, 9, CHECKOUT, "electronics");
      appendRecord(w, avroSchema, 10, VIEW, "electronics");
      appendRecord(w, avroSchema, 10, CART, "electronics");
      appendRecord(w, avroSchema, 11, VIEW, "clothing");
      appendRecord(w, avroSchema, 11, CART, "clothing");
      appendRecord(w, avroSchema, 12, VIEW, "home");
    }

    return List.of(avroFile1, avroFile2);
  }

  private void appendRecord(DataFileWriter<GenericData.Record> writer, org.apache.avro.Schema schema,
      int userId, String action, String category)
      throws Exception {
    GenericData.Record record = new GenericData.Record(schema);
    record.put(USER_ID_COL, userId);
    record.put(ACTION_COL, action);
    record.put(CATEGORY_COL, category);
    writer.append(record);
  }

  // ---------- query builders ----------

  private String funnelCountAggregation(String settings) {
    String settingsClause = (settings == null) ? "" : ", SETTINGS(" + settings + ")";
    return String.format("FUNNEL_COUNT("
        + "STEPS(%1$s = '%2$s', %1$s = '%3$s', %1$s = '%4$s', %1$s = '%5$s'), "
        + "CORRELATE_BY(%6$s)"
        + "%7$s)", ACTION_COL, VIEW, CART, CHECKOUT, PURCHASE, USER_ID_COL, settingsClause);
  }

  private String overallQuery(String settings) {
    return String.format("SELECT %s FROM %s", funnelCountAggregation(settings), TABLE_NAME);
  }

  private String groupByQuery(String settings) {
    return String.format("SELECT %s, %s FROM %s GROUP BY %s ORDER BY %s",
        CATEGORY_COL, funnelCountAggregation(settings), TABLE_NAME, CATEGORY_COL, CATEGORY_COL);
  }

  // ---------- assertion helpers ----------

  private JsonNode getRows(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    if (exceptions != null && exceptions.size() > 0) {
      throw new AssertionError("Query returned exceptions: " + exceptions);
    }
    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "resultTable is null; full response: " + response);
    JsonNode rows = resultTable.get("rows");
    assertNotNull(rows, "rows is null; full response: " + response);
    return rows;
  }

  private void assertOverallResult(JsonNode rows, long[] expected) {
    assertEquals(rows.size(), 1, "Expected exactly one result row");
    JsonNode arr = rows.get(0).get(0);
    assertStepCounts(arr, expected);
  }

  private void assertGroupByResult(JsonNode rows) {
    assertEquals(rows.size(), 3, "Expected three category groups");
    assertEquals(rows.get(0).get(0).textValue(), "clothing");
    assertStepCounts(rows.get(0).get(1), EXPECTED_CLOTHING);
    assertEquals(rows.get(1).get(0).textValue(), "electronics");
    assertStepCounts(rows.get(1).get(1), EXPECTED_ELECTRONICS);
    assertEquals(rows.get(2).get(0).textValue(), "home");
    assertStepCounts(rows.get(2).get(1), EXPECTED_HOME);
  }

  private void assertStepCounts(JsonNode resultArray, long[] expected) {
    assertEquals(resultArray.size(), expected.length, "Step count array length mismatch");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(resultArray.get(i).longValue(), expected[i], "Mismatch at funnel step " + i);
    }
  }

  @DataProvider(name = "allStrategies")
  public static Object[][] allStrategies() {
    return new Object[][]{
        {null}, {"'bitmap'"}, {"'set'"}, {"'theta_sketch'"},
        {"'partitioned'"}, {"'partitioned', 'theta_sketch'"}, {"'partitioned', 'sorted'"}
    };
  }

  @Test(dataProvider = "allStrategies")
  public void testOverall(String settings)
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = getRows(postQuery(overallQuery(settings)));
    assertOverallResult(rows, EXPECTED_OVERALL);
  }

  @Test(dataProvider = "allStrategies")
  public void testGroupBy(String settings)
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = getRows(postQuery(groupByQuery(settings)));
    assertGroupByResult(rows);
  }

  // ---------- 3-step sub-funnel (view → cart → checkout) ----------

  @Test(dataProvider = "useV1QueryEngine")
  public void testThreeStepFunnelOverall(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT FUNNEL_COUNT("
            + "STEPS(%1$s = '%2$s', %1$s = '%3$s', %1$s = '%4$s'), "
            + "CORRELATE_BY(%5$s)) FROM %6$s",
        ACTION_COL, VIEW, CART, CHECKOUT, USER_ID_COL, TABLE_NAME);
    JsonNode rows = getRows(postQuery(query));
    assertEquals(rows.size(), 1);
    assertStepCounts(rows.get(0).get(0), new long[]{12, 10, 6});
  }

  // ---------- 2-step sub-funnel (view → purchase) ----------

  @Test(dataProvider = "useV1QueryEngine")
  public void testTwoStepFunnelOverall(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT FUNNEL_COUNT("
            + "STEPS(%1$s = '%2$s', %1$s = '%3$s'), "
            + "CORRELATE_BY(%4$s)) FROM %5$s",
        ACTION_COL, VIEW, PURCHASE, USER_ID_COL, TABLE_NAME);
    JsonNode rows = getRows(postQuery(query));
    assertEquals(rows.size(), 1);
    assertStepCounts(rows.get(0).get(0), new long[]{12, 3});
  }

  // ---------- with WHERE filter ----------
  // Uses user_id range so both segments contribute rows (avoids server-side NPE
  // when a filter eliminates all rows from a segment).
  // WHERE user_id <= 7 → users 1-6 from segment 1, user 7 from segment 2
  // Expected: view=7, cart=6, checkout=4, purchase=3
  private static final long[] EXPECTED_FILTERED = {7, 6, 4, 3};

  private String filteredQuery(String settings) {
    return overallQuery(settings) + " WHERE " + USER_ID_COL + " <= 7";
  }

  @DataProvider(name = "filteredStrategies")
  public static Object[][] filteredStrategies() {
    return new Object[][]{{null}, {"'partitioned'"}, {"'partitioned', 'sorted'"}};
  }

  @Test(dataProvider = "filteredStrategies")
  public void testWithFilter(String settings)
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = getRows(postQuery(filteredQuery(settings)));
    assertEquals(rows.size(), 1);
    assertStepCounts(rows.get(0).get(0), EXPECTED_FILTERED);
  }
}
