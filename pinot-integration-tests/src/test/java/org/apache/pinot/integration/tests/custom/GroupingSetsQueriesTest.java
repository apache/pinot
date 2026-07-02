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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for GROUP BY GROUPING SETS / ROLLUP / CUBE and the GROUPING() / GROUPING_ID() helper
/// functions in the single-stage (v1) query engine.
///
/// The dataset has two dimensions {@code d1} (values a/b, never null) and {@code d2} (values x or genuine
/// NULL), with a metric {@code met = 1} per row. The genuine NULLs in {@code d2} are the crux of the
/// discriminator test: a ROLLUP(d1, d2) produces BOTH a genuine {@code (a, NULL)} detail group (from the
/// {d1, d2} set, GROUPING(d2)=0) and a rolled-up {@code (a, NULL)} subtotal group (from the {d1} set,
/// GROUPING(d2)=1). These must remain distinct rows with independent counts; without the synthetic
/// $groupingId discriminator they would incorrectly merge.
///
/// This feature is single-stage only, so every test pins {@code setUseMultiStageQueryEngine(false)}.
@Test(suiteName = "CustomClusterIntegrationTest")
public class GroupingSetsQueriesTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "GroupingSetsQueriesTest";
  private static final String D1 = "d1";
  private static final String D2 = "d2";
  /// LONG and DOUBLE grouping columns (functionally determined by d1) to cover the non-STRING key-resolution
  /// branches of the generator. MV column to exercise the multi-value rejection guard.
  private static final String LNG = "lng";
  private static final String DBL = "dbl";
  private static final String MV = "mv";
  private static final String MET = "met";
  /// 2 rows for each of (a,x), (a,null), (b,x), (b,null) => 8 docs.
  private static final int NUM_DOCS = 8;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setNullHandlingEnabled(true).build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(D1, DataType.STRING)
        .addSingleValueDimension(D2, DataType.STRING)
        .addSingleValueDimension(LNG, DataType.LONG)
        .addSingleValueDimension(DBL, DataType.DOUBLE)
        .addMultiValueDimension(MV, DataType.STRING)
        .addMetric(MET, DataType.INT)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema stringSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    /// d2 is nullable: union [null, string].
    org.apache.avro.Schema nullableString = org.apache.avro.Schema.createUnion(
        List.of(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), stringSchema));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(D1, stringSchema, null, null),
        new org.apache.avro.Schema.Field(D2, nullableString, null, null),
        new org.apache.avro.Schema.Field(LNG, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null,
            null),
        new org.apache.avro.Schema.Field(DBL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null,
            null),
        new org.apache.avro.Schema.Field(MV, org.apache.avro.Schema.createArray(stringSchema), null, null),
        new org.apache.avro.Schema.Field(MET, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      int doc = 0;
      for (String d1 : new String[]{"a", "b"}) {
        for (String d2 : new String[]{"x", null}) {
          for (int i = 0; i < 2; i++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(D1, d1);
            record.put(D2, d2);
            /// lng/dbl are functionally determined by d1 so ROLLUP over them mirrors ROLLUP(d1).
            record.put(LNG, d1.equals("a") ? 100L : 200L);
            record.put(DBL, d1.equals("a") ? 1.5 : 2.5);
            record.put(MV, List.of("t1", "t2"));
            record.put(MET, 1);
            writers.get(doc % getNumAvroFiles()).append(record);
            doc++;
          }
        }
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /// Encodes a JSON cell value: literal {@code null} becomes the sentinel "NULL", everything else its text.
  private static String cell(JsonNode row, int index) {
    JsonNode value = row.get(index);
    return value.isNull() ? "NULL" : value.asText();
  }

  /// Runs the query (single-stage) and returns the result rows, surfacing any broker/server exception.
  private JsonNode runRows(String query, boolean nullHandling)
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String prefixed = nullHandling ? "SET enableNullHandling=true; " + query : query;
    JsonNode response = postQuery(prefixed);
    JsonNode exceptions = response.get("exceptions");
    assertTrue(exceptions == null || exceptions.isEmpty(), "query failed: " + response.toPrettyString());
    return response.get("resultTable").get("rows");
  }

  @Test
  public void testRollupWithGenuineAndRolledUpNulls()
      throws Exception {
    /// SELECT layout: d1, d2, COUNT(*), GROUPING(d1), GROUPING(d2)
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING(" + D1 + "), GROUPING(" + D2 + ") FROM "
        + getTableName() + " GROUP BY ROLLUP(" + D1 + ", " + D2 + ")";
    JsonNode rows = runRows(query, true);

    /// Key: d1|d2|grouping(d1)|grouping(d2) -> count
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      String key = cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt() + "|" + row.get(4).asInt();
      actual.put(key, row.get(2).asLong());
    }

    Map<String, Long> expected = new HashMap<>();
    /// {d1, d2} set (GROUPING = 0,0): includes the genuine (a, NULL) and (b, NULL) detail groups.
    expected.put("a|x|0|0", 2L);
    expected.put("a|NULL|0|0", 2L);
    expected.put("b|x|0|0", 2L);
    expected.put("b|NULL|0|0", 2L);
    /// {d1} set (d2 rolled up, GROUPING = 0,1): the (a, NULL) / (b, NULL) SUBTOTALS, distinct from the genuine
    /// (a, NULL) / (b, NULL) detail rows above purely by the grouping-id discriminator.
    expected.put("a|NULL|0|1", 4L);
    expected.put("b|NULL|0|1", 4L);
    /// {} grand total (GROUPING = 1,1)
    expected.put("NULL|NULL|1|1", 8L);

    assertEquals(actual, expected);
    assertEquals(rows.size(), expected.size());
    /// The crux: the genuine and rolled-up (a, NULL) rows did NOT merge.
    assertTrue(actual.containsKey("a|NULL|0|0") && actual.containsKey("a|NULL|0|1"),
        "genuine-NULL detail and rolled-up-NULL subtotal must both be present");
  }

  @Test
  public void testRolledUpNullWithoutNullHandling()
      throws Exception {
    /// ROLLUP(d1) over a column with no genuine nulls. Even with null handling DISABLED, the rolled-up
    /// grand-total row must come back with d1 = NULL (grouping sets force null-aware key round-trip).
    String query = "SELECT " + D1 + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ")";
    JsonNode rows = runRows(query, false);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a", 4L);
    expected.put("b", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
    assertTrue(actual.containsKey("NULL"), "rolled-up grand-total row must have NULL key without null handling");
  }

  @Test
  public void testCube()
      throws Exception {
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING(" + D1 + "), GROUPING(" + D2 + ") FROM "
        + getTableName() + " GROUP BY CUBE(" + D1 + ", " + D2 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      String key = cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt() + "|" + row.get(4).asInt();
      actual.put(key, row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    /// {d1, d2}
    expected.put("a|x|0|0", 2L);
    expected.put("a|NULL|0|0", 2L);
    expected.put("b|x|0|0", 2L);
    expected.put("b|NULL|0|0", 2L);
    /// {d1}
    expected.put("a|NULL|0|1", 4L);
    expected.put("b|NULL|0|1", 4L);
    /// {d2}: d1 rolled up; d2 values are x (4) and genuine NULL (4)
    expected.put("NULL|x|1|0", 4L);
    expected.put("NULL|NULL|1|0", 4L);
    /// {}
    expected.put("NULL|NULL|1|1", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), expected.size());
  }

  @Test
  public void testGroupingSets()
      throws Exception {
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*) FROM " + getTableName()
        + " GROUP BY GROUPING SETS ((" + D1 + "), (" + D2 + "))";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    /// {d1}
    expected.put("a|NULL", 4L);
    expected.put("b|NULL", 4L);
    /// {d2}
    expected.put("NULL|x", 4L);
    expected.put("NULL|NULL", 4L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 4);
  }

  @Test
  public void testGroupingIdMultiArg()
      throws Exception {
    /// GROUPING_ID(d1, d2) returns a 2-bit mask: bit for d1 is MSB, bit for d2 is LSB.
    String query = "SELECT " + D1 + ", " + D2 + ", GROUPING_ID(" + D1 + ", " + D2 + "), COUNT(*) FROM "
        + getTableName() + " GROUP BY ROLLUP(" + D1 + ", " + D2 + ")";
    JsonNode rows = runRows(query, true);
    /// GROUPING_ID values: {d1,d2}->0, {d1}->1, {}->3. Exact shape over the 8-doc dataset: 4 detail groups,
    /// 2 per-d1 subtotals and 1 grand total (7 rows), each grouping set covering all 8 docs.
    Map<Integer, Integer> rowsPerGroupingId = new HashMap<>();
    Map<Integer, Long> docsPerGroupingId = new HashMap<>();
    for (JsonNode row : rows) {
      int groupingId = row.get(2).asInt();
      rowsPerGroupingId.merge(groupingId, 1, Integer::sum);
      docsPerGroupingId.merge(groupingId, row.get(3).asLong(), Long::sum);
    }
    assertEquals(rowsPerGroupingId, Map.of(0, 4, 1, 2, 3, 1));
    assertEquals(docsPerGroupingId, Map.of(0, 8L, 1, 8L, 3, 8L));
    assertEquals(rows.size(), 7);
  }

  @Test
  public void testPlainGroupByRegression()
      throws Exception {
    /// A plain GROUP BY (no grouping sets) must be unaffected: 4 detail groups, no synthetic column.
    String query =
        "SELECT " + D1 + ", " + D2 + ", COUNT(*) FROM " + getTableName() + " GROUP BY " + D1 + ", " + D2;
    JsonNode response = postQuery("SET enableNullHandling=true; " + query);
    JsonNode rows = response.get("resultTable").get("rows");
    JsonNode columnNames = response.get("resultTable").get("dataSchema").get("columnNames");
    /// No $groupingId column should leak into the result schema.
    assertEquals(columnNames.size(), 3);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a|x", 2L);
    expected.put("a|NULL", 2L);
    expected.put("b|x", 2L);
    expected.put("b|NULL", 2L);
    assertEquals(actual, expected);
  }

  @Test
  public void testHavingOnGrouping()
      throws Exception {
    /// HAVING GROUPING(d2) = 1 keeps only rows where d2 is rolled up (the {d1} subtotals and the {} grand total
    /// from a ROLLUP(d1, d2)).
    String query = "SELECT " + D1 + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ", " + D2
        + ") HAVING GROUPING(" + D2 + ") = 1";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a", 4L);
    expected.put("b", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
  }

  @Test
  public void testLongGroupingColumn()
      throws Exception {
    /// Exercises the LONG key-resolution branch of the generator (and rolled-up NULL without null handling).
    String query = "SELECT " + LNG + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + LNG + ")";
    JsonNode rows = runRows(query, false);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("100", 4L);
    expected.put("200", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
  }

  @Test
  public void testDoubleGroupingColumn()
      throws Exception {
    /// Exercises the DOUBLE key-resolution branch of the generator.
    String query = "SELECT " + DBL + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + DBL + ")";
    JsonNode rows = runRows(query, false);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("1.5", 4L);
    expected.put("2.5", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
  }

  @Test
  public void testOrderByGroupingColumnWithoutNullHandling()
      throws Exception {
    /// Regression: ORDER BY a grouping column whose rolled-up grand-total value is NULL must not NPE even
    /// when null handling is disabled (grouping sets produce NULL keys regardless of the null-handling
    /// option, so the order-by comparator must be null-safe).
    String query =
        "SELECT " + D1 + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ") ORDER BY " + D1;
    setUseMultiStageQueryEngine(false);
    JsonNode response = postQuery(query);
    assertEquals(response.get("exceptions").size(), 0, response.toPrettyString());
    JsonNode rows = response.get("resultTable").get("rows");
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a", 4L);
    expected.put("b", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
  }

  @Test
  public void testGroupingOnNonGroupingColumnRejected()
      throws Exception {
    /// GROUPING(arg) requires arg to be a grouping column; met is a metric, so the query must fail clearly
    /// rather than produce a wrong value.
    setUseMultiStageQueryEngine(false);
    String query = "SELECT " + D1 + ", GROUPING(" + MET + ") FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ")";
    JsonNode response = postQuery(query);
    assertTrue(response.get("exceptions").size() > 0,
        "GROUPING() over a non-grouping column must be rejected: " + response.toPrettyString());
  }

  @Test
  public void testAggregationOnlyInHavingOrOrderBy()
      throws Exception {
    /// The only aggregation lives in HAVING / ORDER-BY (none in SELECT): the query must execute as an
    /// aggregation group-by — not be rewritten to DISTINCT nor rejected. ROLLUP(d1) groups over the 8-doc
    /// dataset: (a) = 4 docs, (b) = 4 docs, grand total = 8 docs.
    String havingQuery =
        "SELECT " + D1 + " FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ") HAVING COUNT(*) > 4";
    JsonNode rows = runRows(havingQuery, true);
    assertEquals(rows.size(), 1, "only the grand-total group has more than 4 docs");
    assertEquals(cell(rows.get(0), 0), "NULL");

    String orderByQuery = "SELECT " + D1 + " FROM " + getTableName() + " GROUP BY ROLLUP(" + D1
        + ") ORDER BY COUNT(*) DESC, " + D1;
    rows = runRows(orderByQuery, true);
    assertEquals(rows.size(), 3);
    /// Grand total (8 docs) first, then (a) and (b) (4 docs each) tie-broken by d1.
    assertEquals(cell(rows.get(0), 0), "NULL");
    assertEquals(cell(rows.get(1), 0), "a");
    assertEquals(cell(rows.get(2), 0), "b");
  }

  @Test
  public void testAggregationFreeGroupingSetsRejected()
      throws Exception {
    /// A grouping-set query without any aggregation must fail with a clear compilation error. It must NOT be
    /// rewritten to SELECT DISTINCT (which would drop the rolled-up subtotal rows) nor silently executed as a
    /// selection query (which would ignore the GROUP BY entirely).
    setUseMultiStageQueryEngine(false);
    for (String query : new String[]{
        "SELECT " + D1 + " FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ")",
        "SELECT " + D1 + ", " + D2 + " FROM " + getTableName() + " GROUP BY GROUPING SETS ((" + D1 + "), (" + D2
            + "))"
    }) {
      JsonNode response = postQuery(query);
      /// The broker surfaces either the specific compile error ("requires at least one aggregation function",
      /// pinned in GroupingSetsParserTest) or, when the query compiles on the multi-stage engine, the generic
      /// "retry using the multi-stage query engine" hint. Either way it must be an error, never rows.
      assertTrue(response.get("exceptions").size() > 0,
          "aggregation-free grouping-set query must be rejected: " + response.toPrettyString());
      assertEquals(response.get("numRowsResultSet").asInt(), 0, response.toPrettyString());
    }
  }

  @Test
  public void testMultiValueColumnInGroupingSet()
      throws Exception {
    /// Phase 2: a multi-value column may participate in a grouping set. Every row has mv = [t1, t2].
    /// In the {mv} set the row expands over its values (contributes to both t1 and t2); in the {} set mv is
    /// rolled up so the row counts once toward the grand total.
    String query = "SELECT " + MV + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + MV + ")";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("t1", 8L);
    expected.put("t2", 8L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 3);
  }

  @Test
  public void testFilteredAggregation()
      throws Exception {
    /// Phase 2: filtered aggregations combine with grouping sets. COUNT(*) FILTER (WHERE d2 = 'x') alongside
    /// an unfiltered COUNT(*), under ROLLUP(d1). Layout: d1, cntX, cntAll.
    String query = "SELECT " + D1 + ", COUNT(*) FILTER (WHERE " + D2 + " = 'x'), COUNT(*) FROM " + getTableName()
        + " GROUP BY ROLLUP(" + D1 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, long[]> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), new long[]{row.get(1).asLong(), row.get(2).asLong()});
    }
    assertEquals(rows.size(), 3);
    /// d1=a: 2 of 4 rows have d2='x'; d1=b likewise; grand total: 4 of 8.
    assertEquals(actual.get("a"), new long[]{2L, 4L});
    assertEquals(actual.get("b"), new long[]{2L, 4L});
    assertEquals(actual.get("NULL"), new long[]{4L, 8L});
  }

  @Test
  public void testOrderByAggregation()
      throws Exception {
    /// ORDER BY an aggregation in a grouping-set query exercises the order-by aggregation extractor at the
    /// discriminator-shifted aggregation offset (the $groupingId column sits between the group keys and the
    /// aggregations). Without that offset fix this throws a ClassCastException at the IndexedTable resize.
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + D1 + ", "
        + D2 + ") ORDER BY COUNT(*) DESC, " + D1 + " LIMIT 100";
    JsonNode rows = runRows(query, true);
    /// ROLLUP counts: grand total = 8; the two {d1} subtotals = 4; the four detail groups = 2.
    assertEquals(rows.get(0).get(2).asLong(), 8L);
    long previousCount = Long.MAX_VALUE;
    for (JsonNode row : rows) {
      long count = row.get(2).asLong();
      assertTrue(count <= previousCount, "results must be ordered by COUNT(*) DESC");
      previousCount = count;
    }
  }

  @Test
  public void testOrderByGroupingFunction()
      throws Exception {
    /// Regression: ORDER BY GROUPING(...) / GROUPING_ID(...) must not fail when the IndexedTable builds its
    /// order-by extractors. GROUPING/GROUPING_ID are context-dependent (computed from the $groupingId
    /// discriminator), so TableResizer needs a dedicated extractor; the generic post-aggregation path cannot
    /// resolve them and would throw while constructing the ORDER BY comparator.
    String groupingQuery = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING(" + D2 + ") FROM " + getTableName()
        + " GROUP BY ROLLUP(" + D1 + ", " + D2 + ") ORDER BY GROUPING(" + D2 + "), " + D1 + " LIMIT 100";
    JsonNode rows = runRows(groupingQuery, true);
    assertEquals(rows.size(), 7);
    /// ORDER BY GROUPING(d2) ASC: every detail row (GROUPING(d2)=0) sorts before any rolled-up row (=1).
    int previous = Integer.MIN_VALUE;
    for (JsonNode row : rows) {
      int grouping = row.get(3).asInt();
      assertTrue(grouping >= previous, "results must be ordered by GROUPING(d2) ascending");
      previous = grouping;
    }
    assertEquals(rows.get(0).get(3).asInt(), 0);
    assertEquals(rows.get(rows.size() - 1).get(3).asInt(), 1);

    /// GROUPING_ID(d1, d2) in ORDER BY must also resolve. ROLLUP yields ids 0 ({d1,d2}), 1 ({d1}), 3 ({}).
    String groupingIdQuery = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING_ID(" + D1 + ", " + D2 + ") FROM "
        + getTableName() + " GROUP BY ROLLUP(" + D1 + ", " + D2 + ") ORDER BY GROUPING_ID(" + D1 + ", " + D2
        + ") DESC LIMIT 100";
    JsonNode idRows = runRows(groupingIdQuery, true);
    assertEquals(idRows.size(), 7);
    assertEquals(idRows.get(0).get(3).asInt(), 3, "grand total (GROUPING_ID=3) sorts first under DESC");
    int previousId = Integer.MAX_VALUE;
    for (JsonNode row : idRows) {
      int groupingId = row.get(3).asInt();
      assertTrue(groupingId <= previousId, "results must be ordered by GROUPING_ID(d1, d2) descending");
      previousId = groupingId;
    }
  }

  @Test
  public void testEmptyMatchRollup()
      throws Exception {
    /// Regression: an empty match (filter matches no rows, so segments return empty results) for a
    /// grouping-set query must return 0 rows without error. The empty group-by result block must carry the
    /// same $groupingId column as a non-empty one; otherwise the reducer rejects the narrower schema with a
    /// spurious "upgrade servers" error on a fully-upgraded cluster. runRows() asserts no broker exception.
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*) FROM " + getTableName() + " WHERE " + D1
        + " = 'no_such_value' GROUP BY ROLLUP(" + D1 + ", " + D2 + ")";
    JsonNode rows = runRows(query, true);
    assertEquals(rows.size(), 0);
  }

  @Test
  public void testMixedPlainAndRollup()
      throws Exception {
    /// GROUP BY d1, ROLLUP(d2) == GROUPING SETS ((d1, d2), (d1)): plain keys cross-multiply with grouping
    /// constructs. Layout: d1, d2, COUNT(*), GROUPING(d2). The (a, NULL) detail group (genuine NULL,
    /// GROUPING(d2)=0) must stay distinct from the (a) subtotal (GROUPING(d2)=1).
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING(" + D2 + ") FROM " + getTableName()
        + " GROUP BY " + D1 + ", ROLLUP(" + D2 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt(), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a|x|0", 2L);
    expected.put("a|NULL|0", 2L);
    expected.put("a|NULL|1", 4L);
    expected.put("b|x|0", 2L);
    expected.put("b|NULL|0", 2L);
    expected.put("b|NULL|1", 4L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 6);
  }

  @Test
  public void testCompositeRollupLevel()
      throws Exception {
    /// ROLLUP((d1, d2)): a parenthesized level rolls up both columns together, so the expansion is
    /// GROUPING SETS ((d1, d2), ()) — detail rows plus the grand total, with no per-d1 subtotals.
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING_ID(" + D1 + ", " + D2 + ") FROM "
        + getTableName() + " GROUP BY ROLLUP((" + D1 + ", " + D2 + "))";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt(), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a|x|0", 2L);
    expected.put("a|NULL|0", 2L);
    expected.put("b|x|0", 2L);
    expected.put("b|NULL|0", 2L);
    expected.put("NULL|NULL|3", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 5);
  }

  @Test
  public void testNestedRollupInsideGroupingSets()
      throws Exception {
    /// GROUPING SETS may nest other grouping constructs: GROUPING SETS ((d1), ROLLUP(d2)) expands to
    /// {d1}, {d2}, {}. The {d2} set includes a genuine-NULL d2 group (GROUPING_ID=2) that must stay distinct
    /// from the grand total (GROUPING_ID=3) although both render as (NULL, NULL).
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING_ID(" + D1 + ", " + D2 + ") FROM "
        + getTableName() + " GROUP BY GROUPING SETS ((" + D1 + "), ROLLUP(" + D2 + "))";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt(), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a|NULL|1", 4L);
    expected.put("b|NULL|1", 4L);
    expected.put("NULL|x|2", 4L);
    expected.put("NULL|NULL|2", 4L);
    expected.put("NULL|NULL|3", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 5);
  }

  @Test
  public void testExpressionGroupingColumn()
      throws Exception {
    /// Grouping columns may be transform expressions, not just identifiers: ROLLUP(UPPER(d1)).
    String query =
        "SELECT UPPER(" + D1 + "), COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(UPPER(" + D1 + "))";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("A", 4L);
    expected.put("B", 4L);
    expected.put("NULL", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 3);
  }

  @Test
  public void testWhereFilterWithCube()
      throws Exception {
    /// The WHERE filter applies before grouping: with d2 = 'x' only 4 docs remain, and every CUBE(d1, d2)
    /// subtotal reflects the filtered counts.
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*), GROUPING_ID(" + D1 + ", " + D2 + ") FROM "
        + getTableName() + " WHERE " + D2 + " = 'x' GROUP BY CUBE(" + D1 + ", " + D2 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + cell(row, 1) + "|" + row.get(3).asInt(), row.get(2).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a|x|0", 2L);
    expected.put("b|x|0", 2L);
    expected.put("a|NULL|1", 2L);
    expected.put("b|NULL|1", 2L);
    expected.put("NULL|x|2", 4L);
    expected.put("NULL|NULL|3", 4L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 6);
  }

  @Test
  public void testCaseWhenGroupingRelabelsSubtotals()
      throws Exception {
    /// GROUPING() nested inside a post-aggregation transform: the canonical pattern for relabeling subtotal
    /// rows. CASE WHEN GROUPING(d1) = 1 THEN 'ALL' ELSE d1 END turns the rolled-up NULL into 'ALL'.
    String query = "SELECT CASE WHEN GROUPING(" + D1 + ") = 1 THEN 'ALL' ELSE " + D1 + " END, COUNT(*) FROM "
        + getTableName() + " GROUP BY ROLLUP(" + D1 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, Long> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0), row.get(1).asLong());
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("a", 4L);
    expected.put("b", 4L);
    expected.put("ALL", 8L);
    assertEquals(actual, expected);
    assertEquals(rows.size(), 3);
  }

  @Test
  public void testMultipleAggregationsWithOrderByAndLimit()
      throws Exception {
    /// Several aggregation types under one ROLLUP, with ORDER BY + LIMIT applied after the grouping-set
    /// expansion. Full result: (NULL, 8, 200, 8), (a, 4, 100, 4), (b, 4, 200, 4); LIMIT 2 keeps the first two.
    String query = "SELECT " + D1 + ", SUM(" + MET + "), MAX(" + LNG + "), COUNT(*) FROM " + getTableName()
        + " GROUP BY ROLLUP(" + D1 + ") ORDER BY COUNT(*) DESC, " + D1 + " LIMIT 2";
    JsonNode rows = runRows(query, true);
    assertEquals(rows.size(), 2);
    assertEquals(cell(rows.get(0), 0), "NULL");
    assertEquals(rows.get(0).get(1).asDouble(), 8.0);
    assertEquals(rows.get(0).get(2).asDouble(), 200.0);
    assertEquals(rows.get(0).get(3).asLong(), 8L);
    assertEquals(cell(rows.get(1), 0), "a");
    assertEquals(rows.get(1).get(1).asDouble(), 4.0);
    assertEquals(rows.get(1).get(2).asDouble(), 100.0);
    assertEquals(rows.get(1).get(3).asLong(), 4L);
  }

  @Test
  public void testDistinctCountUnderRollup()
      throws Exception {
    /// DISTINCTCOUNT(d1) recomputes per grouping set: 2 distinct d1 values in every d2 group and in the grand
    /// total. The genuine-NULL d2 group (GROUPING(d2)=0) stays distinct from the grand total (GROUPING(d2)=1).
    String query = "SELECT " + D2 + ", DISTINCTCOUNT(" + D1 + "), COUNT(*), GROUPING(" + D2 + ") FROM "
        + getTableName() + " GROUP BY ROLLUP(" + D2 + ")";
    JsonNode rows = runRows(query, true);
    Map<String, long[]> actual = new HashMap<>();
    for (JsonNode row : rows) {
      actual.put(cell(row, 0) + "|" + row.get(3).asInt(), new long[]{row.get(1).asLong(), row.get(2).asLong()});
    }
    assertEquals(rows.size(), 3);
    assertEquals(actual.get("x|0"), new long[]{2L, 4L});
    assertEquals(actual.get("NULL|0"), new long[]{2L, 4L});
    assertEquals(actual.get("NULL|1"), new long[]{2L, 8L});
  }

  @Test
  public void testHavingOnAggregationAndGrouping()
      throws Exception {
    /// HAVING may combine a regular aggregation with GROUPING(): keep only rolled-up-d2 rows with more than
    /// 4 docs — the per-d1 subtotals have exactly 4, so only the grand total survives.
    String query = "SELECT " + D1 + ", " + D2 + ", COUNT(*) FROM " + getTableName() + " GROUP BY ROLLUP(" + D1
        + ", " + D2 + ") HAVING GROUPING(" + D2 + ") = 1 AND COUNT(*) > 4";
    JsonNode rows = runRows(query, true);
    assertEquals(rows.size(), 1);
    assertEquals(cell(rows.get(0), 0), "NULL");
    assertEquals(cell(rows.get(0), 1), "NULL");
    assertEquals(rows.get(0).get(2).asLong(), 8L);
  }
}
