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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.ResultsBlockUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/**
 * End-to-end (in-process server execution + broker reduce) tests for SQL GROUPING SETS / ROLLUP / CUBE / GROUPING()
 * on the single-stage engine. Two identical segments are used so the cross-segment combine + broker merge paths are
 * exercised (values are therefore doubled relative to a single segment).
 */
public class GroupingSetsQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "GroupingSetsQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension("country", FieldSpec.DataType.STRING)
      .addSingleValueDimension("city", FieldSpec.DataType.STRING)
      .addSingleValueDimension("regionId", FieldSpec.DataType.LONG)
      .addSingleValueDimension("rating", FieldSpec.DataType.DOUBLE)
      .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
      .addMetric("sales", FieldSpec.DataType.INT)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    List<GenericRow> records = new ArrayList<>();
    records.add(row("US", "NY", 1L, 100.0, 10));
    records.add(row("US", "SF", 1L, 200.0, 20));
    records.add(row("CA", "TO", 2L, 300.0, 30));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private static GenericRow row(String country, String city, long regionId, double rating, int sales) {
    GenericRow record = new GenericRow();
    record.putValue("country", country);
    record.putValue("city", city);
    record.putValue("regionId", regionId);
    record.putValue("rating", rating);
    record.putValue("tags", new String[]{"a", "b"});
    record.putValue("sales", sales);
    return record;
  }

  /** Normalizes a result row to a stable string (null -> "null", numbers -> integral form) for set comparison. */
  private static String rowKey(Object[] row) {
    StringBuilder sb = new StringBuilder();
    for (Object value : row) {
      if (value == null) {
        sb.append("null");
      } else if (value instanceof Number) {
        sb.append(((Number) value).longValue());
      } else {
        sb.append(value);
      }
      sb.append('|');
    }
    return sb.toString();
  }

  private Set<String> resultRowKeys(String query) {
    return resultRowKeys(getBrokerResponse(query));
  }

  private Set<String> resultRowKeys(String query, Map<String, String> queryOptions) {
    return resultRowKeys(getBrokerResponse(query, queryOptions));
  }

  private static Set<String> resultRowKeys(BrokerResponseNative response) {
    ResultTable resultTable = response.getResultTable();
    Set<String> keys = new HashSet<>();
    for (Object[] row : resultTable.getRows()) {
      keys.add(rowKey(row));
    }
    return keys;
  }

  @Test
  public void testRollupWithGrouping() {
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales), GROUPING(country), GROUPING(city) "
        + "FROM testTable GROUP BY ROLLUP(country, city) LIMIT 100");
    // BaseQueriesTest runs 2 servers x 2 segments => values are 4x the single-segment values.
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|0|0|", "US|SF|80|0|0|", "CA|TO|120|0|0|",   // detail
        "US|null|120|0|1|", "CA|null|120|0|1|",               // per-country subtotal, city rolled up
        "null|null|240|1|1|"));                               // grand total
    assertEquals(actual, expected);
  }

  @Test
  public void testCube() {
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales) "
        + "FROM testTable GROUP BY CUBE(country, city) LIMIT 100");
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|", "US|SF|80|", "CA|TO|120|",   // (country, city)
        "US|null|120|", "CA|null|120|",           // (country)
        "null|NY|40|", "null|SF|80|", "null|TO|120|",  // (city)
        "null|null|240|"));                       // ()
    assertEquals(actual, expected);
  }

  @Test
  public void testExplicitGroupingSets() {
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales) "
        + "FROM testTable GROUP BY GROUPING SETS ((country, city), (country), ()) LIMIT 100");
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|", "US|SF|80|", "CA|TO|120|",
        "US|null|120|", "CA|null|120|",
        "null|null|240|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testGroupingId() {
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales), GROUPING_ID(country, city) "
        + "FROM testTable GROUP BY ROLLUP(country, city) LIMIT 100");
    // GROUPING_ID(country, city): (c,ci)->0, (c)->1, ()->3.
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|0|", "US|SF|80|0|", "CA|TO|120|0|",
        "US|null|120|1|", "CA|null|120|1|",
        "null|null|240|3|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testHavingOnGrouping() {
    // Keep only the per-country subtotals (city rolled up, country present).
    Set<String> actual = resultRowKeys("SELECT country, SUM(sales), GROUPING(city) FROM testTable "
        + "GROUP BY ROLLUP(country, city) HAVING GROUPING(city) = 1 AND GROUPING(country) = 0 LIMIT 100");
    Set<String> expected = new HashSet<>(Arrays.asList("US|120|1|", "CA|120|1|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testOrderByWithRollup() {
    // ORDER BY exercises the TableResizer (in-segment + indexed-table sort/trim) path.
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales) FROM testTable "
        + "GROUP BY ROLLUP(country, city) ORDER BY SUM(sales) DESC LIMIT 100");
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|", "US|SF|80|", "CA|TO|120|",
        "US|null|120|", "CA|null|120|",
        "null|null|240|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testOrderByGroupKeysWithRollup() {
    // ORDER BY on group-by keys with a small LIMIT routes through the safe-trim SortedRecordTable / SortedRecordsMerger
    // combine path (distinct from testOrderByWithRollup, which orders by an aggregate).
    Set<String> actual = resultRowKeys("SELECT country, city, SUM(sales) FROM testTable "
        + "GROUP BY ROLLUP(country, city) ORDER BY country, city LIMIT 10");
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|NY|40|", "US|SF|80|", "CA|TO|120|",
        "US|null|120|", "CA|null|120|",
        "null|null|240|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testEmptyServerSchemaIncludesGroupingId() {
    // A server with no matching segments must produce a schema with the $groupingId key column, matching populated
    // servers, or broker reduce sees inconsistent column counts.
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery(
            "SELECT country, city, SUM(sales) FROM testTable GROUP BY ROLLUP(country, city)"));
    DataSchema schema = ResultsBlockUtils.buildEmptyQueryResults(queryContext).getDataSchema();
    assertEquals(schema.size(), 4);   // country, city, $groupingId, sum
    assertEquals(schema.getColumnName(2), GroupingSets.GROUPING_ID_COLUMN);
    assertEquals(schema.getColumnDataType(2), DataSchema.ColumnDataType.INT);
  }

  @Test
  public void testOrdinaryGroupByUnaffected() {
    Set<String> actual = resultRowKeys("SELECT country, SUM(sales) FROM testTable GROUP BY country LIMIT 100");
    Set<String> expected = new HashSet<>(Arrays.asList("US|120|", "CA|120|"));
    assertEquals(actual, expected);
  }

  @Test
  public void testNullHandlingNumericGroupingSets() {
    // Null handling forces the no-dictionary (on-the-fly) key-generation path and exercises the STRING / LONG / DOUBLE
    // value resolvers; the results must match the dictionary path.
    Set<String> actual = resultRowKeys(
        "SELECT country, regionId, rating, SUM(sales) FROM testTable GROUP BY ROLLUP(country, regionId, rating) "
            + "LIMIT 100", Map.of("enableNullHandling", "true"));
    Set<String> expected = new HashSet<>(Arrays.asList(
        "US|1|100|40|", "US|1|200|80|", "CA|2|300|120|",   // (country, regionId, rating)
        "US|1|null|120|", "CA|2|null|120|",                // (country, regionId)
        "US|null|null|120|", "CA|null|null|120|",          // (country)
        "null|null|null|240|"));                           // ()
    assertEquals(actual, expected);
  }

  @Test
  public void testMultiValueGroupByRejected() {
    // Multi-valued group-by columns are not supported with grouping sets; expect a clear failure, not wrong results.
    try {
      BrokerResponseNative response =
          getBrokerResponse("SELECT tags, SUM(sales) FROM testTable GROUP BY ROLLUP(tags) LIMIT 100");
      assertFalse(response.getExceptions() == null || response.getExceptions().isEmpty(),
          "Expected an exception for a multi-valued group-by column with grouping sets");
    } catch (RuntimeException e) {
      // Acceptable: the rejection may also surface as a thrown exception.
    }
  }

  @AfterClass
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
