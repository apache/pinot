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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.offheap.ResourceTrackingGroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/// End-to-end differential test for off-heap SSE GROUP BY (`SET groupByOffHeap=true`).
///
/// Every query in the battery is executed twice over the same segments — once on-heap and once with the
/// `groupByOffHeap` query option — and the result rows must match (as unordered multisets for un-ordered
/// queries, exact ordered lists for ORDER BY queries). Numeric exactness: all FLOAT/DOUBLE/BIG_DECIMAL values in
/// the fixtures are dyadic rationals (multiples of 0.25/0.5), so double aggregation is exact and independent of
/// combine order, allowing exact value comparison.
///
/// The fixture cardinalities are chosen against the holder-selection logic in the
/// [DictionaryBasedGroupKeyGenerator][org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator]
/// constructor (arrayBasedThreshold = 10_000, default numGroupsLimit = 100_000):
/// - `dLowStr(10) * dInt(1000) = 10_000` — not > threshold -> ARRAY_BASED (stays on-heap by design)
/// - `dHighStr(20_000)` alone or with others up to `4 * 10^8` -> INT_MAP_BASED
/// - `dHighStr * dHighInt * dLowStr = 4 * 10^9 > Integer.MAX_VALUE` -> LONG_MAP_BASED
/// - `dHighStr * dHighInt * dHighInt2 * dHighLong * dInt = 1.6 * 10^20 > Long.MAX_VALUE` -> ARRAY_MAP_BASED
///
/// After every off-heap execution the test asserts that [PinotDataBuffer#getDirectBufferUsage()] returns to the
/// baseline captured after a warm-up query, proving the close path releases all direct memory. (The baseline is
/// captured after one warm-up off-heap query instead of right after segment load so that any lazily allocated
/// direct buffer elsewhere in the query stack cannot move the baseline mid-test; segments are loaded with mmap,
/// which is not counted as direct usage.)
///
/// Null handling runs against a second small segment fixture (built in the same class) whose columns carry nulls
/// in three patterns: first row null, null only mid-stream (with new values appearing after the null), and all
/// nulls — exercising the off-heap "null shift" logic in NoDictionarySingleColumnGroupKeyGenerator. Since the
/// on-heap null-group counting fix (both modes now count the primitive-type null group in getNumKeys()), the
/// numGroupsLimitReached flag is compared between the two modes on every query, null fixtures included.
public class OffHeapGroupByQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "OffHeapGroupByQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String NULL_SEGMENT_NAME = "testNullSegment";
  private static final long RANDOM_SEED = 42;
  private static final int NUM_RECORDS = 50_000;
  private static final int NUM_NULL_RECORDS = 1_200;
  private static final String OFF_HEAP_PREFIX = "SET groupByOffHeap=true; ";

  // Dict-encoded SV columns
  private static final String D_LOW_STR = "dLowStr";      // cardinality 10
  private static final String D_INT = "dInt";             // cardinality 1000
  private static final String D_HIGH_STR = "dHighStr";    // cardinality 20_000
  private static final String D_HIGH_INT = "dHighInt";    // cardinality 20_000
  private static final String D_HIGH_INT2 = "dHighInt2";  // cardinality 20_000
  private static final String D_HIGH_LONG = "dHighLong";  // cardinality 20_000
  // Raw (no-dictionary) SV columns
  private static final String RAW_INT = "rawInt";
  private static final String RAW_LONG = "rawLong";
  private static final String RAW_FLOAT = "rawFloat";
  private static final String RAW_DOUBLE = "rawDouble";
  private static final String RAW_STRING = "rawString";
  private static final String RAW_BYTES = "rawBytes";
  private static final String RAW_BIG_DECIMAL = "rawBigDecimal";
  // Dict-encoded MV columns
  private static final String MV_INT = "mvInt";           // cardinality 50
  private static final String MV_STR = "mvStr";           // cardinality 30
  private static final String MV_HIGH_INT = "mvHighInt";  // cardinality ~15_000 (forces the IntMap MV path)
  // Metric
  private static final String METRIC = "metric";

  // Null-segment columns: "nf" = first row is null, "nm" = nulls only mid-stream (new values appear after the
  // null stretch), "an" = all rows null
  private static final String[] NULL_TYPE_SUFFIXES = {"Int", "Long", "Float", "Double", "Str", "Bytes", "BigDecimal"};
  private static final String AN_INT = "anInt";
  private static final String AN_STR = "anStr";
  private static final String NS_METRIC = "nsMetric";

  private IndexSegment _mainSegment;
  private IndexSegment _nullSegment;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private long _directBufferBaseline;

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
    FileUtils.deleteDirectory(INDEX_DIR);
    buildMainSegment();
    buildNullSegment();
    _mainSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _nullSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, NULL_SEGMENT_NAME), ReadMode.mmap);
    useMainSegment();

    // Warm-up off-heap query, then capture the direct-buffer baseline (see class doc for why the baseline is
    // captured after the warm-up instead of right after segment load)
    BrokerResponseNative warmUp =
        getBrokerResponse(OFF_HEAP_PREFIX + "SELECT dLowStr, COUNT(*) FROM testTable GROUP BY dLowStr LIMIT 100");
    assertTrue(warmUp.getExceptions().isEmpty(), "Warm-up query failed: " + warmUp.getExceptions());
    _directBufferBaseline = PinotDataBuffer.getDirectBufferUsage();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _mainSegment.destroy();
    _nullSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  private void useMainSegment() {
    _indexSegment = _mainSegment;
    _indexSegments = Arrays.asList(_mainSegment, _mainSegment);
  }

  private void useNullSegment() {
    _indexSegment = _nullSegment;
    _indexSegments = Arrays.asList(_nullSegment, _nullSegment);
  }

  private void buildMainSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(D_LOW_STR, DataType.STRING)
        .addSingleValueDimension(D_INT, DataType.INT)
        .addSingleValueDimension(D_HIGH_STR, DataType.STRING)
        .addSingleValueDimension(D_HIGH_INT, DataType.INT)
        .addSingleValueDimension(D_HIGH_INT2, DataType.INT)
        .addSingleValueDimension(D_HIGH_LONG, DataType.LONG)
        .addSingleValueDimension(RAW_INT, DataType.INT)
        .addSingleValueDimension(RAW_LONG, DataType.LONG)
        .addSingleValueDimension(RAW_FLOAT, DataType.FLOAT)
        .addSingleValueDimension(RAW_DOUBLE, DataType.DOUBLE)
        .addSingleValueDimension(RAW_STRING, DataType.STRING)
        .addSingleValueDimension(RAW_BYTES, DataType.BYTES)
        .addSingleValueDimension(RAW_BIG_DECIMAL, DataType.BIG_DECIMAL)
        .addMultiValueDimension(MV_INT, DataType.INT)
        .addMultiValueDimension(MV_STR, DataType.STRING)
        .addMultiValueDimension(MV_HIGH_INT, DataType.INT)
        .addMetric(METRIC, DataType.DOUBLE)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList(RAW_INT, RAW_LONG, RAW_FLOAT, RAW_DOUBLE, RAW_STRING, RAW_BYTES,
            RAW_BIG_DECIMAL))
        .build();

    Random random = new Random(RANDOM_SEED);
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(D_LOW_STR, "low_" + random.nextInt(10));
      record.putValue(D_INT, random.nextInt(1000));
      record.putValue(D_HIGH_STR, "high_" + (i % 20_000));
      record.putValue(D_HIGH_INT, (i * 7 + 13) % 20_000);
      record.putValue(D_HIGH_INT2, (i * 11 + 5) % 20_000);
      record.putValue(D_HIGH_LONG, (i * 13L + 3) % 20_000);
      record.putValue(RAW_INT, random.nextInt(500) - 250);
      record.putValue(RAW_LONG, (random.nextInt(500) - 250) * 1_000_003L);
      // FLOAT/DOUBLE: dyadic values with identical values across rows, plus both -0.0 and +0.0
      int floatStep = random.nextInt(201) - 100;
      record.putValue(RAW_FLOAT, floatStep == 0 && random.nextBoolean() ? -0.0f : floatStep * 0.25f);
      int doubleStep = random.nextInt(201) - 100;
      record.putValue(RAW_DOUBLE, doubleStep == 0 && random.nextBoolean() ? -0.0d : doubleStep * 0.5d);
      // A small fraction of strings carry surrogate pairs to exercise the 4-byte UTF-8 encoding path
      int stringId = random.nextInt(300);
      record.putValue(RAW_STRING, stringId < 10 ? "raw_😀_" + stringId : "raw_" + stringId);
      record.putValue(RAW_BYTES, ByteBuffer.allocate(8).putLong(random.nextInt(150) * 0x9E3779B97F4AL).array());
      record.putValue(RAW_BIG_DECIMAL, BigDecimal.valueOf((random.nextInt(400) - 200) * 25L, 2));
      record.putValue(MV_INT, randomMvValues(random, 50, null));
      record.putValue(MV_STR, randomMvValues(random, 30, "mv_"));
      record.putValue(MV_HIGH_INT, randomMvValues(random, 15_000, null));
      record.putValue(METRIC, (random.nextInt(2001) - 1000) * 0.25d);
      records.add(record);
    }
    buildSegment(tableConfig, schema, records, SEGMENT_NAME, false);
  }

  private static Object[] randomMvValues(Random random, int cardinality, String stringPrefix) {
    int numValues = 1 + random.nextInt(3);
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      int value = random.nextInt(cardinality);
      values[i] = stringPrefix != null ? stringPrefix + value : value;
    }
    return values;
  }

  private void buildNullSegment()
      throws Exception {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME);
    List<String> noDictionaryColumns = new ArrayList<>();
    DataType[] dataTypes = {
        DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.STRING, DataType.BYTES,
        DataType.BIG_DECIMAL
    };
    for (int i = 0; i < NULL_TYPE_SUFFIXES.length; i++) {
      String nullFirstColumn = "nf" + NULL_TYPE_SUFFIXES[i];
      String nullMidColumn = "nm" + NULL_TYPE_SUFFIXES[i];
      schemaBuilder.addSingleValueDimension(nullFirstColumn, dataTypes[i]);
      schemaBuilder.addSingleValueDimension(nullMidColumn, dataTypes[i]);
      noDictionaryColumns.add(nullFirstColumn);
      noDictionaryColumns.add(nullMidColumn);
    }
    schemaBuilder.addSingleValueDimension(AN_INT, DataType.INT);
    schemaBuilder.addSingleValueDimension(AN_STR, DataType.STRING);
    noDictionaryColumns.add(AN_INT);
    noDictionaryColumns.add(AN_STR);
    schemaBuilder.addMetric(NS_METRIC, DataType.DOUBLE);
    Schema schema = schemaBuilder.build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns).build();

    List<GenericRow> records = new ArrayList<>(NUM_NULL_RECORDS);
    for (int i = 0; i < NUM_NULL_RECORDS; i++) {
      GenericRow record = new GenericRow();
      // "nf" columns: nulls sprinkled from row 0 on; values from an 9-value pool
      boolean nullFirstIsNull = i % 5 == 0;
      int nullFirstPoolIndex = i % 9;
      // "nm" columns: values only (8-value pool prefix) until row 600, nulls for rows [600, 700), then the full
      // 11-value pool so that pool indexes 7-10 first appear after the null stretch (700 % 11 == 7)
      boolean nullMidIsNull = i >= 600 && i < 700;
      int nullMidPoolIndex = i < 600 ? i % 7 : i % 11;
      for (int t = 0; t < NULL_TYPE_SUFFIXES.length; t++) {
        record.putValue("nf" + NULL_TYPE_SUFFIXES[t], nullFirstIsNull ? null : nullPoolValue(t, nullFirstPoolIndex));
        record.putValue("nm" + NULL_TYPE_SUFFIXES[t], nullMidIsNull ? null : nullPoolValue(t, nullMidPoolIndex));
      }
      record.putValue(AN_INT, null);
      record.putValue(AN_STR, null);
      record.putValue(NS_METRIC, (i % 41 - 20) * 0.25d);
      records.add(record);
    }
    buildSegment(tableConfig, schema, records, NULL_SEGMENT_NAME, true);
  }

  private static Object nullPoolValue(int typeIndex, int poolIndex) {
    switch (typeIndex) {
      case 0:
        return poolIndex * 3 - 15;
      case 1:
        return poolIndex * 997L;
      case 2:
        return (poolIndex - 5) * 0.25f;
      case 3:
        return (poolIndex - 5) * 0.5d;
      case 4:
        return "ns_" + poolIndex;
      case 5:
        return new byte[]{(byte) poolIndex, (byte) (poolIndex + 1), (byte) (poolIndex * 2), 7};
      case 6:
        return BigDecimal.valueOf((poolIndex - 5) * 25L, 2);
      default:
        throw new IllegalStateException();
    }
  }

  private void buildSegment(TableConfig tableConfig, Schema schema, List<GenericRow> records, String segmentName,
      boolean nullHandlingEnabled)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(nullHandlingEnabled);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
  }

  // ---------------------------------------------------------------------------------------------
  // Comparison helpers
  // ---------------------------------------------------------------------------------------------

  private BrokerResponseNative runQuery(String query) {
    BrokerResponseNative response = getBrokerResponse(query);
    assertTrue(response.getExceptions().isEmpty(),
        "Query failed: " + query + " with exceptions: " + response.getExceptions());
    return response;
  }

  /// Runs the query on-heap and off-heap, asserts direct memory returns to the baseline after the off-heap run,
  /// and returns both responses for row comparison.
  private BrokerResponseNative[] runBothModes(String query) {
    BrokerResponseNative onHeap = runQuery(query);
    BrokerResponseNative offHeap = runQuery(OFF_HEAP_PREFIX + query);
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), _directBufferBaseline,
        "Off-heap direct memory leaked by query: " + query);
    return new BrokerResponseNative[]{onHeap, offHeap};
  }

  /// Unordered comparison: rows compared as multisets (canonical string form, sorted). The numGroupsLimitReached
  /// flag must match between the two modes for every query (both modes count groups identically, including the
  /// primitive-type null group since the on-heap counting fix).
  private void testQuery(String query) {
    BrokerResponseNative[] responses = runBothModes(query);
    assertEquals(responses[1].isNumGroupsLimitReached(), responses[0].isNumGroupsLimitReached(),
        "numGroupsLimitReached mismatch between modes for query: " + query);
    List<String> onHeapRows = canonicalRows(responses[0].getResultTable(), query);
    List<String> offHeapRows = canonicalRows(responses[1].getResultTable(), query);
    Collections.sort(onHeapRows);
    Collections.sort(offHeapRows);
    assertEquals(offHeapRows, onHeapRows, "Result mismatch (unordered) for query: " + query);
  }

  /// Ordered comparison for ORDER BY queries: exact ordered row lists.
  private void testOrderedQuery(String query) {
    BrokerResponseNative[] responses = runBothModes(query);
    List<String> onHeapRows = canonicalRows(responses[0].getResultTable(), query);
    List<String> offHeapRows = canonicalRows(responses[1].getResultTable(), query);
    assertEquals(offHeapRows, onHeapRows, "Result mismatch (ordered) for query: " + query);
  }

  /// Unordered comparison plus an assertion of the expected numGroupsLimitReached value in both modes.
  private void testCappedQuery(String query, boolean expectLimitReached) {
    BrokerResponseNative[] responses = runBothModes(query);
    assertEquals(responses[0].isNumGroupsLimitReached(), expectLimitReached,
        "Unexpected on-heap numGroupsLimitReached for query: " + query);
    assertEquals(responses[1].isNumGroupsLimitReached(), expectLimitReached,
        "Unexpected off-heap numGroupsLimitReached for query: " + query);
    List<String> onHeapRows = canonicalRows(responses[0].getResultTable(), query);
    List<String> offHeapRows = canonicalRows(responses[1].getResultTable(), query);
    Collections.sort(onHeapRows);
    Collections.sort(offHeapRows);
    assertEquals(offHeapRows, onHeapRows, "Result mismatch (capped) for query: " + query);
  }

  private static List<String> canonicalRows(ResultTable resultTable, String query) {
    assertTrue(resultTable != null, "Missing result table for query: " + query);
    List<Object[]> rows = resultTable.getRows();
    // Every query in the battery matches at least one group; an empty result would make the differential
    // comparison pass vacuously
    assertFalse(rows.isEmpty(), "Empty result rows for query: " + query);
    List<String> canonicalRows = new ArrayList<>(rows.size());
    StringBuilder builder = new StringBuilder();
    for (Object[] row : rows) {
      builder.setLength(0);
      for (Object cell : row) {
        builder.append(canonicalCell(cell)).append('|');
      }
      canonicalRows.add(builder.toString());
    }
    return canonicalRows;
  }

  private static String canonicalCell(Object cell) {
    if (cell == null) {
      return "null";
    }
    if (cell instanceof byte[]) {
      return "bytes:" + BytesUtils.toHexString((byte[]) cell);
    }
    if (cell instanceof Object[]) {
      return "array:" + Arrays.deepToString((Object[]) cell);
    }
    if (cell instanceof int[]) {
      return "ints:" + Arrays.toString((int[]) cell);
    }
    if (cell instanceof long[]) {
      return "longs:" + Arrays.toString((long[]) cell);
    }
    if (cell instanceof double[]) {
      return "doubles:" + Arrays.toString((double[]) cell);
    }
    // Include the type so that a value/type flip between modes cannot cancel out in the string form
    return cell.getClass().getSimpleName() + ':' + cell;
  }

  private static String aggregations(String metricColumn) {
    return "COUNT(*), SUM(" + metricColumn + "), MIN(" + metricColumn + "), MAX(" + metricColumn + "), AVG("
        + metricColumn + "), DISTINCTCOUNT(dInt)";
  }

  // ---------------------------------------------------------------------------------------------
  // Query battery
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testSingleColumnGroupBy() {
    String[] columns = {
        D_LOW_STR, D_INT, D_HIGH_STR, D_HIGH_INT, D_HIGH_INT2, D_HIGH_LONG, RAW_INT, RAW_LONG, RAW_FLOAT, RAW_DOUBLE,
        RAW_STRING, RAW_BYTES, RAW_BIG_DECIMAL
    };
    for (String column : columns) {
      testQuery("SELECT " + column + ", " + aggregations(METRIC) + " FROM testTable GROUP BY " + column
          + " LIMIT 100000");
    }
  }

  @Test
  public void testDictionaryMultiColumnVariants() {
    // ARRAY_BASED: 10 * 1000 = 10_000, not above the array-based threshold
    testQuery("SELECT dLowStr, dInt, COUNT(*), SUM(metric) FROM testTable GROUP BY dLowStr, dInt LIMIT 100000");
    // INT_MAP_BASED: 20_000 * 10 = 200_000
    testQuery("SELECT dHighStr, dLowStr, COUNT(*), SUM(metric) FROM testTable GROUP BY dHighStr, dLowStr"
        + " LIMIT 100000");
    // INT_MAP_BASED with a large product: 20_000 * 20_000 = 4 * 10^8 < Integer.MAX_VALUE
    testQuery("SELECT dHighStr, dHighInt, COUNT(*), SUM(metric) FROM testTable GROUP BY dHighStr, dHighInt"
        + " LIMIT 100000");
    // LONG_MAP_BASED: 20_000 * 20_000 * 10 = 4 * 10^9 > Integer.MAX_VALUE
    testQuery("SELECT dHighStr, dHighInt, dLowStr, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY dHighStr, dHighInt, dLowStr LIMIT 100000");
    // LONG_MAP_BASED: 20_000^3 * 1000 = 8 * 10^15 < Long.MAX_VALUE
    testQuery("SELECT dHighInt, dHighInt2, dHighLong, dInt, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY dHighInt, dHighInt2, dHighLong, dInt LIMIT 100000");
    // ARRAY_MAP_BASED: 20_000^4 * 1000 = 1.6 * 10^20 > Long.MAX_VALUE (long overflow)
    testQuery("SELECT dHighStr, dHighInt, dHighInt2, dHighLong, dInt, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY dHighStr, dHighInt, dHighInt2, dHighLong, dInt LIMIT 100000");
  }

  @Test
  public void testRawAndMixedMultiColumn() {
    testQuery("SELECT rawInt, rawString, COUNT(*), SUM(metric) FROM testTable GROUP BY rawInt, rawString"
        + " LIMIT 100000");
    testQuery("SELECT rawFloat, rawDouble, COUNT(*), SUM(metric) FROM testTable GROUP BY rawFloat, rawDouble"
        + " LIMIT 100000");
    testQuery("SELECT rawLong, rawBytes, rawBigDecimal, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY rawLong, rawBytes, rawBigDecimal LIMIT 100000");
    // Mixed raw + dict
    testQuery("SELECT rawString, dLowStr, COUNT(*), SUM(metric) FROM testTable GROUP BY rawString, dLowStr"
        + " LIMIT 100000");
    testQuery("SELECT dHighStr, rawInt, COUNT(*), SUM(metric) FROM testTable GROUP BY dHighStr, rawInt"
        + " LIMIT 200000");
  }

  @Test
  public void testMultiValueGroupBy() {
    testQuery("SELECT mvInt, " + aggregations(METRIC) + " FROM testTable GROUP BY mvInt LIMIT 100000");
    testQuery("SELECT mvStr, " + aggregations(METRIC) + " FROM testTable GROUP BY mvStr LIMIT 100000");
    // Single high-cardinality MV column: 15_000 > 10_000 forces the IntMap MV path
    testQuery("SELECT mvHighInt, COUNT(*), SUM(metric) FROM testTable GROUP BY mvHighInt LIMIT 100000");
    // MV + MV (ARRAY_BASED: 50 * 30 = 1500)
    testQuery("SELECT mvInt, mvStr, COUNT(*), SUM(metric) FROM testTable GROUP BY mvInt, mvStr LIMIT 100000");
    // MV + SV dict combos across holder variants
    testQuery("SELECT mvInt, dLowStr, COUNT(*), SUM(metric) FROM testTable GROUP BY mvInt, dLowStr LIMIT 100000");
    // INT_MAP_BASED MV: 50 * 20_000 = 10^6
    testQuery("SELECT mvInt, dHighStr, COUNT(*), SUM(metric) FROM testTable GROUP BY mvInt, dHighStr LIMIT 200000");
    // LONG_MAP_BASED MV: 50 * 20_000 * 20_000 = 2 * 10^10
    testQuery("SELECT mvInt, dHighStr, dHighInt, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY mvInt, dHighStr, dHighInt LIMIT 200000");
    // ARRAY_MAP_BASED MV: 50 * 20_000^4 * 1000 = 8 * 10^21 (long overflow)
    testQuery("SELECT mvInt, dHighStr, dHighInt, dHighInt2, dHighLong, dInt, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY mvInt, dHighStr, dHighInt, dHighInt2, dHighLong, dInt LIMIT 200000");
    // MV + raw SV -> NoDictionaryMultiColumn MV path
    testQuery("SELECT mvStr, rawInt, COUNT(*), SUM(metric) FROM testTable GROUP BY mvStr, rawInt LIMIT 200000");
  }

  @Test
  public void testOrderByTrimPath() {
    testOrderedQuery("SELECT dHighStr, COUNT(*) FROM testTable GROUP BY dHighStr"
        + " ORDER BY COUNT(*) DESC, dHighStr LIMIT 10");
    testOrderedQuery("SELECT rawString, SUM(metric) FROM testTable GROUP BY rawString"
        + " ORDER BY SUM(metric) DESC, rawString LIMIT 10");
    testOrderedQuery("SELECT dLowStr, dInt, MAX(metric) FROM testTable GROUP BY dLowStr, dInt"
        + " ORDER BY MAX(metric) DESC, dLowStr, dInt LIMIT 10");
    testOrderedQuery("SELECT rawFloat, COUNT(*) FROM testTable GROUP BY rawFloat ORDER BY rawFloat LIMIT 20");
    testOrderedQuery("SELECT mvInt, SUM(metric) FROM testTable GROUP BY mvInt ORDER BY SUM(metric) DESC, mvInt"
        + " LIMIT 10");
    testOrderedQuery("SELECT dHighInt, rawBigDecimal, MIN(metric) FROM testTable GROUP BY dHighInt, rawBigDecimal"
        + " ORDER BY MIN(metric), dHighInt, rawBigDecimal LIMIT 15");
  }

  @Test
  public void testFilteredGroupBy() {
    String[] queries = {
        "SELECT dInt, COUNT(*), SUM(metric) FROM testTable WHERE dInt IN (1, 2, 3, 4, 5) GROUP BY dInt LIMIT 1000",
        "SELECT rawInt, COUNT(*), SUM(metric) FROM testTable WHERE rawInt IN (0, 1, 2, 3) GROUP BY rawInt"
            + " LIMIT 1000",
        "SELECT dLowStr, dInt, COUNT(*) FROM testTable WHERE dLowStr IN ('low_1', 'low_2') AND dInt = 7"
            + " GROUP BY dLowStr, dInt LIMIT 1000",
        "SELECT dHighStr, COUNT(*) FROM testTable WHERE dHighStr IN ('high_1', 'high_2', 'high_3')"
            + " GROUP BY dHighStr LIMIT 1000"
    };
    for (String query : queries) {
      testQuery(query);
    }
  }

  @Test
  public void testFilteredGroupByWithOptimizedResultHolderCapacity() {
    // Predicate-based upper-bound optimization variants. NOTE: a pre-existing (upstream, on-heap) bug limits what
    // can be exercised here for dictionary columns: with optimizeMaxInitialResultHolderCapacity the predicate
    // sizes shrink cardinalityProduct/_globalGroupIdUpperBound to the IN-list size, which selects the
    // ArrayBasedHolder — whose group ids are raw dictionary-id products. Any matching dictionary id >= the
    // optimized bound then throws ArrayIndexOutOfBoundsException in BOTH modes (the T0 ArrayBased path is on-heap
    // by design even when groupByOffHeap is set). e.g. `WHERE dInt IN (1, 2, 3, 4, 5)` fails with
    // "Index 5 out of bounds for length 5" on this fixture with or without the off-heap option. The dict variant
    // below therefore uses IN (0..4), whose dictionary ids (0..4 — dInt values are 0..999, sorted) fit inside the
    // optimized bound; raw columns are unaffected (the no-dict generators only cap their key maps).
    testQuery("SET optimizeMaxInitialResultHolderCapacity=true; SELECT dInt, COUNT(*), SUM(metric) FROM testTable"
        + " WHERE dInt IN (0, 1, 2, 3, 4) GROUP BY dInt LIMIT 1000");
    testQuery("SET optimizeMaxInitialResultHolderCapacity=true; SELECT rawInt, COUNT(*), SUM(metric)"
        + " FROM testTable WHERE rawInt IN (0, 1, 2, 3) GROUP BY rawInt LIMIT 1000");
    testQuery("SET optimizeMaxInitialResultHolderCapacity=true; SELECT rawString, COUNT(*) FROM testTable"
        + " WHERE rawString IN ('raw_11', 'raw_12') GROUP BY rawString LIMIT 1000");
    testQuery("SET optimizeMaxInitialResultHolderCapacity=true; SELECT rawInt, rawString, COUNT(*) FROM testTable"
        + " WHERE rawInt IN (0, 1, 2, 3) AND rawString IN ('raw_11', 'raw_12') GROUP BY rawInt, rawString"
        + " LIMIT 1000");
  }

  @Test
  public void testFilteredAggregations() {
    // FILTER clauses share one group key generator across executors; with off-heap enabled all executors register
    // their off-heap holders on the same resource-tracking wrapper, closed once
    testQuery("SELECT dLowStr, COUNT(*) FILTER (WHERE dInt > 500), SUM(metric) FILTER (WHERE rawInt > 0),"
        + " COUNT(*) FROM testTable GROUP BY dLowStr LIMIT 1000");
    testQuery("SELECT rawString, COUNT(*) FILTER (WHERE rawDouble > 0), MIN(metric) FILTER (WHERE dInt < 100)"
        + " FROM testTable GROUP BY rawString LIMIT 100000");
    testQuery("SELECT dHighStr, SUM(metric) FILTER (WHERE dLowStr = 'low_3'), COUNT(*) FROM testTable"
        + " GROUP BY dHighStr LIMIT 100000");
  }

  @Test
  public void testNumGroupsLimitCap() {
    // Cap semantics parity: group ids assign in row order, so the kept groups (and rows) must be identical, and
    // the numGroupsLimitReached flag must match (no null groups on this fixture, so no counting divergence)
    testCappedQuery("SET numGroupsLimit=100; SELECT dHighStr, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY dHighStr LIMIT 100000", true);
    testCappedQuery("SET numGroupsLimit=100; SELECT rawString, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY rawString LIMIT 100000", true);
    testCappedQuery("SET numGroupsLimit=100; SELECT dHighStr, dHighInt, COUNT(*) FROM testTable"
        + " GROUP BY dHighStr, dHighInt LIMIT 100000", true);
    testCappedQuery("SET numGroupsLimit=100; SELECT rawInt, rawString, COUNT(*) FROM testTable"
        + " GROUP BY rawInt, rawString LIMIT 100000", true);
    testCappedQuery("SET numGroupsLimit=100; SELECT mvHighInt, COUNT(*) FROM testTable GROUP BY mvHighInt"
        + " LIMIT 100000", true);
    // Limit not reached: flag must be false in both modes
    testCappedQuery("SET numGroupsLimit=100; SELECT dLowStr, COUNT(*) FROM testTable GROUP BY dLowStr LIMIT 1000",
        false);
  }

  @Test
  public void testNullHandlingEnabledWithoutNulls() {
    // enableNullHandling routes all group-bys through the NoDictionary generators even for dict columns; this
    // fixture has no null rows, so results must match the null-disabled semantics-compatible comparison
    String[] columns = {D_INT, D_LOW_STR, D_HIGH_STR, RAW_INT, RAW_STRING, RAW_FLOAT, RAW_BYTES, RAW_BIG_DECIMAL};
    for (String column : columns) {
      testQuery("SET enableNullHandling=true; SELECT " + column + ", COUNT(*), SUM(metric) FROM testTable GROUP BY "
          + column + " LIMIT 100000");
    }
    testQuery("SET enableNullHandling=true; SELECT rawInt, rawString, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY rawInt, rawString LIMIT 100000");
    testQuery("SET enableNullHandling=true; SELECT dLowStr, rawDouble, COUNT(*), SUM(metric) FROM testTable"
        + " GROUP BY dLowStr, rawDouble LIMIT 100000");
    // MV group-by with null handling -> NoDictionarySingleColumn MV path
    testQuery("SET enableNullHandling=true; SELECT mvInt, COUNT(*), SUM(metric) FROM testTable GROUP BY mvInt"
        + " LIMIT 100000");
    testQuery("SET enableNullHandling=true; SELECT mvInt, rawString, COUNT(*) FROM testTable"
        + " GROUP BY mvInt, rawString LIMIT 200000");
  }

  /// Guards the whole differential battery against passing vacuously: proves the `groupByOffHeap` query option
  /// actually reaches the QueryContext through the plan maker, and that DefaultGroupByExecutor reacts to the
  /// flag by wrapping the generator in the off-heap resource tracker (allocating direct memory) — so a broken
  /// option plumbing cannot silently turn every off-heap run in this test into an on-heap run.
  @Test
  public void testGroupByOffHeapOptionPlumbing() {
    // Query option -> QueryContext (InstancePlanMakerImplV2#applyQueryOptions)
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        OFF_HEAP_PREFIX + "SELECT dHighStr, COUNT(*) FROM testTable GROUP BY dHighStr");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      PLAN_MAKER.makeInstancePlan(List.of(new SegmentContext(_mainSegment)), queryContext, EXECUTOR_SERVICE);
    }
    assertTrue(queryContext.isGroupByOffHeap(), "groupByOffHeap query option did not reach the QueryContext");

    // QueryContext flag -> off-heap executor wiring
    ExpressionContext[] groupByExpressions = {ExpressionContext.forIdentifier(D_HIGH_STR)};
    QueryContext offHeapContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY dHighStr");
    offHeapContext.setGroupByOffHeap(true);
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(new SegmentContext(_mainSegment), offHeapContext,
        Arrays.asList(groupByExpressions), DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    DefaultGroupByExecutor executor = new DefaultGroupByExecutor(offHeapContext, groupByExpressions, projectOperator);
    try {
      assertTrue(executor.getGroupKeyGenerator() instanceof ResourceTrackingGroupKeyGenerator,
          "Off-heap group-by must wrap the generator in ResourceTrackingGroupKeyGenerator");
      assertTrue(PinotDataBuffer.getDirectBufferUsage() > _directBufferBaseline,
          "Off-heap group-by executor did not allocate direct memory");
    } finally {
      executor.getGroupKeyGenerator().close();
    }
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), _directBufferBaseline,
        "Off-heap executor leaked direct memory after close");

    // Negative: with the flag off, the generator must not be wrapped
    QueryContext onHeapContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY dHighStr");
    BaseProjectOperator<?> onHeapProjectOperator = new ProjectPlanNode(new SegmentContext(_mainSegment),
        onHeapContext, Arrays.asList(groupByExpressions), DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    DefaultGroupByExecutor onHeapExecutor =
        new DefaultGroupByExecutor(onHeapContext, groupByExpressions, onHeapProjectOperator);
    try {
      assertFalse(onHeapExecutor.getGroupKeyGenerator() instanceof ResourceTrackingGroupKeyGenerator,
          "On-heap group-by must not wrap the generator in ResourceTrackingGroupKeyGenerator");
    } finally {
      onHeapExecutor.getGroupKeyGenerator().close();
    }
  }

  @Test
  public void testNullHandlingWithNulls() {
    useNullSegment();
    try {
      String aggs = "COUNT(*), SUM(nsMetric), MIN(nsMetric), MAX(nsMetric)";
      // Single-column group-by on every type with the FIRST row null and with nulls only mid-stream
      for (String suffix : NULL_TYPE_SUFFIXES) {
        testQuery("SET enableNullHandling=true; SELECT nf" + suffix + ", " + aggs + " FROM testTable GROUP BY nf"
            + suffix + " LIMIT 1000");
        testQuery("SET enableNullHandling=true; SELECT nm" + suffix + ", " + aggs + " FROM testTable GROUP BY nm"
            + suffix + " LIMIT 1000");
      }
      // All-null columns
      testQuery("SET enableNullHandling=true; SELECT anInt, " + aggs + " FROM testTable GROUP BY anInt LIMIT 1000");
      testQuery("SET enableNullHandling=true; SELECT anStr, " + aggs + " FROM testTable GROUP BY anStr LIMIT 1000");
      // Multi-column group-bys with nulls
      testQuery("SET enableNullHandling=true; SELECT nfInt, nmStr, " + aggs + " FROM testTable GROUP BY nfInt, nmStr"
          + " LIMIT 1000");
      testQuery("SET enableNullHandling=true; SELECT nmInt, nmLong, " + aggs + " FROM testTable"
          + " GROUP BY nmInt, nmLong LIMIT 1000");
      testQuery("SET enableNullHandling=true; SELECT anInt, nfStr, " + aggs + " FROM testTable GROUP BY anInt, nfStr"
          + " LIMIT 1000");
      // Ordered null query (null key ordering is deterministic in both modes)
      testOrderedQuery("SET enableNullHandling=true; SELECT nmInt, COUNT(*) FROM testTable GROUP BY nmInt"
          + " ORDER BY COUNT(*) DESC, nmInt LIMIT 5");
      // Null + tiny numGroupsLimit: rows and the cross-mode numGroupsLimitReached flag are compared (both modes
      // count the null group since the on-heap counting fix)
      testQuery("SET enableNullHandling=true; SET numGroupsLimit=5; SELECT nmInt, COUNT(*) FROM testTable"
          + " GROUP BY nmInt LIMIT 1000");
      testQuery("SET enableNullHandling=true; SET numGroupsLimit=5; SELECT nfStr, COUNT(*) FROM testTable"
          + " GROUP BY nfStr LIMIT 1000");
      testQuery("SET enableNullHandling=true; SET numGroupsLimit=3; SELECT nfInt, nmStr, COUNT(*) FROM testTable"
          + " GROUP BY nfInt, nmStr LIMIT 1000");
    } finally {
      useMainSegment();
    }
  }
}
