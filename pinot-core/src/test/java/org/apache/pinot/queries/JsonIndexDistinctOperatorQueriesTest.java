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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Queries tests for [org.apache.pinot.core.operator.query.JsonIndexDistinctOperator] against the JSON index.
///
/// Two segments cover the operator's behavior:
/// - **Full segment**: 500 docs, every doc has both `$.k1` and `$.k2`; `filterCol` is nullable in the last 100 docs.
///   Used for path parity, base-column-filter null handling, same/cross-path `JSON_MATCH` filters, the 5-arg
///   `jsonFilterExpression`, the `jsonIndexDistinctSkipMissingPath` query option, execution-statistics shape, and
///   the construction-time validation throw for invalid 4-arg defaults.
/// - **Sparse segment**: 20 docs, only the first 10 have `$.k1`. Used for the 4-arg-default, the same-path `IS NULL`
///   filter that triggers the default, the 3-arg "Illegal Json Path" throw, and the skip-option's suppression of
///   that throw.
///
/// Composite-only behavior (selective `invertedIndexConfigs`, `jsonExtractScalar` fallback on non-indexed paths)
/// is covered separately in `ai.startree.integration.tests.JsonIndexDistinctOperatorCompositeSharedClusterTest`.
public class JsonIndexDistinctOperatorQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "JsonIndexDistinctOperatorQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String JSON_COL = "jsonCol";
  private static final String FILTER_COL = "filterCol";

  private static final int FULL_NUM_DOCS = 500;
  private static final int FULL_NUM_DISTINCT_K1 = 50;
  private static final int FULL_NUM_NON_NULL_FILTER = 400;

  private static final int SPARSE_NUM_DOCS = 20;
  private static final int SPARSE_NUM_WITH_K1 = 10;

  private static final String OPT_USE_INDEX = "SET useIndexBasedDistinctOperator=true; ";
  private static final String OPT_NULLS = "SET enableNullHandling=true; ";
  private static final String OPT_USE_INDEX_NULLS = OPT_USE_INDEX + OPT_NULLS;
  private static final String OPT_USE_INDEX_SKIP_MISSING_PATH =
      OPT_USE_INDEX + "SET jsonIndexDistinctSkipMissingPath=true; ";

  private IndexSegment _fullSegment;
  private IndexSegment _sparseSegment;
  private IndexSegment _activeSegment;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _activeSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_activeSegment, _activeSegment);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _fullSegment = buildFullSegment();
    _sparseSegment = buildSparseSegment();
  }

  @AfterClass
  public void tearDown() {
    if (_fullSegment != null) {
      _fullSegment.destroy();
    }
    if (_sparseSegment != null) {
      _sparseSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private IndexSegment buildFullSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(JSON_COL, DataType.STRING)
        .addSingleValueDimension(FILTER_COL, DataType.INT)
        .build();

    List<GenericRow> records = new ArrayList<>(FULL_NUM_DOCS);
    for (int i = 0; i < FULL_NUM_DOCS; i++) {
      Map<String, String> json = Map.of(
          "k1", "value-k1-" + (i % FULL_NUM_DISTINCT_K1),
          "k2", "value-k2-" + i
      );
      GenericRow record = new GenericRow();
      record.putValue(JSON_COL, JsonUtils.objectToString(json));
      record.putValue(FILTER_COL, i < FULL_NUM_NON_NULL_FILTER ? i : null);
      records.add(record);
    }

    TableConfig tableConfig = createTableConfig(true);
    return buildSegment("fullSegment", schema, tableConfig, records, true);
  }

  private IndexSegment buildSparseSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(JSON_COL, DataType.STRING)
        .build();

    List<GenericRow> records = new ArrayList<>(SPARSE_NUM_DOCS);
    for (int i = 0; i < SPARSE_NUM_DOCS; i++) {
      Map<String, String> json = new HashMap<>();
      if (i < SPARSE_NUM_WITH_K1) {
        json.put("k1", "k1-" + i);
      }
      json.put("k2", "k2-" + i);
      GenericRow record = new GenericRow();
      record.putValue(JSON_COL, JsonUtils.objectToString(json));
      records.add(record);
    }

    TableConfig tableConfig = createTableConfig(false);
    return buildSegment("sparseSegment", schema, tableConfig, records, false);
  }

  private TableConfig createTableConfig(boolean withFilterCol) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("json", new JsonIndexConfig().toJsonNode());
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .addFieldConfig(new FieldConfig.Builder(JSON_COL)
            .withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexes(indexes)
            .build());
    if (withFilterCol) {
      builder.setNullHandlingEnabled(true);
    }
    return builder.build();
  }

  private IndexSegment buildSegment(String segmentName, Schema schema, TableConfig tableConfig,
      List<GenericRow> records, boolean defaultNullHandling)
      throws Exception {
    File segmentDir = new File(INDEX_DIR, segmentName + "_dir");
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(segmentDir.getAbsolutePath());
    config.setDefaultNullHandlingEnabled(defaultNullHandling);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(segmentDir, segmentName), new IndexLoadingConfig(tableConfig, schema));
  }

  /// With the OSS JSON index `$.k1` is indexed, so `useIndexBasedDistinctOperator=true` must route through
  /// [JsonIndexDistinctOperator] and produce the same set of distinct values as the scan-based baseline.
  @Test
  public void testIndexedPathParity() {
    _activeSegment = _fullSegment;
    String selectExpr = "jsonExtractIndex(jsonCol, '$.k1', 'STRING')";
    String baselineQuery = "SELECT DISTINCT " + selectExpr + " FROM testTable ORDER BY " + selectExpr + " LIMIT 10000";
    String optimizedQuery = OPT_USE_INDEX + baselineQuery;

    BaseOperator<DistinctResultsBlock> optimizedOp = getOperator(optimizedQuery);
    assertTrue(optimizedOp.toExplainString().contains("DISTINCT_JSON_INDEX"));

    BrokerResponseNative baseline = getBrokerResponse(baselineQuery);
    BrokerResponseNative optimized = getBrokerResponse(optimizedQuery);
    assertEquals(extractStringValues(optimized.getResultTable()), extractStringValues(baseline.getResultTable()));
    assertEquals(optimized.getResultTable().getRows().size(), FULL_NUM_DISTINCT_K1);
  }

  /// A nullable filter column drives the doc-id set into [JsonIndexDistinctOperator]. With null handling disabled,
  /// `filterCol < N` still excludes nulls naturally; with `enableNullHandling=true`, `filterCol IS NOT NULL` must
  /// produce the same distinct values as the scan-based baseline. Regression coverage for the prior bug where the
  /// operator did not honor null exclusion delivered through a base-column filter.
  @Test
  public void testNullHandlingOnSeparateFilterColumn() {
    _activeSegment = _fullSegment;
    String selectExpr = "jsonExtractIndex(jsonCol, '$.k1', 'STRING')";

    String rangeBaselineQuery =
        "SELECT DISTINCT " + selectExpr + " FROM testTable WHERE filterCol < " + FULL_NUM_NON_NULL_FILTER + " "
            + "ORDER BY " + selectExpr + " LIMIT 10000";
    String rangeOptimizedQuery = OPT_USE_INDEX + rangeBaselineQuery;
    BaseOperator<DistinctResultsBlock> rangeOptimizedOp = getOperator(rangeOptimizedQuery);
    assertTrue(rangeOptimizedOp.toExplainString().contains("DISTINCT_JSON_INDEX"));
    BrokerResponseNative rangeBaseline = getBrokerResponse(rangeBaselineQuery);
    BrokerResponseNative rangeOptimized = getBrokerResponse(rangeOptimizedQuery);
    assertEquals(extractStringValues(rangeOptimized.getResultTable()),
        extractStringValues(rangeBaseline.getResultTable()));
    assertFalse(containsNull(rangeOptimized.getResultTable()));

    String isNotNullBody =
        "SELECT DISTINCT " + selectExpr + " FROM testTable WHERE filterCol IS NOT NULL "
            + "ORDER BY " + selectExpr + " LIMIT 10000";
    String isNotNullBaselineQuery = OPT_NULLS + isNotNullBody;
    String isNotNullOptimizedQuery = OPT_USE_INDEX_NULLS + isNotNullBody;
    BaseOperator<DistinctResultsBlock> isNotNullOptimizedOp = getOperator(isNotNullOptimizedQuery);
    assertTrue(isNotNullOptimizedOp.toExplainString().contains("DISTINCT_JSON_INDEX"));
    BrokerResponseNative isNotNullBaseline = getBrokerResponse(isNotNullBaselineQuery);
    BrokerResponseNative isNotNullOptimized = getBrokerResponse(isNotNullOptimizedQuery);
    assertEquals(extractStringValues(isNotNullOptimized.getResultTable()),
        extractStringValues(isNotNullBaseline.getResultTable()));
    assertFalse(containsNull(isNotNullOptimized.getResultTable()));
  }

  /// Same-path `JSON_MATCH` on the indexed column means the filter resolves entirely inside the JSON index, so the
  /// distinct set must contain only the values that satisfy the predicate. With `value-k1-0` selected, the result
  /// is a single distinct value.
  @Test
  public void testSamePathJsonMatchFilter() {
    _activeSegment = _fullSegment;
    String query = OPT_USE_INDEX + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable "
        + "WHERE JSON_MATCH(jsonCol, '\"$.k1\" = ''value-k1-0''') LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), Set.of("value-k1-0"));
  }

  /// Cross-path `JSON_MATCH` (filter on `$.k2`, select distinct of `$.k1`) intersects the per-value doc ids from the
  /// `$.k1` JSON-index lookup with the doc set produced by the `WHERE`-clause filter on `$.k2`, returning only the
  /// `$.k1` values for docs whose `$.k2` matches.
  @Test
  public void testCrossPathJsonMatchFilter() {
    _activeSegment = _fullSegment;
    // `$.k2` = `value-k2-7` matches a single doc; that doc's `$.k1` is `value-k1-(7 % 50)` = `value-k1-7`.
    String query = OPT_USE_INDEX + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable "
        + "WHERE JSON_MATCH(jsonCol, '\"$.k2\" = ''value-k2-7''') LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), Set.of("value-k1-7"));
  }

  /// A 4-arg `jsonExtractIndex` whose default literal cannot be parsed into the requested type still routes through
  /// the JSON-index operator (planner-time `canUseJsonIndexDistinct` only checks the function name); the operator's
  /// constructor surfaces the validation failure as `IllegalArgumentException`.
  @Test
  public void testInvalidDefaultArgThrowsAtConstruction() {
    _activeSegment = _fullSegment;
    String query = OPT_USE_INDEX
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'INT', 'abc') FROM testTable LIMIT 100";
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getOperator(query));
    assertTrue(exception.getMessage().contains("Default value"));
  }

  /// With a 4-arg `jsonExtractIndex(..., defaultValue)` and docs that don't have the path, the default value must be
  /// added once to the distinct set. The sparse segment has 10 docs with `$.k1` and 10 without, so the result is the
  /// 10 distinct `$.k1` values plus the literal `missing`.
  @Test
  public void testFourArgDefaultForDocsWithoutPath() {
    _activeSegment = _sparseSegment;
    String query = OPT_USE_INDEX
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING', 'missing') FROM testTable LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));

    Set<String> expected = new HashSet<>();
    for (int i = 0; i < SPARSE_NUM_WITH_K1; i++) {
      expected.add("k1-" + i);
    }
    expected.add("missing");
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), expected);
  }

  /// `"$.k1" IS NULL` selects only docs missing the path. None of the values returned by the JSON-index lookup
  /// intersect the filtered doc set, so `handleMissingDocs` adds the 4-arg default for the unmatched docs. Result
  /// is the default alone.
  @Test
  public void testSamePathIsNullFilterWithDefault() {
    _activeSegment = _sparseSegment;
    String query = OPT_USE_INDEX
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING', 'missing') FROM testTable "
        + "WHERE JSON_MATCH(jsonCol, '\"$.k1\" IS NULL') LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), Set.of("missing"));
  }

  /// 3-arg `jsonExtractIndex` (no default) over a segment where some docs miss the path must throw
  /// `Illegal Json Path` once `handleMissingDocs` is reached.
  @Test
  public void testMissingPathThrowsWithoutDefault() {
    _activeSegment = _sparseSegment;
    String query = OPT_USE_INDEX
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    RuntimeException exception = expectThrows(RuntimeException.class, op::nextBlock);
    assertTrue(exception.getMessage().contains("Illegal Json Path"));
  }

  /// `numDocsScanned` reports the count of matching docs (either the filter bitmap's cardinality, or `_totalDocs` when
  /// the filter is MatchAll). `numEntriesScannedPostFilter` is the count of distinct JSON-index values examined.
  /// `numEntriesScannedInFilter` is reported by the underlying filter operator (0 for MatchAll, positive when the
  /// filter materializes a bitmap).
  @Test
  public void testExecutionStatistics() {
    _activeSegment = _fullSegment;

    // Unfiltered: filter is MatchAll → numDocsScanned == _totalDocs, numEntriesScannedInFilter == 0, every distinct
    // k1 value is examined post-filter.
    String unfilteredQuery =
        OPT_USE_INDEX + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable LIMIT 10000";
    BaseOperator<DistinctResultsBlock> unfilteredOp = getOperator(unfilteredQuery);
    unfilteredOp.nextBlock();
    ExecutionStatistics unfilteredStats = unfilteredOp.getExecutionStatistics();
    assertEquals(unfilteredStats.getNumDocsScanned(), FULL_NUM_DOCS);
    assertEquals(unfilteredStats.getNumEntriesScannedInFilter(), 0);
    assertEquals(unfilteredStats.getNumEntriesScannedPostFilter(), FULL_NUM_DISTINCT_K1);
    assertEquals(unfilteredStats.getNumTotalDocs(), FULL_NUM_DOCS);

    // Base-column filter: numDocsScanned == cardinality of the filter bitmap. numEntriesScannedInFilter > 0 since the
    // scan-based filter materializes its bitmap by visiting docs. `filterCol >= 0` excludes the Integer.MIN_VALUE
    // null-placeholder docs (since null handling is not enabled here), leaving exactly the FULL_NUM_NON_NULL_FILTER
    // non-null docs.
    String filteredQuery = OPT_USE_INDEX + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable "
        + "WHERE filterCol >= 0 LIMIT 10000";
    BaseOperator<DistinctResultsBlock> filteredOp = getOperator(filteredQuery);
    filteredOp.nextBlock();
    ExecutionStatistics filteredStats = filteredOp.getExecutionStatistics();
    assertEquals(filteredStats.getNumDocsScanned(), FULL_NUM_NON_NULL_FILTER);
    assertEquals(filteredStats.getNumEntriesScannedInFilter(), FULL_NUM_DOCS);
    assertEquals(filteredStats.getNumEntriesScannedPostFilter(), FULL_NUM_DISTINCT_K1);
    assertEquals(filteredStats.getNumTotalDocs(), FULL_NUM_DOCS);
  }

  /// 5-arg form pushes the `jsonFilterExpression` literal directly into `getMatchingFlattenedDocsMap`, so the JSON
  /// index returns only entries whose values satisfy the filter. Any doc whose value does not satisfy the filter is
  /// seen by the distinct operator as missing-path; with a 4-arg default present, the default is added to the
  /// distinct set for those docs. Matches `JsonExtractIndexTransformFunction`'s per-doc behavior, where
  /// `getValuesSV` returns null for docs outside the filtered map and the loop substitutes the default.
  @Test
  public void testFiveArgFilterJsonExpression() {
    _activeSegment = _fullSegment;
    // 5-arg filter narrows the index to value-k1-3 (10 docs). The remaining 490 docs see their $.k1 as missing under
    // this filter and pick up the 4-arg default 'missing'. Result is the union of both.
    String query = OPT_USE_INDEX + "SELECT DISTINCT "
        + "jsonExtractIndex(jsonCol, '$.k1', 'STRING', 'missing', '\"$.k1\" = ''value-k1-3''') "
        + "FROM testTable LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()),
        Set.of("value-k1-3", "missing"));
  }

  /// `jsonIndexDistinctSkipMissingPath=true` disables `handleMissingDocs` entirely. Even when docs are
  /// "missing" from the index (here, all docs outside the 5-arg filter), the 4-arg default is NOT added, no null is
  /// added under nullHandling, and the 3-arg "Illegal Json Path" throw is suppressed. The distinct set is exactly
  /// the values the JSON index returned.
  @Test
  public void testSkipMissingPath() {
    _activeSegment = _fullSegment;
    String query = OPT_USE_INDEX_SKIP_MISSING_PATH
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING', 'missing', '\"$.k1\" = ''value-k1-3''') "
        + "FROM testTable LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    // No 'missing' even though the 4-arg default is set and 490 docs are "missing" under the 5-arg filter.
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), Set.of("value-k1-3"));
  }

  /// With the skip option, a 3-arg call over a sparse segment (docs without `$.k1`) no longer throws — it just
  /// returns the values it did find in the index.
  @Test
  public void testSkipMissingPathSuppressesThrow() {
    _activeSegment = _sparseSegment;
    String query = OPT_USE_INDEX_SKIP_MISSING_PATH
        + "SELECT DISTINCT jsonExtractIndex(jsonCol, '$.k1', 'STRING') FROM testTable LIMIT 100";
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    assertTrue(op.toExplainString().contains("DISTINCT_JSON_INDEX"));
    Set<String> expected = new HashSet<>();
    for (int i = 0; i < SPARSE_NUM_WITH_K1; i++) {
      expected.add("k1-" + i);
    }
    assertEquals(extractStringValues(getBrokerResponse(query).getResultTable()), expected);
  }

  private static Set<String> extractStringValues(ResultTable resultTable) {
    Set<String> values = new HashSet<>();
    for (Object[] row : resultTable.getRows()) {
      values.add(row[0] == null ? null : (String) row[0]);
    }
    return values;
  }

  private static boolean containsNull(ResultTable resultTable) {
    for (Object[] row : resultTable.getRows()) {
      if (row[0] == null) {
        return true;
      }
    }
    return false;
  }
}
