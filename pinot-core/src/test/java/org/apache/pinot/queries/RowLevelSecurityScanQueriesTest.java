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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.RowLevelSecurityFilterOperator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// End-to-end test of the row-level security (RLS) row filter on the server.
///
/// The segment holds the rows of two tenants, interleaved. Tenant `A` has a numeric `payload`. Tenant `B` has a
/// `payload` that is not a number, so `CAST(payload AS INT)` throws on its rows and the error message carries the
/// hidden value. The RLS filter is `tenant = 'A'`. As the broker does, each query has the RLS predicate in its WHERE
/// clause and in the `rlsFilters-<table>` query option. A query must not evaluate the user expression on a row of
/// tenant `B`, so it must not fail.
///
/// Each case runs on two loads of the same segment: one with no index on `tenant` and one with an inverted index on
/// `tenant`. Both loads have an inverted index on `bucket`.
public class RowLevelSecurityScanQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RowLevelSecurityScanQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final int NUM_ROWS = 5000;
  // BaseQueriesTest queries two instances of two copies of the segment
  private static final int NUM_SEGMENT_COPIES = 4;
  private static final String RLS_FILTER = "tenant = 'A'";
  private static final String HIDDEN_PREFIX = "secret-";
  private static final String VISIBLE_POISON = "authorized-poison";

  private ImmutableSegment _noIndexSegment;
  private ImmutableSegment _invertedIndexSegment;
  // Segments with only the rows of tenant B and only the rows of tenant A
  private ImmutableSegment _hiddenOnlySegment;
  private ImmutableSegment _tenantOnlySegment;
  private ImmutableSegment _tenantOnlyWithPoisonSegment;
  private ImmutableSegment _mixedWithPoisonSegment;
  // Segment with a star-tree on (tenant, bucket)
  private ImmutableSegment _starTreeSegment;
  private Schema _schema;
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
    FileUtils.deleteDirectory(INDEX_DIR);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("tenant", DataType.STRING)
        .addSingleValueDimension("payload", DataType.STRING)
        .addSingleValueDimension("bucket", DataType.INT)
        .build();
    TableConfig noIndexConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("bucket"))
        .build();
    TableConfig invertedIndexConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("tenant", "bucket"))
        .build();
    TableConfig starTreeConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setStarTreeIndexConfigs(List.of(new StarTreeIndexConfig(List.of("tenant", "bucket"), null,
            List.of("COUNT__*", "SUM__id"), null, 1)))
        .build();

    _noIndexSegment = buildSegment("mixed", noIndexConfig, schema, i -> true);
    _invertedIndexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, "mixed"),
        new IndexLoadingConfig(invertedIndexConfig, schema));
    _hiddenOnlySegment = buildSegment("hiddenOnly", noIndexConfig, schema, i -> !isTenantA(i));
    _tenantOnlySegment = buildSegment("tenantOnly", noIndexConfig, schema, RowLevelSecurityScanQueriesTest::isTenantA);
    _tenantOnlyWithPoisonSegment = buildSegment("tenantOnlyWithPoison", invertedIndexConfig, schema,
        rowsWithVisiblePoison(false));
    _mixedWithPoisonSegment = buildSegment("mixedWithPoison", invertedIndexConfig, schema, rowsWithVisiblePoison(true));
    _starTreeSegment = buildSegment("starTree", starTreeConfig, schema, i -> true);
    _schema = schema;
  }

  private static ImmutableSegment buildSegment(String segmentName, TableConfig tableConfig, Schema schema,
      IntPredicate rowFilter)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (!rowFilter.test(i)) {
        continue;
      }
      rows.add(row(i));
    }
    return buildSegment(segmentName, tableConfig, schema, rows);
  }

  private static ImmutableSegment buildSegment(String segmentName, TableConfig tableConfig, Schema schema,
      List<GenericRow> rows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), new IndexLoadingConfig(tableConfig, schema));
  }

  private static List<GenericRow> rowsWithVisiblePoison(boolean includeHiddenRows) {
    int poisonId = 4 * BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE;
    List<GenericRow> rows = new ArrayList<>();
    for (int id = 0; id <= poisonId; id++) {
      if (includeHiddenRows || isTenantA(id)) {
        GenericRow row = row(id);
        if (id == poisonId) {
          row.putValue("payload", VISIBLE_POISON);
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static GenericRow row(int i) {
    GenericRow row = new GenericRow();
    row.putValue("id", i);
    row.putValue("tenant", isTenantA(i) ? "A" : "B");
    row.putValue("payload", isTenantA(i) ? Integer.toString(payload(i)) : HIDDEN_PREFIX + i);
    row.putValue("bucket", i % 10);
    return row;
  }

  @AfterClass
  public void tearDown() {
    _noIndexSegment.destroy();
    _invertedIndexSegment.destroy();
    _hiddenOnlySegment.destroy();
    _tenantOnlySegment.destroy();
    _tenantOnlyWithPoisonSegment.destroy();
    _mixedWithPoisonSegment.destroy();
    _starTreeSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private static boolean isTenantA(int i) {
    return i % 2 == 0;
  }

  /// The payload of a tenant `A` row, in [1, 100].
  private static int payload(int i) {
    return i % 100 + 1;
  }

  /// Each case is a user filter, the tenant `A` rows it must match, whether the segment has an inverted index on
  /// `tenant`, and the extra query options.
  ///
  /// - A top-level expression. With no index on `tenant`, `AndDocIdIterator` evaluates the expression on a block
  ///   of docs that starts at the target doc and includes the rows of tenant `B`.
  /// - An expression inside OR. The OR evaluates its children on the whole segment.
  /// - An expression inside NOT. The NOT evaluates its child on the whole segment.
  /// - Each case also runs with an inverted index on `tenant`.
  /// - Each case also runs with `andRestrictionPushdownMode=NEVER` and the inverted index on `tenant` skipped.
  /// - Each case also runs with null handling, which derives the NULL documents of an AND or an OR with one more
  ///   AND.
  @DataProvider
  public static Object[][] userFilters() {
    IntPredicate castAbove50 = i -> payload(i) > 50;
    IntPredicate idBelow100 = i -> i < 100;
    Object[][] filters = {
        {"top-level expression", "CAST(payload AS INT) > 50", castAbove50},
        {"expression inside OR", "(CAST(payload AS INT) > 50 OR id < 100)", castAbove50.or(idBelow100)},
        {"expression inside NOT", "NOT (CAST(payload AS INT) > 50)", castAbove50.negate()},
        {"expression inside NOT of OR", "NOT (CAST(payload AS INT) > 50 OR id < 100)",
            castAbove50.or(idBelow100).negate()},
        {"expression inside OR next to an index predicate", "(CAST(payload AS INT) > 50 OR bucket = 2)",
            castAbove50.or(i -> i % 10 == 2)}
    };
    Map<String, Map<String, String>> optionVariants = Map.of(
        "default options", Map.of(),
        "pushdown NEVER and skipIndexes on tenant", Map.of(QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, "never",
            QueryOptionKey.SKIP_INDEXES, "tenant=inverted"),
        "null handling", Map.of(QueryOptionKey.ENABLE_NULL_HANDLING, "true"));
    List<Object[]> cases = new ArrayList<>();
    for (Object[] filter : filters) {
      for (boolean invertedIndex : new boolean[]{false, true}) {
        for (Map.Entry<String, Map<String, String>> options : optionVariants.entrySet()) {
          cases.add(new Object[]{filter[0] + ", " + options.getKey(), filter[1], filter[2], invertedIndex,
              options.getValue()});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "userFilters")
  public void testUserExpressionDoesNotRunOnHiddenRows(String name, String userFilter, IntPredicate expected,
      boolean invertedIndex, Map<String, String> options) {
    useSegment(invertedIndex);
    String caseName = name + (invertedIndex ? ", inverted index on tenant" : ", no index on tenant");
    long expectedCount = 0;
    long expectedSum = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i) && expected.test(i)) {
        expectedCount++;
        expectedSum += i;
      }
    }
    assertTrue(expectedCount > 0, "The filter must match some rows to be meaningful");

    BrokerResponseNative response =
        getRlsBrokerResponse("SELECT COUNT(*), SUM(id) FROM testTable", userFilter, options);

    assertTrue(response.getExceptions().isEmpty(), caseName + ": " + response.getExceptions());
    Object[] row = response.getResultTable().getRows().get(0);
    assertEquals(((Number) row[0]).longValue(), expectedCount * NUM_SEGMENT_COPIES, caseName);
    assertEquals(((Number) row[1]).longValue(), expectedSum * NUM_SEGMENT_COPIES, caseName);
  }

  /// A selection query must not evaluate the expression on hidden rows either, also when it stops at its LIMIT.
  @Test(dataProvider = "userFilters")
  public void testSelectionDoesNotRunOnHiddenRows(String name, String userFilter, IntPredicate expected,
      boolean invertedIndex, Map<String, String> options) {
    useSegment(invertedIndex);
    BrokerResponseNative response = getRlsBrokerResponse("SELECT id FROM testTable", userFilter + " LIMIT 10", options);
    assertTrue(response.getExceptions().isEmpty(), name + ": " + response.getExceptions());
    for (Object[] row : response.getResultTable().getRows()) {
      int id = (Integer) row[0];
      assertTrue(isTenantA(id) && expected.test(id), name + ": unexpected row " + id);
    }
  }

  @Test
  public void testSelectionStreamsRestrictedExpression() {
    String filter = "CAST(payload AS INT) > 50";
    SelectionRun streamed = runSingleSegmentSelection(_invertedIndexSegment, filter + " LIMIT 10");
    SelectionRun eager = runSingleSegmentSelection(_invertedIndexSegment, filter + " ORDER BY id LIMIT 10");

    // The segment's ids increase with docId. A cast on any intervening hidden row would fail the query.
    assertEquals(streamed.ids(), IntStream.range(0, 10).map(i -> 50 + 2 * i).boxed().toList());
    assertEquals(streamed.ids(), eager.ids());
    assertEquals(eager.entriesScanned(), NUM_ROWS / 2L);
    assertTrue(streamed.entriesScanned() >= streamed.ids().size());
    assertTrue(streamed.entriesScanned() < eager.entriesScanned(),
        "The unordered LIMIT should stop reading payload before all authorized rows are evaluated");
  }

  @Test
  public void testSinglePredicateLimitDoesNotDependOnHiddenRows() {
    List<Integer> expectedIds = IntStream.range(0, 10).map(i -> 2 * i).boxed().toList();
    for (ImmutableSegment segment : List.of(_tenantOnlyWithPoisonSegment, _mixedWithPoisonSegment)) {
      SelectionRun result = runSingleSegmentSelection(segment, "CAST(payload AS INT) > 0 LIMIT 10");
      assertEquals(result.ids(), expectedIds, segment.getSegmentName());
    }
  }

  @Test
  public void testCompositePredicateStaysEagerWhenOneBranchIsPruned() {
    // A bucket of 9 is absent from the first segment and exists only on hidden rows in the second.
    String filter = "(CAST(payload AS INT) > 0 OR bucket = 9) LIMIT 10";
    for (ImmutableSegment segment : List.of(_tenantOnlyWithPoisonSegment, _mixedWithPoisonSegment)) {
      NumberFormatException failure =
          expectThrows(NumberFormatException.class, () -> runSingleSegmentSelection(segment, filter));
      assertTrue(failure.getMessage().contains(VISIBLE_POISON), failure.toString());
    }
  }

  @DataProvider
  public static Object[][] streamingScanPredicates() {
    return new Object[][]{
        {"payload = '51'", IntStream.range(0, 10).map(i -> 50 + 100 * i).boxed().toList()},
        {"CAST(payload AS INT) = 99", IntStream.range(0, 10).map(i -> 98 + 100 * i).boxed().toList()},
        // The literal exists in the segment dictionary, so the predicate needs a scan even though RLS hides it.
        {"payload = 'secret-1'", List.of()}
    };
  }

  @Test(dataProvider = "streamingScanPredicates")
  public void testRestrictedSelectionClosesScanReader(String filter, List<Integer> expectedIds) {
    DataSource source = _invertedIndexSegment.getDataSource("payload");
    ForwardIndexReader<?> forwardIndex = source.getForwardIndex();
    assertTrue(forwardIndex.isDictionaryEncoded());
    ForwardIndexReader<?> trackedReader = mock(ForwardIndexReader.class, delegatesTo(forwardIndex));
    ForwardIndexReaderContext context = mock(ForwardIndexReaderContext.class);
    doReturn(context).when(trackedReader).createContext(anyMap());
    AtomicInteger docsRead = new AtomicInteger();
    doAnswer(invocation -> {
      int[] docIds = invocation.getArgument(0);
      int length = invocation.getArgument(1);
      assertSame(invocation.getArgument(3), context);
      for (int i = 0; i < length; i++) {
        assertTrue(isTenantA(docIds[i]), "Read hidden document " + docIds[i]);
      }
      forwardIndex.readDictIds(docIds, length, invocation.getArgument(2), null);
      docsRead.addAndGet(length);
      return null;
    }).when(trackedReader).readDictIds(any(int[].class), anyInt(), any(int[].class), any());
    DataSource trackedSource = mock(DataSource.class, delegatesTo(source));
    doReturn(trackedReader).when(trackedSource).getForwardIndex();
    IndexSegment segment = mock(IndexSegment.class, delegatesTo(_invertedIndexSegment));
    doReturn(trackedSource).when(segment).getDataSource("payload", _schema);

    SelectionRun result = runSingleSegmentSelection(segment, filter + " LIMIT 10");

    assertEquals(result.ids(), expectedIds);
    assertEquals(result.entriesScanned(), docsRead.longValue());
    if (expectedIds.isEmpty()) {
      assertEquals(docsRead.get(), NUM_ROWS / 2);
    } else {
      assertTrue(docsRead.get() < NUM_ROWS / 2, "The LIMIT should stop the scan before it reaches EOF");
      verify(trackedReader, atLeast(2)).readDictIds(any(int[].class), anyInt(), any(int[].class), any());
    }
    verify(trackedReader).createContext(anyMap());
    verify(context).close();
  }

  /// Group-by, distinct and order-by queries use the same filter plan, so they must not evaluate the expression on
  /// hidden rows either.
  @Test(dataProvider = "userFilters")
  public void testGroupByDistinctAndOrderByDoNotRunOnHiddenRows(String name, String userFilter,
      IntPredicate expected, boolean invertedIndex, Map<String, String> options) {
    useSegment(invertedIndex);
    long expectedCount = 0;
    Set<Integer> expectedBuckets = new HashSet<>();
    int expectedMinId = Integer.MAX_VALUE;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i) && expected.test(i)) {
        expectedCount++;
        expectedBuckets.add(i % 10);
        expectedMinId = Math.min(expectedMinId, i);
      }
    }

    BrokerResponseNative groupBy =
        getRlsBrokerResponse("SELECT bucket, COUNT(*) FROM testTable", userFilter + " GROUP BY bucket", options);
    assertTrue(groupBy.getExceptions().isEmpty(), name + ": " + groupBy.getExceptions());
    long count = 0;
    Set<Integer> buckets = new HashSet<>();
    for (Object[] row : groupBy.getResultTable().getRows()) {
      buckets.add((Integer) row[0]);
      count += ((Number) row[1]).longValue();
    }
    assertEquals(buckets, expectedBuckets, name);
    assertEquals(count, expectedCount * NUM_SEGMENT_COPIES, name);

    BrokerResponseNative distinct = getRlsBrokerResponse("SELECT DISTINCT bucket FROM testTable", userFilter, options);
    assertTrue(distinct.getExceptions().isEmpty(), name + ": " + distinct.getExceptions());
    Set<Integer> distinctBuckets = new HashSet<>();
    for (Object[] row : distinct.getResultTable().getRows()) {
      distinctBuckets.add((Integer) row[0]);
    }
    assertEquals(distinctBuckets, expectedBuckets, name);

    BrokerResponseNative orderBy =
        getRlsBrokerResponse("SELECT id FROM testTable", userFilter + " ORDER BY id LIMIT 10", options);
    assertTrue(orderBy.getExceptions().isEmpty(), name + ": " + orderBy.getExceptions());
    assertEquals(orderBy.getResultTable().getRows().get(0)[0], expectedMinId, name);
  }

  /// The RLS filter can be an expression, an OR, or the AND of several policies, which the broker writes as
  /// `( f1 ) AND ( f2 )`. These shapes are not a single column predicate, so the RLS operator materializes them
  /// through the generic filter path. Each shape matches the rows of tenant `A`.
  @DataProvider
  public static Object[][] policyShapes() {
    List<Object[]> cases = new ArrayList<>();
    for (String policy : List.of("lower(tenant) = 'a'", "tenant = 'A' OR id < 0", "( tenant = 'A' ) AND ( id >= 0 )")) {
      for (boolean invertedIndex : new boolean[]{false, true}) {
        cases.add(new Object[]{policy, invertedIndex});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "policyShapes")
  public void testPolicyShapes(String policy, boolean invertedIndex) {
    useSegment(invertedIndex);
    long expectedCount = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i) && payload(i) > 50) {
        expectedCount++;
      }
    }
    BrokerResponseNative response =
        getBrokerResponse("SELECT COUNT(*) FROM testTable WHERE ( " + policy + " ) AND CAST(payload AS INT) > 50",
            rlsOptions(policy, Map.of()));
    assertTrue(response.getExceptions().isEmpty(), policy + ": " + response.getExceptions());
    assertEquals(((Number) response.getResultTable().getRows().get(0)[0]).longValue(),
        expectedCount * NUM_SEGMENT_COPIES, policy);
  }

  /// A full-policy override must not match only the independent policy after the broker flattened its WHERE copy.
  @Test
  public void testWholePolicyOverrideCannotRemoveRowFilterBarrier() {
    useSegment(false);
    String policy = "tenant = 'A' AND payload = '1'";
    Map<Expression, Expression> overrides = Map.of(
        OPTIMIZER.optimizeFilter(CalciteSqlParser.compileToExpression(policy), _schema),
        CalciteSqlParser.compileToExpression("TRUE"));
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT COUNT(*) FROM testTable WHERE CAST(payload AS INT) > 0 AND (" + policy + ")");
    OPTIMIZER.optimize(pinotQuery, _schema);
    Expression whereBeforeOverrides = pinotQuery.getFilterExpression().deepCopy();
    pinotQuery.setFilterExpression(QueryOptimizer.applyExpressionOverride(pinotQuery.getFilterExpression(), overrides));
    assertEquals(pinotQuery.getFilterExpression(), whereBeforeOverrides,
        "The full-policy subtree must be absent from the flattened broker WHERE");
    pinotQuery.setQueryOptions(rlsOptions(policy, Map.of()));
    PlanMaker planMaker = new InstancePlanMakerImplV2() {
      @Override
      public Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
          ExecutorService executorService) {
        queryContext.setSchema(_schema);
        queryContext.setRowFilterExpressionOverrideMap(overrides);
        return super.makeInstancePlan(segmentContexts, queryContext, executorService);
      }
    };

    BrokerResponseNative response = getBrokerResponse(pinotQuery, planMaker);

    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(((Number) response.getResultTable().getRows().get(0)[0]).longValue(),
        (long) (NUM_ROWS / 100) * NUM_SEGMENT_COPIES);
  }

  /// The plan shows the RLS filter operator above the user filter.
  @Test
  public void testExplainPlan() {
    useSegment(false);
    BrokerResponseNative response =
        getRlsBrokerResponse("EXPLAIN PLAN FOR SELECT COUNT(*) FROM testTable", "CAST(payload AS INT) > 50", Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    List<String> operators = new ArrayList<>();
    for (Object[] row : response.getResultTable().getRows()) {
      operators.add(row[0].toString());
    }
    assertTrue(operators.contains("FILTER_ROW_LEVEL_SECURITY"), operators.toString());
  }

  /// A filtered aggregation with an expression inside OR in its sub-filter.
  @Test(dataProvider = "segmentVariants")
  public void testFilteredAggregation(boolean invertedIndex) {
    useSegment(invertedIndex);
    long expectedFiltered = 0;
    long expectedAll = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i)) {
        expectedAll++;
        if (payload(i) > 50 || i < 100) {
          expectedFiltered++;
        }
      }
    }
    BrokerResponseNative response = getRlsBrokerResponse(
        "SELECT COUNT(*) FILTER (WHERE CAST(payload AS INT) > 50 OR id < 100), COUNT(*) FROM testTable", null,
        Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    Object[] row = response.getResultTable().getRows().get(0);
    assertEquals(((Number) row[0]).longValue(), expectedFiltered * NUM_SEGMENT_COPIES);
    assertEquals(((Number) row[1]).longValue(), expectedAll * NUM_SEGMENT_COPIES);
  }

  /// A filtered aggregation on a segment that holds only tenant rows: the main filter matches all documents, so
  /// the sub-filter runs alone on the segment.
  @Test
  public void testFilteredAggregationWhenMainFilterMatchesAll() {
    useSegment(_tenantOnlySegment);
    BrokerResponseNative response = getRlsBrokerResponse(
        "SELECT COUNT(*) FILTER (WHERE CAST(payload AS INT) > 50 OR id < 100), COUNT(*) FROM testTable", null,
        Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    long expectedAll = NUM_ROWS / 2;
    assertEquals(((Number) response.getResultTable().getRows().get(0)[1]).longValue(),
        expectedAll * NUM_SEGMENT_COPIES);
  }

  /// `COUNT(*)` with an inverted index on the user predicate counts only the tenant rows.
  @Test(dataProvider = "segmentVariants")
  public void testCountWithIndexedUserPredicate(boolean invertedIndex) {
    useSegment(invertedIndex);
    long expectedCount = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i) && i % 10 == 2) {
        expectedCount++;
      }
    }
    BrokerResponseNative response = getRlsBrokerResponse("SELECT COUNT(*) FROM testTable", "bucket = 2", Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(((Number) response.getResultTable().getRows().get(0)[0]).longValue(),
        expectedCount * NUM_SEGMENT_COPIES);
  }

  /// A segment with no tenant rows plans an empty filter, and a segment with only tenant rows does not build the
  /// RLS bitmap.
  @Test
  public void testPlanTimeShortCuts() {
    QueryContext queryContext = getRlsQueryContext("SELECT COUNT(*) FROM testTable", "CAST(payload AS INT) > 50");

    BaseFilterOperator hiddenOnly =
        new FilterPlanNode(new SegmentContext(_hiddenOnlySegment), queryContext).run();
    assertTrue(hiddenOnly instanceof EmptyFilterOperator, hiddenOnly.toString());

    BaseFilterOperator tenantOnly =
        new FilterPlanNode(new SegmentContext(_tenantOnlySegment), queryContext).run();
    assertFalse(tenantOnly instanceof RowLevelSecurityFilterOperator, tenantOnly.toString());

    BaseFilterOperator mixed = new FilterPlanNode(new SegmentContext(_noIndexSegment), queryContext).run();
    assertTrue(mixed instanceof RowLevelSecurityFilterOperator, mixed.toString());

    useSegment(_hiddenOnlySegment);
    BrokerResponseNative response =
        getRlsBrokerResponse("SELECT COUNT(*) FROM testTable", "CAST(payload AS INT) > 50", Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(((Number) response.getResultTable().getRows().get(0)[0]).longValue(), 0L);
  }

  /// The server fails the query when it cannot parse the RLS filter.
  @Test
  public void testInvalidRowFilterFailsTheQuery() {
    useSegment(false);
    assertThrows(BadQueryRequestException.class, () -> getBrokerResponse("SELECT COUNT(*) FROM testTable",
        Map.of(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME), "tenant = = 'A'")));
  }

  /// `numEntriesScannedInFilter` does not include the cost of the RLS bitmap. Without an index on `tenant`, the
  /// bitmap scans every document of the segment. The WHERE clause then evaluates the RLS predicate again, on the
  /// tenant rows only.
  @Test
  public void testEntriesScannedInFilterExcludeTheRowFilter() {
    useSegment(false);
    BrokerResponseNative response = getRlsBrokerResponse("SELECT COUNT(*) FROM testTable", null, Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    long numTenantRows = NUM_ROWS / 2;
    assertEquals(response.getNumEntriesScannedInFilter(), numTenantRows * NUM_SEGMENT_COPIES);
  }

  /// A star-tree with the RLS column as a dimension. The star-tree filter holds the RLS predicate, because the
  /// predicate stays in the WHERE clause, so the result has only the tenant rows.
  @Test
  public void testStarTree() {
    useSegment(_starTreeSegment);
    long expectedCount = 0;
    long expectedSum = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isTenantA(i) && i % 10 == 2) {
        expectedCount++;
        expectedSum += i;
      }
    }
    BrokerResponseNative response =
        getRlsBrokerResponse("SELECT COUNT(*), SUM(id) FROM testTable", "bucket = 2", Map.of());
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    Object[] row = response.getResultTable().getRows().get(0);
    assertEquals(((Number) row[0]).longValue(), expectedCount * NUM_SEGMENT_COPIES);
    assertEquals(((Number) row[1]).longValue(), expectedSum * NUM_SEGMENT_COPIES);
    // The star-tree answers from its pre-aggregated records, one per segment copy
    assertEquals(response.getNumDocsScanned(), NUM_SEGMENT_COPIES);

    // A group-by on the RLS column returns only the tenant group
    BrokerResponseNative groupBy =
        getRlsBrokerResponse("SELECT tenant, COUNT(*) FROM testTable", "bucket < 5 GROUP BY tenant", Map.of());
    assertTrue(groupBy.getExceptions().isEmpty(), groupBy.getExceptions().toString());
    List<Object[]> groups = groupBy.getResultTable().getRows();
    assertEquals(groups.size(), 1);
    assertEquals(groups.get(0)[0], "A");
    assertTrue(groupBy.getNumDocsScanned() < NUM_ROWS / 2, "The star-tree must answer the group-by");
  }

  /// In a consuming segment, the inverted index on the RLS column grows after the plan read the number of
  /// documents. The RLS bitmap and the user filter must not reach the rows added later, which include hidden rows.
  @Test
  public void testConsumingSegment()
      throws Exception {
    MutableSegmentImpl consuming =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, Set.of(), Set.of(), Set.of("tenant"), false);
    try {
      int numPlannedDocs = 1000;
      for (int i = 0; i < numPlannedDocs; i++) {
        consuming.index(row(i), null);
      }
      QueryContext queryContext = getRlsQueryContext("SELECT COUNT(*) FROM testTable", "CAST(payload AS INT) > 50");
      queryContext.setAndRestrictionPushdownEnabled(true);
      BaseFilterOperator filterOperator = new FilterPlanNode(new SegmentContext(consuming), queryContext).run();
      assertTrue(filterOperator instanceof RowLevelSecurityFilterOperator, filterOperator.toString());

      // Rows arrive after planning. The tenant A rows have a hidden payload, so the user filter throws on them too.
      for (int i = numPlannedDocs; i < 2 * numPlannedDocs; i++) {
        GenericRow row = row(i);
        row.putValue("payload", HIDDEN_PREFIX + i);
        consuming.index(row, null);
      }

      MutableRoaringBitmap docIds =
          BlockDocIdSet.collect(filterOperator.nextBlock().getBlockDocIdSet().iterator());
      MutableRoaringBitmap expected = new MutableRoaringBitmap();
      for (int i = 0; i < numPlannedDocs; i++) {
        if (isTenantA(i) && payload(i) > 50) {
          expected.add(i);
        }
      }
      assertEquals(docIds, expected);
    } finally {
      consuming.destroy();
    }
  }

  /// Under upsert, the result respects both the RLS bitmap and the valid-document snapshot, and the user filter
  /// does not run on hidden rows.
  @Test(dataProvider = "segmentVariants")
  public void testValidDocIdsSnapshot(boolean invertedIndex) {
    ImmutableSegment segment = invertedIndex ? _invertedIndexSegment : _noIndexSegment;
    MutableRoaringBitmap validDocIds = new MutableRoaringBitmap();
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      // Every third document is an older version of its primary key
      if (i % 3 != 0) {
        validDocIds.add(i);
        if (isTenantA(i) && payload(i) > 50) {
          expected.add(i);
        }
      }
    }
    QueryContext queryContext = getRlsQueryContext("SELECT COUNT(*) FROM testTable", "CAST(payload AS INT) > 50");
    queryContext.setAndRestrictionPushdownEnabled(true);
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(validDocIds);

    BaseFilterOperator filterOperator = new FilterPlanNode(segmentContext, queryContext).run();
    assertTrue(filterOperator instanceof RowLevelSecurityFilterOperator, filterOperator.toString());
    assertEquals(BlockDocIdSet.collect(filterOperator.nextBlock().getBlockDocIdSet().iterator()), expected);
  }

  /// A policy with a decimal literal on an INT column. The broker and the MSE leaf optimize the WHERE copy of the
  /// policy (`bucket = 2.0` becomes `bucket = 2`), so the server optimizes the row filter too. The plan maker sets the
  /// schema on the query context, as `ServerQueryExecutorV1Impl` does before it makes the plan.
  @Test(dataProvider = "segmentVariants")
  public void testNumericLiteralPolicyOnIntColumn(boolean invertedIndex) {
    useSegment(invertedIndex);
    String policy = "bucket = 2.0";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT COUNT(*) FROM testTable WHERE " + policy + " AND CAST(payload AS INT) > 0 OR id < 0");
    // Shape the WHERE clause as RlsFiltersRewriter does: the policy ANDed with the user filter
    pinotQuery.setFilterExpression(CalciteSqlParser.compileToExpression(
        policy + " AND (" + (isTenantA(2) ? "id >= 0" : "id < 0") + ")"));
    OPTIMIZER.optimize(pinotQuery, _schema);
    pinotQuery.setQueryOptions(new HashMap<>(Map.of(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME), policy)));

    PlanMaker planMaker = new InstancePlanMakerImplV2() {
      @Override
      public Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
          ExecutorService executorService) {
        queryContext.setSchema(_schema);
        return super.makeInstancePlan(segmentContexts, queryContext, executorService);
      }
    };

    BrokerResponseNative response = getBrokerResponse(pinotQuery, planMaker);

    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(((Number) response.getResultTable().getRows().get(0)[0]).longValue(),
        (long) (NUM_ROWS / 10) * NUM_SEGMENT_COPIES);
  }

  @DataProvider
  public static Object[][] segmentVariants() {
    return new Object[][]{{false}, {true}};
  }

  private void useSegment(boolean invertedIndex) {
    useSegment(invertedIndex ? _invertedIndexSegment : _noIndexSegment);
  }

  private void useSegment(IndexSegment segment) {
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  /// Runs the query as the broker sends it under RLS: the RLS predicate ANDed with the user filter in the WHERE clause,
  /// and the RLS filter in the `rlsFilters-<table>` query option.
  private BrokerResponseNative getRlsBrokerResponse(String query, @Nullable String userFilter,
      Map<String, String> options) {
    return getBrokerResponse(withRowFilter(query, userFilter), rlsOptions(options));
  }

  private static QueryContext getRlsQueryContext(String query, @Nullable String userFilter) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(withRowFilter(query, userFilter));
    pinotQuery.setQueryOptions(rlsOptions(Map.of()));
    return QueryContextConverterUtils.getQueryContext(pinotQuery);
  }

  private SelectionRun runSingleSegmentSelection(IndexSegment segment, String filter) {
    QueryContext queryContext = getRlsQueryContext("SELECT id FROM testTable", filter);
    queryContext.setSchema(_schema);
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      Operator<SelectionResultsBlock> selection =
          new SelectionPlanNode(new SegmentContext(segment), queryContext).run();
      List<Integer> ids = selection.nextBlock().getRows().stream().map(row -> (Integer) row[0]).toList();
      return new SelectionRun(ids, selection.getExecutionStatistics().getNumEntriesScannedInFilter());
    }
  }

  private record SelectionRun(List<Integer> ids, long entriesScanned) {
  }

  private static String withRowFilter(String query, @Nullable String userFilter) {
    return query + " WHERE " + RLS_FILTER + (userFilter != null ? " AND " + userFilter : "");
  }

  private static Map<String, String> rlsOptions(Map<String, String> options) {
    return rlsOptions(RLS_FILTER, options);
  }

  private static Map<String, String> rlsOptions(String policy, Map<String, String> options) {
    Map<String, String> queryOptions = new HashMap<>(options);
    queryOptions.put(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME), policy);
    return queryOptions;
  }
}
