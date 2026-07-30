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
package org.apache.pinot.core.operator.combine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Combine-level tests for {@link StreamingSelectionOrderByCombineOperator} (step-3 operator) and its wiring into
 * {@link CombinePlanNode#getCombineOperator()} (step-4).
 *
 * <p>The streaming combine must return the same globally-sorted top-K rows as the default
 * {@link MinMaxValueBasedSelectionOrderByCombineOperator}, only (in streaming mode) spread across several bounded
 * blocks. Each functional test therefore asserts <b>streaming-vs-non-streaming parity</b>: it runs the identical query
 * twice over the same in-memory segments - once with {@code sortedSelectionMergeEnabled=true} (asserting the new
 * operator was actually selected) and once with the hint off (asserting the {@code MinMax} operator was selected) -
 * then checks the two row sets are equal as a multiset and that the streaming output is fully sorted by the order-by
 * comparator.
 *
 * <p>To keep the top-K boundary unambiguous (operators may legitimately disagree on which of several rows that tie on
 * every order-by key fall inside the limit) every parity query ends its ORDER BY with the globally-unique
 * {@code valCol}
 * so the comparator is a total order; the merge still genuinely interleaves segments because the primary sort column
 * ({@code sortedCol}) overlaps across segments. Multiset (rather than positional) comparison then tolerates only the
 * harmless reordering of fully-equal projected rows.
 */
public class StreamingSelectionOrderByCombineOperatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "StreamingSelectionOrderByCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";

  private static final String SORTED_COL = "sortedCol";
  private static final String TAIL_COL = "tailCol";
  private static final String VAL_COL = "valCol";
  private static final String NULLABLE_COL = "nullableCol";
  // Non-INT projected columns so the parity assertion can catch a stored-type / boxing regression (e.g. LONG emitted
  // where MinMax emits INT), which an all-INT suite cannot observe.
  private static final String LONG_COL = "longCol";
  private static final String STR_COL = "strCol";

  // Create (MAX_NUM_THREADS_PER_QUERY * 2) sorted segments so the leaf runs plan nodes across multiple threads.
  private static final int NUM_SEGMENTS = QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 2;
  private static final int NUM_RECORDS_PER_SEGMENT = 100;

  private static final TableConfig SORTED_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSortedColumn(SORTED_COL).build();
  private static final TableConfig UNSORTED_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(SORTED_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(TAIL_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(VAL_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(NULLABLE_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
      .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING)
      .build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  // Sorted segments with overlapping primary-column ranges, so the k-way merge interleaves them (genuine merge rather
  // than concatenation). Built with null handling on so the null-handling test sees real nulls in NULLABLE_COL; reads
  // with null handling off fall back to the column default.
  private List<IndexSegment> _sortedSegments;
  // Sorted segments with disjoint, globally-increasing ranges: ORDER BY sortedCol with a small LIMIT drains only the
  // lowest segment, so min/max pruning must skip the rest (none acquired/scanned).
  private List<IndexSegment> _disjointSegments;
  // A mix of sorted (streaming child) and physically-unsorted (single materialized top-K block child) segments,
  // exercising both SegmentCursor backings in one merge.
  private List<IndexSegment> _mixedSegments;
  // Very low cardinality primary column (4 distinct values across 100 rows) so each value is a long run: exercises the
  // run/heap path in the streaming children and ties on the primary key at the prune boundary.
  private List<IndexSegment> _lowCardSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    _sortedSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _sortedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "sorted_" + i, buildOverlappingSortedRecords(i), true));
    }

    _disjointSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _disjointSegments.add(buildSegment(SORTED_TABLE_CONFIG, "disjoint_" + i, buildDisjointSortedRecords(i), false));
    }

    _lowCardSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _lowCardSegments.add(buildSegment(SORTED_TABLE_CONFIG, "lowCard_" + i, buildLowCardinalityRecords(i), false));
    }

    // Two sorted + two unsorted segments, globally-unique valCol across all four so the multiset comparison is exact.
    _mixedSegments = new ArrayList<>(4);
    _mixedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "mixedSorted_0", buildOverlappingSortedRecords(0), false));
    _mixedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "mixedSorted_1", buildOverlappingSortedRecords(1), false));
    _mixedSegments.add(buildSegment(UNSORTED_TABLE_CONFIG, "mixedUnsorted_0", buildUnsortedRecords(2), false));
    _mixedSegments.add(buildSegment(UNSORTED_TABLE_CONFIG, "mixedUnsorted_1", buildUnsortedRecords(3), false));
  }

  private static List<GenericRow> buildOverlappingSortedRecords(int index) {
    int baseValue = index * NUM_RECORDS_PER_SEGMENT / 2;
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, baseValue + i);
      record.putValue(TAIL_COL, NUM_RECORDS_PER_SEGMENT - i);
      // Globally unique across all segments -> a total order when used as the final order-by key.
      record.putValue(VAL_COL, index * 1_000_000 + i);
      // Beyond the int range so a regression that narrows LONG -> INT would change the boxed value.
      record.putValue(LONG_COL, 10_000_000_000L + index * 1_000_000L + i);
      record.putValue(STR_COL, "s_" + index + "_" + i);
      // Every 7th row is null so the null-handling test exercises null projection.
      if (i % 7 == 0) {
        record.addNullValueField(NULLABLE_COL);
      } else {
        record.putValue(NULLABLE_COL, i);
      }
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildDisjointSortedRecords(int index) {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    int baseValue = index * 1000;
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, baseValue + i);
      record.putValue(TAIL_COL, i);
      record.putValue(VAL_COL, baseValue + i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 10_000_000_000L + baseValue + i);
      record.putValue(STR_COL, "d_" + index + "_" + i);
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildLowCardinalityRecords(int index) {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      // 4 distinct values per segment, non-decreasing so the segment is physically sorted on SORTED_COL.
      record.putValue(SORTED_COL, i / 25);
      record.putValue(TAIL_COL, NUM_RECORDS_PER_SEGMENT - i);
      record.putValue(VAL_COL, index * 1_000_000 + i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 10_000_000_000L + index * 1_000_000L + i);
      record.putValue(STR_COL, "l_" + index + "_" + i);
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildUnsortedRecords(int index) {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      // A non-monotonic permutation of [0, NUM_RECORDS_PER_SEGMENT) (7919 is prime and coprime with 100), so the
      // column is genuinely not physically sorted and SelectionPlanNode falls back to a materialized top-K block.
      record.putValue(SORTED_COL, (i * 7919) % NUM_RECORDS_PER_SEGMENT);
      record.putValue(TAIL_COL, i);
      record.putValue(VAL_COL, index * 1_000_000 + i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 10_000_000_000L + index * 1_000_000L + i);
      record.putValue(STR_COL, "u_" + index + "_" + i);
      records.add(record);
    }
    return records;
  }

  private static IndexSegment buildSegment(TableConfig tableConfig, String segmentName, List<GenericRow> records,
      boolean nullHandling)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(nullHandling);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
  }

  @Test
  public void testAscendingParity() {
    assertParity(_sortedSegments, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", false);
  }

  @Test
  public void testDescendingParity() {
    // Reverse order must be allowed for the per-segment forward-scan to iterate sortedCol descending; otherwise the
    // segment falls back to the materialized DESC operator (covered separately by testDescIncompatibleFallbackParity).
    assertParity(_sortedSegments,
        "SET allowReverseOrder=true; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, valCol DESC "
            + "LIMIT 50", false);
  }

  @Test
  public void testDescIncompatibleFallbackParity() {
    // allowReverseOrder=false + DESC -> the streaming child cannot scan descending, so SelectionPlanNode emits the
    // materialized DESC top-K block; the combine still routes to the streaming combine and merges single-block cursors.
    assertParity(_sortedSegments,
        "SET allowReverseOrder=false; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, valCol DESC "
            + "LIMIT 50", false);
  }

  @Test
  public void testLimitOffsetParity() {
    // The server retains limit + offset rows; the broker applies the offset later, so both operators keep 40 rows.
    assertParity(_sortedSegments,
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 30 OFFSET 10", false);
  }

  @Test
  public void testLowCardinalityMultiColumnParity() {
    // Low-cardinality primary column => long runs and many sortedCol ties at the prune boundary; valCol breaks ties.
    assertParity(_lowCardSegments,
        "SELECT sortedCol, tailCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 40", false);
  }

  @Test
  public void testTwoPhaseSelectNonOrderByParity() {
    // tailCol is selected but not an order-by key -> the streaming children take the two-phase (order-by-then-fetch)
    // path. valCol is order-by-only, exercising the phase-1 projection of a non-selected order-by column.
    assertParity(_sortedSegments, "SELECT tailCol, sortedCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50",
        false);
  }

  @Test
  public void testNonIntProjectionParity() {
    // Projects a LONG and a STRING column so the multiset comparison would catch a stored-type / boxing regression that
    // an all-INT projection cannot observe.
    assertParity(_sortedSegments,
        "SELECT strCol, longCol, sortedCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", false);
  }

  @Test
  public void testMixedSortedAndUnsortedSegmentsParity() {
    assertParity(_mixedSegments, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", false);
  }

  @Test
  public void testNullHandlingEnabledParity() {
    // Null handling on disables min/max pruning (the combine activates every segment); nullableCol carries real nulls.
    assertParity(_sortedSegments,
        "SELECT nullableCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", true);
  }

  @Test
  public void testNullHandlingDisabledParity() {
    // Same segments/query as the enabled case but null handling off: nulls read back as the column default, pruning on.
    assertParity(_sortedSegments,
        "SELECT nullableCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", false);
  }

  @Test
  public void testStreamingMultiBlockExactCount() {
    // A precise check on the bounded-flush behavior: 20 rows flushed in blocks of 3 yields ceil(20/3) = 7 data blocks.
    int blockSize = 3;
    int limit = 20;
    @Language("sql") String query =
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT " + limit;
    Result streaming = run(_sortedSegments, query, true, false, true, blockSize);
    assertEquals(streaming._combineOperator.getClass(), StreamingSelectionOrderByCombineOperator.class);
    assertEquals(streaming._rows.size(), limit);
    assertEquals(streaming._numBlocks, (limit + blockSize - 1) / blockSize, "Unexpected number of streamed blocks");
    assertSorted(streaming._rows, orderByComparator(query, false));
    assertMultisetEquals(streaming._rows, run(_sortedSegments, query, false, false, false, 0)._rows);
  }

  @Test
  public void testPruningSkipsOutOfTopKSegments() {
    // Disjoint, globally-increasing ranges + small LIMIT: only the lowest segment can contribute, the rest are pruned
    // (never acquired or scanned), so far fewer than all docs are scanned.
    Result result = run(_disjointSegments, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 5",
        true, false, false, 0);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    assertEquals(result._rows.size(), 5);
    for (int i = 0; i < 5; i++) {
      assertEquals((int) result._rows.get(i)[0], i, "Unexpected value at position " + i);
    }
    int totalDocs = NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT;
    assertTrue(result._numDocsScanned < totalDocs, "Pruning should avoid scanning every doc, scanned: "
        + result._numDocsScanned + " of " + totalDocs);
    assertTrue(result._numDocsScanned <= NUM_RECORDS_PER_SEGMENT,
        "Only the lowest-range segment should be scanned, but docs scanned was: " + result._numDocsScanned);
  }

  @Test
  public void testEmptyResultSchemaFallback() {
    // A filter that matches nothing: every streaming child returns no rows, so the combine rebuilds the result schema
    // from the segment metadata (order-by expressions first) rather than from a child block.
    Result result = run(_sortedSegments,
        "SELECT sortedCol, valCol FROM testTable WHERE sortedCol < 0 ORDER BY sortedCol, valCol LIMIT 10", true, false,
        false, 0);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    assertTrue(result._rows.isEmpty(), "Expected an empty result, got: " + result._rows.size() + " rows");
    assertEquals(result._schema, new DataSchema(new String[]{SORTED_COL, VAL_COL},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT}));
  }

  @Test
  public void testHintOffSelectsMinMaxOperator() {
    // Default behavior is unchanged when the hint is off: the classic MinMax operator is still selected.
    Result result = run(_sortedSegments, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50",
        false, false, false, 0);
    assertTrue(result._combineOperator instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "Hint off must keep the default MinMax combine operator, got: "
            + result._combineOperator.getClass().getSimpleName());
  }

  @Test
  public void testNonIdentifierOrderByFallsBack() {
    // Even with the hint on, a non-identifier first order-by expression falls back to SelectionOrderByCombineOperator
    // (the streaming operator and its segment-level counterpart only support a leading identifier).
    Result result = run(_sortedSegments,
        "SELECT sortedCol, valCol FROM testTable ORDER BY ADD(sortedCol, 1), valCol LIMIT 50", true, false, false, 0);
    assertEquals(result._combineOperator.getClass(), SelectionOrderByCombineOperator.class,
        "Non-identifier first order-by must fall back to SelectionOrderByCombineOperator, got: "
            + result._combineOperator.getClass().getSimpleName());
  }

  /**
   * Asserts streaming-vs-non-streaming parity for {@code query}. Runs the MinMax combine (hint off) as the reference,
   * then runs the streaming combine in BOTH single-block mode and bounded multi-block streaming mode, asserting each
   * selects the streaming operator and produces rows that are sorted by the order-by comparator and equal the MinMax
   * rows as a multiset. The streaming variant additionally checks the bounded-flush invariants.
   */
  private void assertParity(List<IndexSegment> segments, @Language("sql") String query, boolean nullHandling) {
    Result baseline = run(segments, query, false, nullHandling, false, 0);
    assertEquals(baseline._combineOperator.getClass(), MinMaxValueBasedSelectionOrderByCombineOperator.class,
        "Baseline must be the MinMax combine operator, got: " + baseline._combineOperator.getClass().getSimpleName());
    Comparator<Object[]> comparator = orderByComparator(query, nullHandling);

    // Classic single-stage path (null streamer): the streaming combine flushes the whole merge as one block.
    Result singleStage = run(segments, query, true, nullHandling, false, 0);
    assertStreamingParity(singleStage, baseline, comparator, query, false, 0);

    // MSE leaf path (non-null streamer): a small block size forces several bounded data blocks before the metadata
    // block, genuinely exercising the streaming flush path rather than a single trimmed block.
    int blockSize = 3;
    Result streamed = run(segments, query, true, nullHandling, true, blockSize);
    assertStreamingParity(streamed, baseline, comparator, query, true, blockSize);
  }

  private void assertStreamingParity(Result result, Result baseline, Comparator<Object[]> comparator,
      @Language("sql") String query, boolean streaming, int blockSize) {
    assertEquals(result._combineOperator.getClass(), StreamingSelectionOrderByCombineOperator.class,
        "Expected the streaming combine operator for query: " + query);
    assertEquals(result._schema, baseline._schema, "Schema mismatch for query: " + query);
    assertSorted(result._rows, comparator);
    assertMultisetEquals(result._rows, baseline._rows);
    if (streaming) {
      int total = 0;
      for (int size : result._blockSizes) {
        assertTrue(size > 0 && size <= blockSize,
            "Streamed block size out of range (0, " + blockSize + "] for query " + query + ": " + size);
        total += size;
      }
      assertEquals(total, result._rows.size(), "Streamed block sizes must sum to the row count for query: " + query);
      if (result._rows.size() > blockSize) {
        assertTrue(result._numBlocks >= 2, "Expected multiple streamed blocks for query: " + query);
      }
    }
  }

  /** Runs one combine over {@code segments} and collects its rows, blocks, schema and docs-scanned stat. */
  private Result run(List<IndexSegment> segments, @Language("sql") String query, boolean hintOn, boolean nullHandling,
      boolean streaming, int blockSize) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setNullHandlingEnabled(nullHandling);
    if (hintOn) {
      queryContext.setSortedSelectionMergeEnabled(true);
      if (blockSize > 0) {
        queryContext.setSortedSelectionMergeBlockSize(blockSize);
      }
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    List<PlanNode> planNodes = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      SegmentContext segmentContext = new SegmentContext(segment);
      planNodes.add(streaming ? PLAN_MAKER.makeStreamingSegmentPlanNode(segmentContext, queryContext)
          : PLAN_MAKER.makeSegmentPlanNode(segmentContext, queryContext));
    }
    ResultsBlockStreamer streamer = streaming ? block -> {
    } : null;
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, streamer);

    Result result = new Result();
    Operator<?> combineOperator = combinePlanNode.run();
    result._combineOperator = combineOperator;
    result._rows = new ArrayList<>();
    result._blockSizes = new ArrayList<>();
    if (streaming) {
      // Drive the streaming combine: collect bounded data blocks until the terminal metadata block, which carries the
      // aggregated execution stats.
      while (true) {
        BaseResultsBlock block = (BaseResultsBlock) combineOperator.nextBlock();
        if (block instanceof MetadataResultsBlock) {
          if (result._schema == null) {
            result._schema = block.getDataSchema();
          }
          result._numDocsScanned = block.getNumDocsScanned();
          break;
        }
        SelectionResultsBlock dataBlock = (SelectionResultsBlock) block;
        if (result._schema == null) {
          result._schema = dataBlock.getDataSchema();
        }
        List<Object[]> rows = dataBlock.getRows();
        assertNotNull(rows);
        result._rows.addAll(rows);
        result._blockSizes.add(rows.size());
        result._numBlocks++;
        assertTrue(result._numBlocks < 1_000_000, "Streaming combine did not terminate");
      }
    } else {
      SelectionResultsBlock block = (SelectionResultsBlock) combineOperator.nextBlock();
      result._schema = block.getDataSchema();
      List<Object[]> rows = block.getRows();
      assertNotNull(rows);
      result._rows.addAll(rows);
      result._blockSizes.add(rows.size());
      result._numBlocks = 1;
      result._numDocsScanned = block.getNumDocsScanned();
    }
    return result;
  }

  private static Comparator<Object[]> orderByComparator(@Language("sql") String query, boolean nullHandling) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assertNotNull(orderByExpressions);
    return OrderByComparatorFactory.getComparator(orderByExpressions, nullHandling);
  }

  private static void assertSorted(List<Object[]> rows, Comparator<Object[]> comparator) {
    for (int i = 1; i < rows.size(); i++) {
      assertTrue(comparator.compare(rows.get(i - 1), rows.get(i)) <= 0,
          "Rows not sorted by the order-by comparator at position " + i);
    }
  }

  /** Asserts the two row lists contain the same rows, independent of the ordering of fully-equal projected rows. */
  private static void assertMultisetEquals(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(toCanonical(actual), toCanonical(expected), "Row multisets differ");
  }

  /**
   * Canonicalizes rows for multiset comparison. Each cell is encoded with its runtime class so a stored-type / boxing
   * regression (e.g. a LONG emitted where the reference emits INT) changes the encoding and fails the assertion, which
   * a plain {@code Arrays.toString} (type-blind) comparison would miss.
   */
  private static List<String> toCanonical(List<Object[]> rows) {
    return rows.stream().map(row -> {
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < row.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        Object cell = row[i];
        sb.append(cell == null ? "null" : cell.getClass().getSimpleName() + ":" + cell);
      }
      return sb.append(']').toString();
    }).sorted().collect(Collectors.toList());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    EXECUTOR.shutdownNow();
    for (List<IndexSegment> segments : List.of(_sortedSegments, _disjointSegments, _mixedSegments, _lowCardSegments)) {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /** Captured output of a single combine run. */
  private static class Result {
    private Operator<?> _combineOperator;
    private DataSchema _schema;
    private List<Object[]> _rows;
    private List<Integer> _blockSizes;
    private int _numBlocks;
    private long _numDocsScanned;
  }
}
