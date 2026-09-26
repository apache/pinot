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
package org.apache.pinot.core.operator.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
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
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Segment-level tests for {@link StreamingSelectionOrderByOperator}.
///
/// <p>The operator emits the same globally-sorted rows as the existing materialized selection ORDER BY operators, only
/// spread across many lazily-produced blocks. Each test therefore asserts <b>stream-vs-materialized parity</b>: it
/// drives the streaming operator through {@link SelectionPlanNode} with {@code sortedSelectionMergeMode=ON},
/// concatenates
/// every {@link Operator#nextBlock()} output until {@code null}, and asserts the concatenation equals the single block
/// the materialized operator ({@link SelectionPartiallyOrderedByLinearOperator} / {@link SelectionOrderByOperator})
/// produces for the identical query with the hint off.
///
/// <p>To keep element-wise comparison deterministic (priority-queue draining is not stable for rows that tie on every
/// order-by column) each fixture makes the order-by column tuple unique per row: the {@code _segment} fixture has a
/// unique sorted column, while the {@code _dupSegment} / {@code _largeSegment} fixtures repeat the sorted column but
/// pair it with a unique {@code TAIL_COL} order-by tail. The all-order-by-columns-tie case (where stream and
/// materialized output may legitimately differ in row order) is intentionally out of scope here and is covered by the
/// combine-level test via multiset comparison.
public class StreamingSelectionOrderByOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StreamingSelectionOrderByOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";

  private static final String SORTED_COL = "sortedCol";
  private static final String TAIL_COL = "tailCol";
  private static final String VAL_COL = "valCol";
  private static final String NULLABLE_COL = "nullableCol";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSortedColumn(SORTED_COL).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(SORTED_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(TAIL_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(VAL_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(NULLABLE_COL, FieldSpec.DataType.INT)
      .build();

  /// Unique sorted column, no nulls. Exercises the no-tail emission mode.
  private static final int NUM_RECORDS = 30;
  /// Repeated sorted column (RUN_SIZE rows per value) paired with a unique tail. Exercises the run / heap path, and
  /// carries nulls in NULLABLE_COL for the null-handling cases.
  private static final int NUM_DISTINCT_SORTED = 10;
  private static final int RUN_SIZE = 4;
  private static final int NUM_DUP_RECORDS = NUM_DISTINCT_SORTED * RUN_SIZE;
  /// A single primary-value run larger than one project block (DocIdSetPlanNode.MAX_DOC_PER_CALL == 10_000), so the
  /// forward scan crosses a block boundary within one run.
  private static final int NUM_LARGE_RECORDS = 12_000;
  /// Two runs where the first is larger than one project block: with a limit greater than the first run's size the
  /// operator both crosses a project-block boundary mid-run AND carries _pendingRow across a run boundary to emit a
  /// second block.
  private static final int MULTI_RUN0_SIZE = 10_500;
  private static final int MULTI_RUN1_SIZE = 1_000;
  private static final int NUM_MULTI_RUN_RECORDS = MULTI_RUN0_SIZE + MULTI_RUN1_SIZE;
  /// Project block size used by the empty-block injection tests only, small enough that a modest fixture spans
  /// several blocks so the injected empty block lands mid-scan.
  private static final int MAX_DOCS_PER_PROJECT_BLOCK = 4;

  private IndexSegment _segment;
  private IndexSegment _dupSegment;
  private IndexSegment _largeSegment;
  private IndexSegment _multiRunSegment;
  /// Carries nulls in an order-by (tail) column so the streaming operator's own null path (phase-1 materialization and
  /// the null-aware comparator) is exercised, not just the shared two-phase fetch.
  private IndexSegment _nullTailSegment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _segment = buildSegment("uniqueSorted", buildUniqueSortedRecords(), false);
    _dupSegment = buildSegment("dupSorted", buildDupSortedRecords(), true);
    _largeSegment = buildSegment("largeRun", buildLargeRunRecords(), false);
    _multiRunSegment = buildSegment("multiRun", buildMultiRunRecords(), false);
    _nullTailSegment = buildSegment("nullTail", buildNullTailRecords(), true);
  }

  private static List<GenericRow> buildUniqueSortedRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, i);
      record.putValue(TAIL_COL, NUM_RECORDS - i);
      record.putValue(VAL_COL, i * 3);
      record.putValue(NULLABLE_COL, i);
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildDupSortedRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_DUP_RECORDS);
    for (int i = 0; i < NUM_DUP_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, i / RUN_SIZE);
      // Tail resets to a descending sequence within each run so the column is NOT globally sorted (the run/heap path is
      // only taken when the tail is unsorted); the (sortedCol, tailCol) tuple is still unique per row.
      record.putValue(TAIL_COL, RUN_SIZE - 1 - (i % RUN_SIZE));
      record.putValue(VAL_COL, i * 2);
      // Every third row carries a null so the two-phase fetch is exercised with and without null handling.
      if (i % 3 == 0) {
        record.addNullValueField(NULLABLE_COL);
      } else {
        record.putValue(NULLABLE_COL, i);
      }
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildLargeRunRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_LARGE_RECORDS);
    for (int i = 0; i < NUM_LARGE_RECORDS; i++) {
      GenericRow record = new GenericRow();
      // All rows share one sorted value, so they form a single run spanning multiple project blocks.
      record.putValue(SORTED_COL, 0);
      // A non-monotonic permutation of [0, NUM_LARGE_RECORDS) (7919 is prime and coprime with NUM_LARGE_RECORDS, so the
      // mapping is a bijection): unique tail values that are not physically sorted, forcing the run/heap path.
      record.putValue(TAIL_COL, (i * 7919) % NUM_LARGE_RECORDS);
      record.putValue(VAL_COL, i);
      record.putValue(NULLABLE_COL, i);
      records.add(record);
    }
    return records;
  }

  private static List<GenericRow> buildMultiRunRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_MULTI_RUN_RECORDS);
    appendRun(records, 0, MULTI_RUN0_SIZE);
    appendRun(records, 1, MULTI_RUN1_SIZE);
    return records;
  }

  /// Appends one run of {@code runSize} rows all sharing {@code sortedValue}, with a tail that descends within the run
  /// (so the tail column is not globally sorted) and is unique per (sortedCol, tailCol) tuple.
  private static void appendRun(List<GenericRow> records, int sortedValue, int runSize) {
    for (int j = 0; j < runSize; j++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, sortedValue);
      record.putValue(TAIL_COL, runSize - 1 - j);
      record.putValue(VAL_COL, sortedValue * 1_000_000 + j);
      record.putValue(NULLABLE_COL, j);
      records.add(record);
    }
  }

  private static List<GenericRow> buildNullTailRecords() {
    List<GenericRow> records = new ArrayList<>();
    // Three runs, each with exactly one null tail and two distinct non-null tails, so ordering stays deterministic
    // (no all-order-by-columns tie) while a null flows through the order-by tail column.
    for (int g = 0; g < 3; g++) {
      for (int j = 0; j < 3; j++) {
        GenericRow record = new GenericRow();
        record.putValue(SORTED_COL, g);
        if (j == 0) {
          record.addNullValueField(TAIL_COL);
        } else {
          record.putValue(TAIL_COL, g * 10 + j);
        }
        record.putValue(VAL_COL, g * 100 + j);
        record.putValue(NULLABLE_COL, g * 100 + j);
        records.add(record);
      }
    }
    return records;
  }

  private static IndexSegment buildSegment(String segmentName, List<GenericRow> records, boolean nullHandling)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
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
  public void testSingleSortedColumnAscending() {
    assertParity(_segment, "SELECT sortedCol FROM testTable ORDER BY sortedCol", false, 1);
  }

  @Test
  public void testSingleSortedColumnDescending() {
    // Reverse order must be allowed for the forward-scan project to iterate the sorted column descending; otherwise
    // SelectionPlanNode falls back to the materialized DESC operator and the streaming operator is never built.
    assertParity(_segment, "SET allowReverseOrder=true; SELECT sortedCol FROM testTable ORDER BY sortedCol DESC", false,
        1);
  }

  @Test
  public void testSortedPrefixWithUnsortedTail() {
    // Repeated sorted value + unique tail exercises nextRun(): per-run top-K heap and the one-row lookahead. With the
    // default LIMIT 10 spanning ~3 runs of 4, the streaming operator emits more than one block.
    assertParity(_dupSegment, "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol", false, 2);
  }

  @Test
  public void testTwoPhaseSingleSortedColumn() {
    // Output has a non-order-by column (valCol) -> two-phase fetch. Unique sorted column keeps ordering deterministic.
    assertParity(_segment, "SELECT valCol, sortedCol FROM testTable ORDER BY sortedCol", false, 1);
  }

  @Test
  public void testTwoPhaseWithUnsortedTail() {
    // Two-phase fetch combined with the run/heap path (sorted prefix + unsorted tail).
    assertParity(_dupSegment, "SELECT valCol FROM testTable ORDER BY sortedCol, tailCol", false, 1);
  }

  @Test
  public void testLimitOffsetNoTail() {
    // Server retains limit + offset rows; the broker applies the offset later.
    assertParity(_segment, "SELECT sortedCol FROM testTable ORDER BY sortedCol LIMIT 5 OFFSET 3", false, 1);
  }

  @Test
  public void testLimitOffsetWithTail() {
    assertParity(_dupSegment, "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol LIMIT 7 OFFSET 5",
        false, 1);
  }

  @Test
  public void testRunSpanningMultipleProjectBlocks() {
    // One run of 12_000 rows (> one 10k project block); the heap caps at limit + offset while the scan crosses the
    // block boundary. A single primary value means a single run, hence a single emitted block.
    StreamingSelectionOrderByOperator operator = assertParity(_largeSegment,
        "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol LIMIT 25", false, 1);
    // The whole run is scanned to find its top-K, which only happens if the forward scan pulled every project block
    // (proving the scan genuinely crossed the 10k boundary rather than stopping at the first block).
    assertEquals(operator.getExecutionStatistics().getNumDocsScanned(), NUM_LARGE_RECORDS);
  }

  @Test
  public void testRunBoundaryAcrossProjectBlocks() {
    // First run (10_500 rows) is larger than one project block, and the limit (10_600) exceeds it, so the operator
    // crosses a project-block boundary mid-run AND carries _pendingRow across the run boundary to emit a second block.
    StreamingSelectionOrderByOperator operator = assertParity(_multiRunSegment,
        "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol LIMIT 10600", false, 2);
    assertEquals(operator.getExecutionStatistics().getNumDocsScanned(), NUM_MULTI_RUN_RECORDS);
  }

  @Test
  public void testNullHandlingEnabled() {
    assertParity(_dupSegment, "SELECT nullableCol, sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol", true,
        1);
  }

  @Test
  public void testNullHandlingDisabled() {
    assertParity(_dupSegment, "SELECT nullableCol, sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol",
        false, 1);
  }

  @Test
  public void testNullInOrderByColumnWithNullHandling() {
    // Drives a null through the order-by tail column (not just the carried non-order-by column), exercising the
    // streaming operator's null-aware comparator and phase-1 null materialization.
    String query = "SELECT tailCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, tailCol";
    assertParity(_nullTailSegment, query, true, 1);
    // Absolute anchor: with null handling on, the order-by tail column (index 1 after extractExpressions) actually
    // carries nulls through to the output. Guards against both operators substituting a default value identically.
    List<Object[]> rows = collectStreamingRows(_nullTailSegment, query, true);
    assertTrue(rows.stream().anyMatch(row -> row[1] == null), "Expected a null in the order-by tail column");
  }

  @Test
  public void testNullInOrderByColumnWithoutNullHandling() {
    String query = "SELECT tailCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, tailCol";
    assertParity(_nullTailSegment, query, false, 1);
    // With null handling off the null reads back as the column's default value, so no output cell is null.
    List<Object[]> rows = collectStreamingRows(_nullTailSegment, query, false);
    assertTrue(rows.stream().noneMatch(row -> row[1] == null),
        "Expected no nulls in the output when null handling is disabled");
  }

  /// No-tail mode, single phase: {@link StreamingSelectionOrderByOperator} must skip a zero-document project block
  /// rather than read it as end-of-segment.
  @Test
  public void testEmptyProjectBlockDoesNotTruncateSortedScan() {
    assertEmptyProjectBlocksAreSkipped(_segment, "SELECT sortedCol FROM testTable ORDER BY sortedCol LIMIT 25", 1, 2,
        1, 0);
  }

  /// Same, two phase: this is also the only path that asks the injected block for {@code getDocIds()}.
  @Test
  public void testEmptyProjectBlockDoesNotTruncateTwoPhaseSortedScan() {
    assertEmptyProjectBlocksAreSkipped(_segment, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol LIMIT 25",
        1, 2, 1, 0);
  }

  /// Tail mode: the run scan reaches the project operator through {@code nextRow()}, which already tolerated empty
  /// blocks. Pins that the two scan paths agree.
  @Test
  public void testEmptyProjectBlockDoesNotTruncateRunScan() {
    assertEmptyProjectBlocksAreSkipped(_dupSegment,
        "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol LIMIT 25", 1, 2, 1, 0);
  }

  /// The skip is a loop, not a single lookahead: several empty blocks in a row must all be skipped.
  @Test
  public void testConsecutiveEmptyProjectBlocksAreAllSkipped() {
    assertEmptyProjectBlocksAreSkipped(_segment, "SELECT sortedCol FROM testTable ORDER BY sortedCol LIMIT 25", 1, 2,
        3, 0);
  }

  /// Empty blocks arriving immediately before exhaustion exercise the loop's other exit: the skip must fall through
  /// to the project operator's {@code null} rather than spin or emit a phantom row. The limit exceeds the fixture so
  /// the scan actually reaches the end instead of stopping on the row budget.
  @Test
  public void testEmptyProjectBlocksBeforeExhaustionEndTheScan() {
    assertEmptyProjectBlocksAreSkipped(_segment, "SELECT sortedCol FROM testTable ORDER BY sortedCol LIMIT 100", 1, 0,
        0, 2);
  }

  /// The skip loop sits directly on top of the {@code limit + offset} budget bookkeeping, so cover a non-zero offset.
  @Test
  public void testEmptyProjectBlockIsSkippedWithOffset() {
    assertEmptyProjectBlocksAreSkipped(_segment,
        "SELECT sortedCol FROM testTable ORDER BY sortedCol LIMIT 15 OFFSET 10", 1, 2, 1, 0);
  }

  /// {@code nextRow()} rebuilds the phase-1 null bitmaps in the same branch that pulls the next non-empty block, so
  /// the skip must not desynchronise a bitmap from the block it was built against. Uses the fixture that carries a
  /// null in the order-by tail column, with null handling on.
  @Test
  public void testEmptyProjectBlockIsSkippedWithNullHandling() {
    assertEmptyProjectBlocksAreSkipped(_nullTailSegment,
        "SELECT tailCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, tailCol LIMIT 25", 1, true, 2, 1, 0);
  }

  @Test
  public void testZeroMatchSegmentEmitsOneSchemaBlock() {
    assertZeroMatchEmitsOneSchemaBlock(_segment, "SELECT sortedCol FROM testTable WHERE sortedCol < 0 ORDER BY "
        + "sortedCol", "SELECT sortedCol FROM testTable ORDER BY sortedCol", 1, false);
  }

  @Test
  public void testZeroMatchSegmentEmitsOneSchemaBlockTwoPhase() {
    assertZeroMatchEmitsOneSchemaBlock(_segment, "SELECT sortedCol, valCol FROM testTable WHERE sortedCol < 0 ORDER BY "
        + "sortedCol", "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol", 1, false);
  }

  @Test
  public void testZeroMatchSegmentEmitsOneSchemaBlockWithTailToSort() {
    assertZeroMatchEmitsOneSchemaBlock(_dupSegment, "SELECT sortedCol, tailCol FROM testTable WHERE sortedCol < 0 "
        + "ORDER BY sortedCol, tailCol", "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol", 1,
        false);
  }

  @Test
  public void testZeroMatchSegmentEmitsOneSchemaBlockWithNullHandling() {
    assertZeroMatchEmitsOneSchemaBlock(_nullTailSegment, "SELECT tailCol, sortedCol, valCol FROM testTable WHERE "
            + "sortedCol < 0 ORDER BY sortedCol, tailCol",
        "SELECT tailCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, tailCol", 1, true);
  }

  @Test
  public void testMatchingSegmentEmitsNoTrailingEmptyBlock() {
    assertNoEmptyBlockAmongEmittedBlocks(_segment, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol", 1,
        false);
  }

  @Test
  public void testMatchingSegmentEmitsNoTrailingEmptyBlockWithTailToSort() {
    assertNoEmptyBlockAmongEmittedBlocks(_dupSegment,
        "SELECT sortedCol, tailCol, valCol FROM testTable ORDER BY sortedCol, tailCol", 1, false);
  }

  @Test
  public void testMatchingSegmentEmitsNoTrailingEmptyBlockWithNullHandling() {
    assertNoEmptyBlockAmongEmittedBlocks(_nullTailSegment,
        "SELECT tailCol, sortedCol, valCol FROM testTable ORDER BY sortedCol, tailCol", 1, true);
  }

  /// A DESC query that cannot iterate the forward index in reverse falls through to the materialized operator,
  /// reusing the streaming attempt's project or building a wider one. Both must match the plan that never entered
  /// the streaming branch.
  @Test
  public void testDescIncompatibleFallbackMatchesTheMaterializedPlan() {
    // Streaming project covers everything, so it is reused.
    assertDescIncompatibleFallback(
        "SET allowReverseOrder=false; SELECT sortedCol FROM testTable ORDER BY sortedCol DESC", true);
    // Streaming project covers the order-by prefix only, so the fallback builds its own.
    assertDescIncompatibleFallback(
        "SET allowReverseOrder=false; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC", true);
    // Zero-match: the reused project is driven to exhaustion without yielding a row.
    assertDescIncompatibleFallback(
        "SET allowReverseOrder=false; SELECT sortedCol FROM testTable WHERE sortedCol < 0 ORDER BY sortedCol DESC",
        false);
    // Not covered: allowReverseOrder on but withOrder(DESC) throwing. No fixture reaches it, and it lands on this
    // same fall-through anyway.
  }

  private void assertDescIncompatibleFallback(@Language("sql") String query, boolean expectRows) {
    SelectionResultsBlock block =
        planWithCountedDataSourceLookups(query, SortedSelectionMergeMode.ON, new AtomicInteger()).nextBlock();
    assertNotNull(block, "The fallback operator must emit a block");
    List<Object[]> rows = block.getRows();
    if (expectRows) {
      assertFalse(rows.isEmpty(), "The fallback must still produce rows, else the assertions below are vacuous");
    } else {
      assertTrue(rows.isEmpty(), "A zero-match query must not produce rows");
    }

    SelectionResultsBlock baselineBlock =
        planWithCountedDataSourceLookups(query, SortedSelectionMergeMode.OFF, new AtomicInteger()).nextBlock();
    assertNotNull(baselineBlock);
    List<Object[]> expectedRows = baselineBlock.getRows();
    assertEquals(block.getDataSchema(), baselineBlock.getDataSchema(), "Schema mismatch for query: " + query);
    assertEquals(rows.size(), expectedRows.size(), "Row count mismatch for query: " + query);
    for (int i = 0; i < expectedRows.size(); i++) {
      assertEquals(rows.get(i), expectedRows.get(i), "Row " + i + " mismatch for query: " + query);
    }
  }

  /// Each {@link ProjectPlanNode} build looks up one data source per projected column, so a plan that builds the
  /// project twice looks up more than the materialized plan, which builds it once.
  @Test
  public void testDescIncompatibleFallbackBuildsTheProjectOnce() {
    @Language("sql") String query =
        "SET allowReverseOrder=false; SELECT sortedCol FROM testTable ORDER BY sortedCol DESC";
    AtomicInteger numStreamingLookups = new AtomicInteger();
    AtomicInteger numMaterializedLookups = new AtomicInteger();
    planWithCountedDataSourceLookups(query, SortedSelectionMergeMode.ON, numStreamingLookups);
    planWithCountedDataSourceLookups(query, SortedSelectionMergeMode.OFF, numMaterializedLookups);
    assertTrue(numMaterializedLookups.get() > 0, "Baseline must look up at least one data source");
    assertEquals(numStreamingLookups.get(), numMaterializedLookups.get(),
        "The DESC-incompatible fall-through built the project twice");
  }

  /// Plans {@code query} in {@code mode} against a segment counting data source lookups into {@code numLookups},
  /// asserting it lands on the materialized DESC operator.
  private Operator<SelectionResultsBlock> planWithCountedDataSourceLookups(@Language("sql") String query,
      SortedSelectionMergeMode mode, AtomicInteger numLookups) {
    IndexSegment countingSegment = mock(IndexSegment.class, delegatesTo(_segment));
    // doAnswer, not when(...): building a when() on a delegating mock forwards to the real segment with nulls.
    doAnswer(invocation -> {
      numLookups.incrementAndGet();
      return _segment.getDataSource(invocation.getArgument(0), invocation.getArgument(1));
    }).when(countingSegment).getDataSource(anyString(), any());
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSortedSelectionMergeMode(mode);
    Operator<SelectionResultsBlock> operator =
        new SelectionPlanNode(new SegmentContext(countingSegment), queryContext).run();
    assertTrue(operator instanceof SelectionPartiallyOrderedByDescOperation,
        "DESC without reverse order must fall back to the materialized DESC operator, got: "
            + operator.getClass().getSimpleName());
    return operator;
  }

  /// Every block the tail-to-sort path emits carries a structurally mutable row list. Defensive rather than a fix for
  /// a reachable failure: no consumer of this operator adds to the list today, but {@link SelectionResultsBlock} is a
  /// shared type and {@code SelectionOperatorUtils.mergeWithoutOrdering()} adds to the row list of the block it merges
  /// into, which a fixed-size {@code Arrays.asList} view would reject.
  @Test
  public void testRunPathEmitsGrowableRowLists() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT sortedCol, tailCol FROM testTable ORDER BY sortedCol, tailCol");
    queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
    Operator<SelectionResultsBlock> operator =
        new SelectionPlanNode(new SegmentContext(_dupSegment), queryContext).run();
    assertTrue(operator instanceof StreamingSelectionOrderByOperator,
        "Expected the streaming operator, got: " + operator.getClass().getSimpleName());

    int numBlocks = 0;
    SelectionResultsBlock block;
    while ((block = operator.nextBlock()) != null) {
      numBlocks++;
      List<Object[]> rows = block.getRows();
      assertFalse(rows.isEmpty(), "Block " + numBlocks + " must carry rows, else the assertion below is vacuous");
      int numRows = rows.size();
      rows.add(new Object[]{0, 0});
      assertEquals(rows.size(), numRows + 1, "Block " + numBlocks + " did not accept an appended row");
    }
    // One block per run, so more than one block proves the tail-to-sort path (and therefore drainAscending) ran; the
    // no-tail path emits a single block for this fixture.
    assertTrue(numBlocks > 1, "Fixture must exercise the tail-to-sort run path, got " + numBlocks + " block(s)");
  }

  /// The counterpart to the zero-match tests: a segment that does match rows must never emit an empty block, so the
  /// guarantee reads "at least one block" and not "always an extra one".
  private void assertNoEmptyBlockAmongEmittedBlocks(IndexSegment segment, @Language("sql") String query,
      int numSortedExpressions, boolean nullHandling) {
    StreamingSelectionOrderByOperator operator =
        buildOperatorWithInjectableProject(segment, query, numSortedExpressions, nullHandling, 0, 0, 0);
    int numBlocks = 0;
    SelectionResultsBlock block;
    while ((block = operator.nextBlock()) != null) {
      numBlocks++;
      assertFalse(block.getRows().isEmpty(),
          "Block " + numBlocks + " is empty: the schema block must only be emitted when no rows were emitted at all");
    }
    assertTrue(numBlocks > 1, "Fixture must span more than one block, otherwise a trailing block cannot be observed");
  }

  /// A segment matching no rows must still emit exactly one block before signalling exhaustion: empty, but carrying
  /// the same {@link DataSchema} the identical query produces when it does match rows. Consumers therefore never have
  /// to reconstruct a schema the segment already knows.
  private void assertZeroMatchEmitsOneSchemaBlock(IndexSegment segment, @Language("sql") String zeroMatchQuery,
      @Language("sql") String matchingQuery, int numSortedExpressions, boolean nullHandling) {
    SelectionResultsBlock matchingBlock =
        buildOperatorWithInjectableProject(segment, matchingQuery, numSortedExpressions, nullHandling, 0, 0, 0)
            .nextBlock();
    assertNotNull(matchingBlock);
    assertFalse(matchingBlock.getRows().isEmpty(), "The control query must actually match rows");
    DataSchema expectedSchema = matchingBlock.getDataSchema();
    assertNotNull(expectedSchema);

    StreamingSelectionOrderByOperator operator =
        buildOperatorWithInjectableProject(segment, zeroMatchQuery, numSortedExpressions, nullHandling, 0, 0, 0);
    SelectionResultsBlock block = operator.nextBlock();
    assertNotNull(block, "A zero-match segment must still emit one block carrying the schema");
    assertTrue(block.getRows().isEmpty(), "A zero-match segment must not emit rows");
    assertEquals(block.getDataSchema(), expectedSchema,
        "The zero-match schema must equal the schema the same query produces when it matches rows");
    assertNull(operator.nextBlock(), "Exactly one block may precede exhaustion");
  }

  /// Drives the operator twice over {@code segment}: once against the real project operator, once against one that
  /// splices zero-document blocks into its output, and asserts the emitted rows are identical.
  ///
  /// <p>No doc-id-set operator produces an empty block today, so the case has to be injected. The project operator is
  /// built with a deliberately small {@code maxDocsPerCall} so the scan spans several blocks.
  ///
  /// @param injectBeforeRealBlock 1-based index of the real block to splice empties in front of; 0 for none
  /// @param numEmptyBlocks how many consecutive empty blocks to splice in at that point
  /// @param numTrailingEmptyBlocks how many empty blocks to emit after the last real block, before exhaustion
  private void assertEmptyProjectBlocksAreSkipped(IndexSegment segment, @Language("sql") String query,
      int numSortedExpressions, int injectBeforeRealBlock, int numEmptyBlocks, int numTrailingEmptyBlocks) {
    assertEmptyProjectBlocksAreSkipped(segment, query, numSortedExpressions, false, injectBeforeRealBlock,
        numEmptyBlocks, numTrailingEmptyBlocks);
  }

  /// As above, with explicit control over null handling.
  private void assertEmptyProjectBlocksAreSkipped(IndexSegment segment, @Language("sql") String query,
      int numSortedExpressions, boolean nullHandling, int injectBeforeRealBlock, int numEmptyBlocks,
      int numTrailingEmptyBlocks) {
    List<Object[]> expected =
        drain(buildOperatorWithInjectableProject(segment, query, numSortedExpressions, nullHandling, 0, 0, 0));
    assertTrue(expected.size() > MAX_DOCS_PER_PROJECT_BLOCK,
        "Fixture must span more than one project block, otherwise the injection is not exercised");
    List<Object[]> actual = drain(
        buildOperatorWithInjectableProject(segment, query, numSortedExpressions, nullHandling, injectBeforeRealBlock,
            numEmptyBlocks, numTrailingEmptyBlocks));
    assertEquals(actual.size(), expected.size(),
        "Row count changed when empty project blocks were injected, so the scan was truncated");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(actual.get(i), expected.get(i), "Row " + i + " mismatch for query: " + query);
    }
  }

  /// Builds a {@link StreamingSelectionOrderByOperator} directly (rather than through {@link SelectionPlanNode}) so
  /// the project operator can be wrapped. Passing 0 for every injection parameter yields the undecorated operator.
  private StreamingSelectionOrderByOperator buildOperatorWithInjectableProject(IndexSegment segment,
      @Language("sql") String query, int numSortedExpressions, boolean nullHandling, int injectBeforeRealBlock,
      int numEmptyBlocks, int numTrailingEmptyBlocks) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setNullHandlingEnabled(nullHandling);
    queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(queryContext, segment);
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assertNotNull(orderByExpressions);
    // Mirrors SelectionPlanNode: only the order-by expressions are scanned when there is a second phase.
    List<ExpressionContext> projectExpressions = expressions;
    if (expressions.size() > orderByExpressions.size()) {
      projectExpressions = new ArrayList<>(orderByExpressions.size());
      for (OrderByExpressionContext orderByExpression : orderByExpressions) {
        projectExpressions.add(orderByExpression.getExpression());
      }
    }
    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(new SegmentContext(segment), queryContext, projectExpressions,
            MAX_DOCS_PER_PROJECT_BLOCK).run();
    BaseProjectOperator<?> effective =
        injectBeforeRealBlock == 0 && numTrailingEmptyBlocks == 0
            ? projectOperator
            : new EmptyBlockInjectingProjectOperator(projectOperator, injectBeforeRealBlock, numEmptyBlocks,
                numTrailingEmptyBlocks);
    return new StreamingSelectionOrderByOperator(segment, queryContext, expressions, effective, numSortedExpressions);
  }

  private static List<Object[]> drain(StreamingSelectionOrderByOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block;
    while ((block = operator.nextBlock()) != null) {
      rows.addAll(block.getRows());
    }
    return rows;
  }

  /// Forwards everything to a real project operator, but splices zero-document blocks into its output:
  /// {@code numEmptyBlocks} of them just before the {@code injectBeforeRealBlock}-th real block, and
  /// {@code numTrailingEmptyBlocks} after the last real block but before exhaustion.
  ///
  /// <p>Every real block is still delivered, merely deferred, so the decorator provably drops nothing of its own -
  /// any row loss observed by a test is the operator under test truncating its scan.
  private static class EmptyBlockInjectingProjectOperator extends BaseProjectOperator<ValueBlock> {
    private final BaseProjectOperator<?> _delegate;
    private final int _injectBeforeRealBlock;
    private final int _numEmptyBlocks;
    private final int _numTrailingEmptyBlocks;
    private final Deque<ValueBlock> _pending = new ArrayDeque<>();
    private int _realBlocksSeen;
    private ValueBlock _lastRealBlock;
    private boolean _injected;
    private boolean _trailingEmitted;

    EmptyBlockInjectingProjectOperator(BaseProjectOperator<?> delegate, int injectBeforeRealBlock, int numEmptyBlocks,
        int numTrailingEmptyBlocks) {
      _delegate = delegate;
      _injectBeforeRealBlock = injectBeforeRealBlock;
      _numEmptyBlocks = numEmptyBlocks;
      _numTrailingEmptyBlocks = numTrailingEmptyBlocks;
    }

    @Override
    protected ValueBlock getNextBlock() {
      if (!_pending.isEmpty()) {
        return _pending.poll();
      }
      ValueBlock block = _delegate.nextBlock();
      if (block == null) {
        // The empty blocks shadow the last real block so their value sets stay usable; there is nothing to shadow if
        // the delegate never produced one, in which case the stream simply ends.
        if (_trailingEmitted || _numTrailingEmptyBlocks == 0 || _lastRealBlock == null) {
          return null;
        }
        _trailingEmitted = true;
        for (int i = 0; i < _numTrailingEmptyBlocks; i++) {
          _pending.add(new EmptyValueBlock(_lastRealBlock));
        }
        return _pending.poll();
      }
      _lastRealBlock = block;
      _realBlocksSeen++;
      if (!_injected && _realBlocksSeen == _injectBeforeRealBlock) {
        _injected = true;
        for (int i = 0; i < _numEmptyBlocks; i++) {
          _pending.add(new EmptyValueBlock(block));
        }
        _pending.add(block);
        return _pending.poll();
      }
      return block;
    }

    @Override
    public Map<String, ColumnContext> getSourceColumnContextMap() {
      return _delegate.getSourceColumnContextMap();
    }

    @Override
    public ColumnContext getResultColumnContext(ExpressionContext expression) {
      return _delegate.getResultColumnContext(expression);
    }

    @Override
    public BaseProjectOperator<ValueBlock> withOrder(DocIdOrder newOrder) {
      throw new UnsupportedOperationException("Test decorator does not support reordering");
    }

    @Override
    public boolean isCompatibleWith(DocIdOrder order) {
      return _delegate.isCompatibleWith(order);
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return _delegate.getExecutionStatistics();
    }

    @Override
    public List<? extends Operator> getChildOperators() {
      return List.of(_delegate);
    }

    @Override
    public String toExplainString() {
      return "EMPTY_BLOCK_INJECTING_PROJECT";
    }
  }

  /// A block reporting zero documents. Value sets are forwarded to the real block it shadows so that a consumer which
  /// builds its fetchers before checking the document count does not fail for the wrong reason.
  private static class EmptyValueBlock implements ValueBlock {
    private final ValueBlock _delegate;

    EmptyValueBlock(ValueBlock delegate) {
      _delegate = delegate;
    }

    @Override
    public int getNumDocs() {
      return 0;
    }

    @Override
    public int[] getDocIds() {
      return new int[0];
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      return _delegate.getBlockValueSet(expression);
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return _delegate.getBlockValueSet(column);
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      return _delegate.getBlockValueSet(paths);
    }
  }

  /// Runs {@code query} twice over {@code segment} - once with the streaming hint on, once off - and asserts the
  /// concatenated streaming blocks equal the materialized operator's single block, cell by cell. Returns the (now
  /// exhausted) streaming operator so callers can make extra assertions on its execution statistics.
  ///
  /// @param expectedMinBlocks the minimum number of non-null blocks the streaming operator must emit. Tail-mode
  ///     cases that span multiple runs pass {@code >= 2} to prove the output is genuinely streamed; cases whose
  ///     result fits in a single trimmed block pass {@code 1}.
  private StreamingSelectionOrderByOperator assertParity(IndexSegment segment, @Language("sql") String query,
      boolean nullHandling, int expectedMinBlocks) {
    // Streaming path.
    QueryContext streamingContext = QueryContextConverterUtils.getQueryContext(query);
    streamingContext.setNullHandlingEnabled(nullHandling);
    streamingContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
    Operator<SelectionResultsBlock> streamingOperator =
        new SelectionPlanNode(new SegmentContext(segment), streamingContext).run();
    assertTrue(streamingOperator instanceof StreamingSelectionOrderByOperator,
        "Expected the streaming operator to be built, got: " + streamingOperator.getClass().getSimpleName());

    List<Object[]> streamingRows = new ArrayList<>();
    DataSchema streamingSchema = null;
    int numBlocks = 0;
    SelectionResultsBlock block;
    while ((block = streamingOperator.nextBlock()) != null) {
      numBlocks++;
      if (streamingSchema == null) {
        streamingSchema = block.getDataSchema();
      }
      streamingRows.addAll(block.getRows());
    }
    assertTrue(numBlocks >= expectedMinBlocks,
        "Expected at least " + expectedMinBlocks + " streaming block(s), got: " + numBlocks);

    // Materialized path (hint off).
    QueryContext materializedContext = QueryContextConverterUtils.getQueryContext(query);
    materializedContext.setNullHandlingEnabled(nullHandling);
    Operator<SelectionResultsBlock> materializedOperator =
        new SelectionPlanNode(new SegmentContext(segment), materializedContext).run();
    assertFalse(materializedOperator instanceof StreamingSelectionOrderByOperator,
        "Materialized baseline must not be the streaming operator");
    SelectionResultsBlock materializedBlock = materializedOperator.nextBlock();
    assertNotNull(materializedBlock);
    List<Object[]> expectedRows = materializedBlock.getRows();

    assertEquals(streamingSchema, materializedBlock.getDataSchema(), "Schema mismatch for query: " + query);
    assertEquals(streamingRows.size(), expectedRows.size(), "Row count mismatch for query: " + query);
    for (int i = 0; i < expectedRows.size(); i++) {
      assertEquals(streamingRows.get(i), expectedRows.get(i), "Row " + i + " mismatch for query: " + query);
    }
    return (StreamingSelectionOrderByOperator) streamingOperator;
  }

  /// Drains the streaming operator for {@code query} and returns all rows concatenated in emission order.
  private List<Object[]> collectStreamingRows(IndexSegment segment, @Language("sql") String query,
      boolean nullHandling) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setNullHandlingEnabled(nullHandling);
    queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
    Operator<SelectionResultsBlock> operator = new SelectionPlanNode(new SegmentContext(segment), queryContext).run();
    List<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block;
    while ((block = operator.nextBlock()) != null) {
      rows.addAll(block.getRows());
    }
    return rows;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment segment : new IndexSegment[]{_segment, _dupSegment, _largeSegment, _multiRunSegment,
        _nullTailSegment}) {
      if (segment != null) {
        segment.destroy();
      }
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
