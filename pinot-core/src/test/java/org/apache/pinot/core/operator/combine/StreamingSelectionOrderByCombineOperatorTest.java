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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.StreamingSelectionOrderByOperator;
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
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Combine-level tests for {@link StreamingSelectionOrderByCombineOperator} (step-3 operator) and its wiring into
/// {@link CombinePlanNode#getCombineOperator()} (step-4).
///
/// <p>The streaming combine must return the same globally-sorted top-K rows as the default
/// {@link MinMaxValueBasedSelectionOrderByCombineOperator}, only (in streaming mode) spread across several bounded
/// blocks. Each functional test therefore asserts <b>streaming-vs-non-streaming parity</b>: it runs the identical query
/// twice over the same in-memory segments - once with {@code sortedSelectionMergeMode=ON} (asserting the new
/// operator was actually selected) and once with the hint off (asserting the {@code MinMax} operator was selected) -
/// then checks the two row sets are equal as a multiset and that the streaming output is fully sorted by the order-by
/// comparator.
///
/// <p>To keep the top-K boundary unambiguous (operators may legitimately disagree on which of several rows that tie on
/// every order-by key fall inside the limit) every parity query ends its ORDER BY with the globally-unique
/// {@code valCol}
/// so the comparator is a total order; the merge still genuinely interleaves segments because the primary sort column
/// ({@code sortedCol}) overlaps across segments. Multiset (rather than positional) comparison then tolerates only the
/// harmless reordering of fully-equal projected rows.
public class StreamingSelectionOrderByCombineOperatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "StreamingSelectionOrderByCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";

  private static final String SORTED_COL = "sortedCol";
  private static final String TAIL_COL = "tailCol";
  private static final String VAL_COL = "valCol";
  private static final String NULLABLE_COL = "nullableCol";
  /// Non-INT projected columns so the parity assertion can catch a stored-type / boxing regression (e.g. LONG emitted
  /// where MinMax emits INT), which an all-INT suite cannot observe.
  private static final String LONG_COL = "longCol";
  private static final String STR_COL = "strCol";

  /// Create (MAX_NUM_THREADS_PER_QUERY * 2) sorted segments so the leaf runs plan nodes across multiple threads.
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

  /// Sorted segments with overlapping primary-column ranges, so the k-way merge interleaves them (genuine merge rather
  /// than concatenation). Built with null handling on so the null-handling test sees real nulls in NULLABLE_COL; reads
  /// with null handling off fall back to the column default.
  private List<IndexSegment> _sortedSegments;
  /// Sorted segments with disjoint, globally-increasing ranges: ORDER BY sortedCol with a small LIMIT drains only the
  /// lowest segment, so min/max pruning must skip the rest (none acquired/scanned).
  private List<IndexSegment> _disjointSegments;
  /// A mix of sorted (streaming child) and physically-unsorted (single materialized top-K block child) segments,
  /// exercising both SegmentCursor backings in one merge.
  private List<IndexSegment> _mixedSegments;
  /// Physically sorted on the leading order-by column, but that column also carries real nulls. `isSorted()` on the
  /// metadata still says true, so only the null-bitmap check separates these from [#_sortedSegments].
  private List<IndexSegment> _nullBearingSortedSegments;
  /// No segment is physically sorted on the leading order-by column, so every child is a materializing top-K operator.
  /// This is the shape AUTO must refuse and the shape ON must still get right.
  private List<IndexSegment> _unsortedSegments;
  /// Very low cardinality primary column (4 distinct values across 100 rows) so each value is a long run: exercises the
  /// run/heap path in the streaming children and ties on the primary key at the prune boundary.
  private List<IndexSegment> _lowCardSegments;

  /// A sparse segment plus one beginning inside its first gap; see [#buildGapFillingSortedRecords].
  private List<IndexSegment> _interleavedSegments;

  /// Two segments whose rows tie on every order-by expression; see [#buildTiedSortedRecords].
  private List<IndexSegment> _tiedSegments;

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

    _unsortedSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _unsortedSegments.add(buildSegment(UNSORTED_TABLE_CONFIG, "unsorted_" + i, buildUnsortedRecords(i), false));
    }

    // Nulls occupy the lowest stored values (INT null default is Integer.MIN_VALUE), so sortedCol still ascends and
    // the segment is still built as physically sorted -- exactly the shape where isSorted() alone is not trustworthy.
    _nullBearingSortedSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _nullBearingSortedSegments.add(
          buildSegment(SORTED_TABLE_CONFIG, "nullSorted_" + i, buildNullBearingSortedRecords(i), true));
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

    // Two segments whose rows are identical on the full order-by key; see #testFullOrderByKeyTiesAcrossSegments.
    _tiedSegments = new ArrayList<>(2);
    _tiedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "tied_0", buildTiedSortedRecords("a"), false));
    _tiedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "tied_1", buildTiedSortedRecords("b"), false));

    // Sparse segment plus a dense one starting inside its first gap; see #testLateActivatedCursorWinsImmediately.
    _interleavedSegments = new ArrayList<>(2);
    _interleavedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "sparse_0", buildSparseSortedRecords(), false));
    _interleavedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "denseGap_0", buildGapFillingSortedRecords(), false));
  }

  /// Leading order-by values `0, 1000, 2000, ...`: gaps wide enough for another segment's entire range to sit between
  /// two consecutive rows, with no value shared with it.
  private static List<GenericRow> buildSparseSortedRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, i * 1000);
      record.putValue(TAIL_COL, i);
      // Globally unique against the gap-filling segment below, so the two never tie on the full order-by key.
      record.putValue(VAL_COL, 2_000_000 + i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 10_000_000_000L + i);
      record.putValue(STR_COL, "sp_" + i);
      records.add(record);
    }
    return records;
  }

  /// Rows that are identical on both order-by expressions across every segment built from this, differing only in a
  /// non-order-by column, so the merge's tie branch is reached on every single row.
  private static List<GenericRow> buildTiedSortedRecords(String tag) {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, i);
      record.putValue(TAIL_COL, i);
      // Deliberately NOT unique across segments: sortedCol and valCol are the order-by key, so every row of one
      // segment ties exactly with a row of the other.
      record.putValue(VAL_COL, i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 10_000_000_000L + i);
      record.putValue(STR_COL, tag + "_" + i);
      records.add(record);
    }
    return records;
  }

  /// Leading order-by values `5, 6, ..., 104`: the whole range sits inside the sparse segment's first gap (between its
  /// rows 0 and 1000), so this segment is pruned at the start and activated only once the frontier passes 5.
  private static List<GenericRow> buildGapFillingSortedRecords() {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(SORTED_COL, 5 + i);
      record.putValue(TAIL_COL, i);
      record.putValue(VAL_COL, 3_000_000 + i);
      record.putValue(NULLABLE_COL, i);
      record.putValue(LONG_COL, 20_000_000_000L + i);
      record.putValue(STR_COL, "gf_" + i);
      records.add(record);
    }
    return records;
  }

  /// Like [#buildOverlappingSortedRecords] but the leading order-by column itself is null in the first few rows.
  private static List<GenericRow> buildNullBearingSortedRecords(int index) {
    List<GenericRow> records = buildOverlappingSortedRecords(index);
    for (int i = 0; i < 3; i++) {
      GenericRow record = records.get(i);
      record.putValue(SORTED_COL, null);
      record.addNullValueField(SORTED_COL);
    }
    return records;
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
    // ON forces the path unconditionally, so allowReverseOrder=false + DESC still reaches the streaming combine over
    // materialized DESC top-K blocks -- slower than the baseline, but it must stay correct. AUTO refuses this shape
    // (see testAutoDoesNotSelectStreamingForDescWithoutReverseOrder).
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
        true, false, true, 0);
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

  /// A cursor activated *late* can hold a smaller row than the retained leader. Two things must be right for it to
  /// land in the correct place: the leader is re-compared after `activateEligibleCursors()` rather than before, and
  /// the pruning frontier is the row about to be emitted rather than the last one emitted. Either mistake emits the
  /// leader's larger row first.
  ///
  /// The sparse segment runs `0, 1000, 2000, ...` and the gap filler `5, 6, ..., 104`, so the filler is pruned while
  /// the frontier is 0 and becomes eligible only when the leader reaches 1000 -- where its head (5) must displace it at
  /// once. The ranges share no value, so the expected sequence below is a strict total order with no tie to mask a
  /// misordering. Parity does not cover this: [#assertMultisetEquals] normalises the reordering away, hence the
  /// positional assertion.
  @Test
  public void testLateActivatedCursorWinsImmediately() {
    @Language("sql") String query =
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 8";
    Result result = run(_interleavedSegments, query, true, false, true, 3);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    // 0 from the sparse segment, then the gap filler's 5, 6, 7, ... -- its next row (1000) comes far later.
    int[] expected = {0, 5, 6, 7, 8, 9, 10, 11};
    assertEquals(result._rows.size(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals((int) result._rows.get(i)[0], expected[i],
          "Unexpected value at position " + i + "; a late-activated cursor was not given the lead immediately");
    }
    assertParity(_interleavedSegments, query, false);
  }

  /// Pruning must survive leader retention: one cursor serving a long run leaves the heap empty, and a frontier read
  /// from the heap alone would call that "unknown" and activate every remaining segment -- switching pruning off on
  /// exactly the near-disjoint shape this operator exists for. [#testPruningSkipsOutOfTopKSegments] retains the leader
  /// only a few times; this limit spans several blocks, widening the empty-heap window.
  @Test
  public void testPruningSurvivesALongRetainedRun() {
    Result result = run(_disjointSegments,
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 60", true, false, true, 7);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    assertEquals(result._rows.size(), 60);
    assertTrue(result._numBlocks > 1, "Fixture must span several blocks, else the retained run is too short");
    for (int i = 0; i < 60; i++) {
      assertEquals((int) result._rows.get(i)[0], i, "Unexpected value at position " + i);
    }
    assertTrue(result._numDocsScanned <= NUM_RECORDS_PER_SEGMENT,
        "All 60 rows come from the lowest-range segment, so no other segment may be activated; docs scanned was: "
            + result._numDocsScanned);
  }

  /// The merge cedes the lead only on a strict loss, so a tie keeps the incumbent -- a branch no other fixture reaches,
  /// because they all give `valCol` a globally unique value precisely to avoid ties. Here every row of one segment ties
  /// with a row of the other on the complete order-by key.
  ///
  /// What is asserted is that ties are *harmless*: nothing is dropped, duplicated or emitted out of order. Which of two
  /// fully-tied rows comes first is deliberately not pinned -- the incumbent-wins rule is an artefact of leader
  /// retention, the heap's own tie-breaking was arbitrary before it, and neither path promises stability. Asserting it
  /// would freeze behaviour the operator does not guarantee.
  /// The DESC counterpart of [#testPruningSurvivesALongRetainedRun]. The pruning test in `sortsBeyond` flips its
  /// comparison on `_asc`, and every other live-pruning test runs ASC only -- so a sign flip or swapped operand in the
  /// DESC half would otherwise go unnoticed, silently either over-scanning or, worse, pruning a segment that still had
  /// rows to give.
  @Test
  public void testPruningSurvivesALongRetainedRunDesc() {
    Result result = run(_disjointSegments,
        "SET allowReverseOrder=true; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, valCol DESC "
            + "LIMIT 60", true, false, true, 7);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    assertEquals(result._rows.size(), 60);
    assertTrue(result._numBlocks > 1, "Fixture must span several blocks, else the retained run is too short");
    // DESC activates the highest-range segment first; its top value counts down from there.
    int top = (NUM_SEGMENTS - 1) * 1000 + NUM_RECORDS_PER_SEGMENT - 1;
    for (int i = 0; i < 60; i++) {
      assertEquals((int) result._rows.get(i)[0], top - i, "Unexpected value at position " + i);
    }
    assertTrue(result._numDocsScanned <= NUM_RECORDS_PER_SEGMENT,
        "All 60 rows come from the highest-range segment, so no other segment may be activated; docs scanned was: "
            + result._numDocsScanned);
  }

  @Test
  public void testFullOrderByKeyTiesAcrossSegments() {
    // Explicit limit: the default of 10 would stop the merge long before most ties are reached.
    @Language("sql") String query =
        "SELECT sortedCol, valCol, strCol FROM testTable ORDER BY sortedCol, valCol LIMIT 200";
    assertParity(_tiedSegments, query, false);

    Result streamed = run(_tiedSegments, query, true, false, true, 5);
    assertEquals(streamed._rows.size(), 2 * NUM_RECORDS_PER_SEGMENT, "Every tied row must survive the merge");
    for (int i = 0; i < streamed._rows.size(); i++) {
      // Both segments contribute one row per sortedCol value, so the merged sequence is each value twice.
      assertEquals((int) streamed._rows.get(i)[0], i / 2, "Unexpected value at position " + i);
    }
  }

  @Test
  public void testEmptyResultOnStreamingPath() {
    // A filter that matches nothing. Every streaming child still emits one empty schema-carrying block, so the combine
    // does capture a DataSchema -- but no rows ever reach the output buffer, so it never flushes a data block and
    // exits through the terminal MetadataResultsBlock, whose schema is unconditionally null.
    // Carrying the schema end-to-end therefore means changing that terminal-block convention, which is shared by every
    // BaseStreamingCombineOperator subclass; both the MSE leaf and the gRPC streaming reduce tolerate the null today,
    // so it is pinned here rather than left free to change unnoticed.
    // TODO: revisit if the terminal block ever carries a schema -- the per-child guarantee that would need is now in
    //       place. See https://github.com/apache/pinot/pull/19120#discussion_r3871714002
    Result result = run(_sortedSegments,
        "SELECT sortedCol, valCol FROM testTable WHERE sortedCol < 0 ORDER BY sortedCol, valCol LIMIT 10", true, false,
        true, 0);
    assertTrue(result._combineOperator instanceof StreamingSelectionOrderByCombineOperator);
    assertTrue(result._rows.isEmpty(), "Expected an empty result, got: " + result._rows.size() + " rows");
    assertNull(result._schema, "A zero-match streaming query carries no schema today; see the comment above");
  }

  @Test
  public void testZeroMatchSegmentsMergedAlongsideAMatchingSegment() {
    // Disjoint segment i covers [i * 1000, i * 1000 + 100), so this filter matches every row of the first segment and
    // nothing at all in the rest. Each zero-match child still emits its empty schema-carrying block, so the merge runs
    // cursors that never set a head alongside one that does -- the shape this feature targets once pruning or
    // colocation leaves only part of the fan-out in range.
    @Language("sql") String query =
        "SELECT sortedCol, valCol FROM testTable WHERE sortedCol < 1000 ORDER BY sortedCol, valCol LIMIT 5";
    assertParity(_disjointSegments, query, false);
    Result streamed = run(_disjointSegments, query, true, false, true, 3);
    assertFalse(streamed._rows.isEmpty(), "The in-range segment must contribute rows, else the test is vacuous");
    assertNotNull(streamed._schema, "A merge that produced rows must carry a schema");
  }

  /// The operator passes a null merger to the base class and replaces the base streaming loop wholesale. The two base
  /// entry points that would dereference that merger must fail loud, so a future change routing back through them
  /// breaks at the seam instead of with an NPE at query time.
  @Test
  public void testBaseWorkerEntryPointsFailLoud() {
    Operator<?> combineOperator = planCombineOperator(_sortedSegments,
        hintedContext("SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50"), true);
    assertTrue(combineOperator instanceof StreamingSelectionOrderByCombineOperator,
        "Expected the streaming combine operator, got: " + combineOperator.getClass().getSimpleName());
    StreamingSelectionOrderByCombineOperator streamingCombine =
        (StreamingSelectionOrderByCombineOperator) combineOperator;
    expectThrows(IllegalStateException.class, () -> streamingCombine.isQuerySatisfied(null, null));
    expectThrows(IllegalStateException.class, streamingCombine::processSegments);
  }

  @Test
  public void testHintOffSelectsMinMaxOperator() {
    // Default behavior is unchanged when the hint is off: the classic MinMax operator is still selected.
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50");
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Operator<?> combineOperator = planCombineOperator(_sortedSegments, queryContext, true);
    assertTrue(combineOperator instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "Hint off must keep the default MinMax combine operator, got: "
            + combineOperator.getClass().getSimpleName());
  }

  @Test
  public void testHintOnBlockingPathKeepsMinMaxOperator() {
    // The merge can only stream where there is a ResultsBlockStreamer. With none, the hint must not select it: the
    // blocking caller takes a single nextBlock(), so the merge could never flush more than one block.
    Operator<?> combineOperator = planCombineOperator(_sortedSegments,
        hintedContext("SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50"), false);
    assertTrue(combineOperator instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "Hint on the blocking path must keep the MinMax combine operator, got: "
            + combineOperator.getClass().getSimpleName());
  }

  @Test
  public void testBlockingInstancePlanDisablesTheHint() {
    // The leaf gate reads the same flag as the combine gate but cannot see the streamer, so the blocking instance
    // plan clears the flag outright rather than leaving the option half-honored: streaming leaves swapped in under
    // a blocking MinMax combine that cannot drive them.
    QueryContext queryContext =
        hintedContext("SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50");
    List<SegmentContext> segmentContexts = new ArrayList<>(_sortedSegments.size());
    for (IndexSegment segment : _sortedSegments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    PLAN_MAKER.makeInstancePlan(segmentContexts, queryContext, EXECUTOR);
    assertFalse(queryContext.isSortedSelectionMergeEnabled(),
        "The blocking instance plan must disable the streaming selection merge for the whole plan");
    // The observable consequence: leaves planned for this query are the materializing operators, not the multi-block
    // streaming one that only the streaming combine knows how to drain.
    Operator<?> leafOperator = PLAN_MAKER.makeSegmentPlanNode(segmentContexts.get(0), queryContext).run();
    assertFalse(leafOperator instanceof StreamingSelectionOrderByOperator,
        "The blocking path must not plan a multi-block streaming leaf, got: "
            + leafOperator.getClass().getSimpleName());
  }

  @Test
  public void testNonIdentifierOrderByFallsBackWithTheModeOff() {
    // Default behaviour for a non-identifier first order-by is unchanged: SelectionOrderByCombineOperator. Only an
    // explicit ON reaches the streaming merge for this shape (see testOnForcesStreamingForAnExpressionOrderBy);
    // AUTO refuses it (see testAutoDoesNotSelectStreamingForAnExpressionOrderBy).
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY ADD(sortedCol, 1), valCol LIMIT 50");
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Operator<?> combineOperator = planCombineOperator(_sortedSegments, queryContext, true);
    assertEquals(combineOperator.getClass(), SelectionOrderByCombineOperator.class,
        "Non-identifier first order-by must fall back to SelectionOrderByCombineOperator, got: "
            + combineOperator.getClass().getSimpleName());
  }

  /// Asserts streaming-vs-blocking parity for {@code query}. Runs the MinMax combine on the blocking path (hint off)
  /// as the reference, then runs the streaming combine on the streaming path, asserting it selects the streaming
  /// operator and produces rows that are sorted by the order-by comparator and equal the MinMax rows as a multiset,
  /// plus the bounded-flush invariants. A small block size forces several bounded data blocks before the metadata
  /// block, genuinely exercising the streaming flush path rather than a single trimmed block.
  private void assertParity(List<IndexSegment> segments, @Language("sql") String query, boolean nullHandling) {
    Result baseline = run(segments, query, false, nullHandling, false, 0);
    assertEquals(baseline._combineOperator.getClass(), MinMaxValueBasedSelectionOrderByCombineOperator.class,
        "Baseline must be the MinMax combine operator, got: " + baseline._combineOperator.getClass().getSimpleName());
    Comparator<Object[]> comparator = orderByComparator(query, nullHandling);

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

  /// Plans the combine operator that would run {@code queryContext} over {@code segments}, without driving it. Use
  /// this for gate assertions: an operator that is not the streaming combine (the fallbacks) does not terminate with
  /// a metadata block, so it cannot be driven by the streaming loop in [#run].
  private static Operator<?> planCombineOperator(List<IndexSegment> segments, QueryContext queryContext,
      boolean streaming) {
    List<PlanNode> planNodes = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      SegmentContext segmentContext = new SegmentContext(segment);
      planNodes.add(streaming ? PLAN_MAKER.makeStreamingSegmentPlanNode(segmentContext, queryContext)
          : PLAN_MAKER.makeSegmentPlanNode(segmentContext, queryContext));
    }
    ResultsBlockStreamer streamer = streaming ? block -> {
    } : null;
    return new CombinePlanNode(planNodes, queryContext, EXECUTOR, streamer).run();
  }

  @Test
  public void testAutoSelectsStreamingWhenAllSegmentsAreSorted() {
    // AUTO is resolved from segment metadata by the streaming instance plan, before any plan node is built.
    QueryContext queryContext = modeContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "AUTO must resolve to ON when every segment is sorted on the leading order-by column");
    assertTrue(planCombineOperator(_sortedSegments, queryContext, true)
            instanceof StreamingSelectionOrderByCombineOperator,
        "The resolved mode must select the streaming combine");
  }

  @Test
  public void testAutoSelectsMinMaxWhenNoSegmentIsSorted() {
    // Every child would be a materializing SelectionOrderByOperator doing a full scan plus top-K synchronously on the
    // consumer thread, one segment after another -- strictly worse than the parallel MinMax combine.
    QueryContext queryContext = modeContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_unsortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "AUTO must resolve to OFF when no segment is sorted on the leading order-by column");
    assertTrue(planCombineOperator(_unsortedSegments, queryContext, true)
            instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "The resolved mode must keep the MinMax combine");
  }

  @Test
  public void testAutoHonoursTheMinSortedRatioThreshold() {
    // _mixedSegments is 2 sorted of 4, so a ratio of exactly 0.5 must pass and anything above it must fail. This pins
    // the comparison as >= rather than >, and pins that the threshold is read from the query context.
    String query = "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50";

    QueryContext atThreshold = modeContext(query, SortedSelectionMergeMode.AUTO);
    atThreshold.setSortedSelectionMergeAutoMinSortedRatio(0.5);
    makeStreamingInstancePlan(_mixedSegments, atThreshold);
    assertEquals(atThreshold.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "A sorted ratio equal to the threshold must select the streaming merge");

    QueryContext aboveThreshold = modeContext(query, SortedSelectionMergeMode.AUTO);
    aboveThreshold.setSortedSelectionMergeAutoMinSortedRatio(0.75);
    makeStreamingInstancePlan(_mixedSegments, aboveThreshold);
    assertEquals(aboveThreshold.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "A sorted ratio below the threshold must keep the MinMax combine");
  }

  @Test
  public void testAutoIgnoresNullsInTheLeadingColumn() {
    // Pins the known gap documented by the TODO in InstancePlanMakerImplV2#isSortedEnoughForStreamingMerge, so it is
    // enforced by CI rather than only described. AUTO resolves from physical sortedness alone, because the null check
    // that SelectionPlanNode#isColumnSorted adds reads the segment's mapped buffer and AUTO runs before any acquire.
    // So a null-bearing sorted column resolves to ON with null handling either off or on -- but with it on, the leaf
    // gate then refuses to stream, and the merge runs over fully materialized children. Correct, just not fast.
    //
    // Tighten this to OFF for the null-handling case when the follow-up lands.
    // See https://github.com/apache/pinot/pull/19120#discussion_r3871713975
    String query = "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50";

    QueryContext nullHandlingOff = modeContext(query, SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_nullBearingSortedSegments, nullHandlingOff);
    assertEquals(nullHandlingOff.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "With null handling off the segments are sorted, so AUTO must select the streaming merge");

    QueryContext nullHandlingOn = modeContext(query, SortedSelectionMergeMode.AUTO);
    nullHandlingOn.setNullHandlingEnabled(true);
    makeStreamingInstancePlan(_nullBearingSortedSegments, nullHandlingOn);
    assertEquals(nullHandlingOn.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "AUTO currently reads physical sortedness only, so null handling does not change the decision");
    // The leaf, which runs post-acquire and can afford the null check, still refuses to stream these segments.
    List<SegmentContext> segmentContexts = new ArrayList<>(1);
    segmentContexts.add(new SegmentContext(_nullBearingSortedSegments.get(0)));
    Operator<?> leafOperator = PLAN_MAKER.makeSegmentPlanNode(segmentContexts.get(0), nullHandlingOn).run();
    assertFalse(leafOperator instanceof StreamingSelectionOrderByOperator,
        "A null-bearing leading column must not produce a streaming leaf under null handling, got: "
            + leafOperator.getClass().getSimpleName());
  }

  @Test
  public void testAutoSelectsMinMaxWhenThereAreNoSegments() {
    QueryContext queryContext = modeContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(List.of(), queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "An empty segment set must resolve to OFF rather than dividing by zero");
  }

  @Test
  public void testUnresolvedAutoFallsBackToOff() {
    // AUTO is a request for a decision, not a decision, but a gate reached before the plan maker resolved it must
    // not fail the query either. isSortedSelectionMergeEnabled() degrades an unresolved AUTO to OFF (with a
    // warning) rather than forcing the merge unconditionally and silently ignoring the ratio, so the gate should
    // select the same fallback operator it would for OFF.
    QueryContext queryContext = modeContext(
        "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50", SortedSelectionMergeMode.AUTO);
    Operator<?> combineOperator = planCombineOperator(_sortedSegments, queryContext, true);
    assertTrue(combineOperator instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "An unresolved AUTO must degrade to OFF, got: " + combineOperator.getClass().getSimpleName());
  }

  @Test
  public void testAutoDoesNotSelectStreamingForAnExpressionOrderBy() {
    // Sortedness is only defined for a physical column; forcing the merge over an expression is what ON is for.
    QueryContext queryContext =
        modeContext("SELECT sortedCol, valCol FROM testTable ORDER BY ADD(sortedCol, 1) LIMIT 50",
            SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "AUTO must not select the streaming merge for a non-identifier leading order-by expression");
  }

  @Test
  public void testAutoDoesNotSelectStreamingForDescWithoutReverseOrder() {
    // DataSourceMetadata.isSorted() reports ascending physical order, so with allowReverseOrder off no leaf can scan
    // sortedCol descending: every segment falls back to the materialized SelectionPartiallyOrderedByDescOperation.
    // Resolving to ON here would install the streaming combine over children that each materialize their top-K
    // serially, losing the MinMax combine's parallelism and its min/max segment pruning for no gain.
    QueryContext queryContext =
        modeContext("SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, valCol DESC LIMIT 50",
            SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "AUTO must not select the streaming merge for a DESC order-by that no leaf can stream");
    assertTrue(planCombineOperator(_sortedSegments, queryContext, true)
            instanceof MinMaxValueBasedSelectionOrderByCombineOperator,
        "The resolved mode must keep the MinMax combine");
  }

  @Test
  public void testAutoSelectsStreamingForDescWithReverseOrder() {
    // With allowReverseOrder on, getSortedByProject() hands the leaf a DESC-compatible project operator, so the
    // streaming children this merge needs do get built and AUTO should take the path.
    QueryContext queryContext = modeContext(
        "SET allowReverseOrder=true; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, valCol DESC "
            + "LIMIT 50", SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "AUTO must select the streaming merge for DESC once reverse iteration is allowed");
    assertTrue(planCombineOperator(_sortedSegments, queryContext, true)
            instanceof StreamingSelectionOrderByCombineOperator,
        "The resolved mode must select the streaming combine");
  }

  @Test
  public void testAutoHonoursTheMinSortedRatioThresholdForDesc() {
    // The DESC gate is a precondition, not a replacement for the ratio check: once reverse iteration is allowed, a
    // DESC query must still clear the same threshold an ASC one does. _mixedSegments is 2 sorted of 4, so 0.5 passes
    // and 0.75 does not, exactly as in testAutoHonoursTheMinSortedRatioThreshold.
    String query = "SET allowReverseOrder=true; SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol DESC, "
        + "valCol DESC LIMIT 50";

    QueryContext atThreshold = modeContext(query, SortedSelectionMergeMode.AUTO);
    atThreshold.setSortedSelectionMergeAutoMinSortedRatio(0.5);
    makeStreamingInstancePlan(_mixedSegments, atThreshold);
    assertEquals(atThreshold.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "A DESC sorted ratio equal to the threshold must select the streaming merge");

    QueryContext aboveThreshold = modeContext(query, SortedSelectionMergeMode.AUTO);
    aboveThreshold.setSortedSelectionMergeAutoMinSortedRatio(0.75);
    makeStreamingInstancePlan(_mixedSegments, aboveThreshold);
    assertEquals(aboveThreshold.getSortedSelectionMergeMode(), SortedSelectionMergeMode.OFF,
        "A DESC sorted ratio below the threshold must keep the MinMax combine");
  }

  @Test
  public void testAutoOnlyChecksTheLeadingOrderByDirection() {
    // Only the leading expression rides the segment's physical order, so a DESC tail is sorted in memory and needs no
    // reverse scan. Pins the gate as "leading expression is DESC", not "any expression is DESC".
    QueryContext queryContext =
        modeContext("SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol DESC LIMIT 50",
            SortedSelectionMergeMode.AUTO);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "An ASC leading expression must select the streaming merge regardless of the tail's direction");
    assertTrue(planCombineOperator(_sortedSegments, queryContext, true)
            instanceof StreamingSelectionOrderByCombineOperator,
        "The resolved mode must select the streaming combine");
  }

  @Test
  public void testOnForcesStreamingForAnExpressionOrderBy() {
    // ON takes the path unconditionally, expressions included, so it can be benchmarked against the default. There is
    // no leading physical column, so frontier pruning is inert and every cursor is activated in plan order.
    QueryContext queryContext =
        modeContext("SELECT sortedCol, valCol FROM testTable ORDER BY ADD(sortedCol, 1) LIMIT 50",
            SortedSelectionMergeMode.ON);
    makeStreamingInstancePlan(_sortedSegments, queryContext);
    assertEquals(queryContext.getSortedSelectionMergeMode(), SortedSelectionMergeMode.ON,
        "ON must pass through the AUTO resolution untouched");
    assertTrue(planCombineOperator(_sortedSegments, queryContext, true)
            instanceof StreamingSelectionOrderByCombineOperator,
        "ON must select the streaming combine even for an expression order-by");
  }

  @Test
  public void testOnOverUnsortedSegmentsMatchesTheBaseline() {
    // Forced over unsorted segments the merge runs on materialized SelectionOrderByOperator children. Slower, but it
    // must still produce exactly the baseline result -- that is what makes it usable for A/B.
    assertParity(_unsortedSegments, "SELECT sortedCol, valCol FROM testTable ORDER BY sortedCol, valCol LIMIT 50",
        false);
  }

  /// Runs the streaming instance plan for its side effect on {@code queryContext}: resolving AUTO from segment
  /// metadata. The plan itself is discarded; the gate tests assert on the resolved mode and on the operator that
  /// [#planCombineOperator] then selects.
  private static void makeStreamingInstancePlan(List<IndexSegment> segments, QueryContext queryContext) {
    List<SegmentContext> segmentContexts = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    PLAN_MAKER.makeStreamingInstancePlan(segmentContexts, queryContext, EXECUTOR, block -> {
    });
  }

  /// Builds a query context for {@code query} with the streaming merge mode set to {@code mode}.
  private static QueryContext modeContext(@Language("sql") String query, SortedSelectionMergeMode mode) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSortedSelectionMergeMode(mode);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    return queryContext;
  }

  /// Builds a query context for {@code query} with the streaming merge hint on.
  private static QueryContext hintedContext(@Language("sql") String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    return queryContext;
  }

  /// Runs one combine over {@code segments} and collects its rows, blocks, schema and docs-scanned stat.
  private Result run(List<IndexSegment> segments, @Language("sql") String query, boolean hintOn, boolean nullHandling,
      boolean streaming, int blockSize) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setNullHandlingEnabled(nullHandling);
    if (hintOn) {
      queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
      if (blockSize > 0) {
        queryContext.setSortedSelectionMergeBlockSize(blockSize);
      }
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    Result result = new Result();
    Operator<?> combineOperator = planCombineOperator(segments, queryContext, streaming);
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

  /// Asserts the two row lists contain the same rows, independent of the ordering of fully-equal projected rows.
  private static void assertMultisetEquals(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(toCanonical(actual), toCanonical(expected), "Row multisets differ");
  }

  /// Canonicalizes rows for multiset comparison. Each cell is encoded with its runtime class so a stored-type / boxing
  /// regression (e.g. a LONG emitted where the reference emits INT) changes the encoding and fails the assertion, which
  /// a plain {@code Arrays.toString} (type-blind) comparison would miss.
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
    for (List<IndexSegment> segments : List.of(_sortedSegments, _disjointSegments, _mixedSegments, _lowCardSegments,
        _interleavedSegments, _tiedSegments)) {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /// Captured output of a single combine run.
  private static class Result {
    private Operator<?> _combineOperator;
    private DataSchema _schema;
    private List<Object[]> _rows;
    private List<Integer> _blockSizes;
    private int _numBlocks;
    private long _numDocsScanned;
  }
}
