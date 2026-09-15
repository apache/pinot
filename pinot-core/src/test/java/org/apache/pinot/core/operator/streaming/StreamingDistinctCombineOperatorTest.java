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
package org.apache.pinot.core.operator.streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock.EarlyTerminationReason;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.DistinctCombineOperator;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Test for [StreamingDistinctCombineOperator].
public class StreamingDistinctCombineOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StreamingDistinctCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  private static final int NUM_SEGMENTS = 4;
  // Every segment holds the same 50 distinct values, each appearing twice. Identical content across segments is
  // deliberate: it guarantees that a value flushed in one window is re-emitted in a later one, which is exactly the
  // idempotence property the downstream FINAL stage relies on.
  private static final int NUM_DISTINCT_VALUES = 50;
  private static final int NUM_DOCS_PER_SEGMENT = NUM_DISTINCT_VALUES * 2;
  private static final long TOTAL_NUM_DOCS = (long) NUM_SEGMENTS * NUM_DOCS_PER_SEGMENT;

  private static final String INT_COLUMN = "intColumn";
  private static final String DICT_STRING_COLUMN = "dictStringColumn";
  private static final String RAW_STRING_COLUMN = "rawStringColumn";

  // The MSE leaf stage pushes no LIMIT down, so the leaf query runs with Integer.MAX_VALUE. That is what makes
  // DistinctTable.hasLimit() false and sends every executor down the unbounded add path, so model it here.
  private static final String NO_LIMIT = " LIMIT " + Integer.MAX_VALUE;
  // A filter (any filter) keeps DistinctPlanNode off the DictionaryBasedDistinctOperator fast path, which reads the
  // whole dictionary instead of scanning.
  private static final String MATCH_ALL = " WHERE intColumn >= 0";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setNoDictionaryColumns(List.of(RAW_STRING_COLUMN))
          // Only used by testIndexBasedDistinctOperatorPath; inert unless useIndexBasedDistinctOperator is set.
          .setInvertedIndexColumns(List.of(DICT_STRING_COLUMN)).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(DICT_STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(RAW_STRING_COLUMN, FieldSpec.DataType.STRING)
      .build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createOfflineSegment(i));
    }
  }

  /// Builds the shared fixture: every segment holds the SAME 50 values. See [#NUM_DISTINCT_VALUES] for why.
  private IndexSegment createOfflineSegment(int index)
      throws Exception {
    List<GenericRow> records = new ArrayList<>(NUM_DOCS_PER_SEGMENT);
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      for (int j = 0; j < 2; j++) {
        GenericRow record = new GenericRow();
        record.putValue(INT_COLUMN, i);
        record.putValue(DICT_STRING_COLUMN, "d" + i);
        record.putValue(RAW_STRING_COLUMN, "r" + i);
        records.add(record);
      }
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    String segmentName = SEGMENT_NAME_PREFIX + index;
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
  }

  /// With identical segments the first adopted block already exceeds any useful threshold, so it is flushed with
  /// nothing merged into it and the accumulate-across-segments path is never reached. This fixture gives each
  /// segment a disjoint range so the accumulator has to merge before it trips the threshold.
  @Test
  public void testAccumulatorMergesAcrossSegmentsBeforeFlushing()
      throws Exception {
    File disjointDir = new File(FileUtils.getTempDirectory(), "StreamingDistinctCombineOperatorTest_disjoint");
    FileUtils.deleteDirectory(disjointDir);
    List<IndexSegment> segments = new ArrayList<>(NUM_SEGMENTS);
    try {
      for (int index = 0; index < NUM_SEGMENTS; index++) {
        List<GenericRow> records = new ArrayList<>(NUM_DISTINCT_VALUES);
        for (int i = index * NUM_DISTINCT_VALUES; i < (index + 1) * NUM_DISTINCT_VALUES; i++) {
          GenericRow record = new GenericRow();
          record.putValue(INT_COLUMN, i);
          record.putValue(DICT_STRING_COLUMN, "d" + i);
          record.putValue(RAW_STRING_COLUMN, "r" + i);
          records.add(record);
        }
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
        segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
        String segmentName = "disjointSegment_" + index;
        segmentGeneratorConfig.setSegmentName(segmentName);
        segmentGeneratorConfig.setOutDir(disjointDir.getPath());
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
        driver.build();
        segments.add(ImmutableSegmentLoader.load(new File(disjointDir, segmentName), ReadMode.mmap));
      }

      // Threshold above one segment's cardinality (50), so at least two segments must merge before a flush.
      QueryContext queryContext =
          newQueryContext("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT);
      queryContext.setMaxExecutionThreads(1);
      List<Operator> operators = new ArrayList<>(segments.size());
      for (IndexSegment segment : segments) {
        operators.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(segment), queryContext).run());
      }
      StreamingDistinctCombineOperator combineOperator =
          new StreamingDistinctCombineOperator(operators, queryContext, EXECUTOR, 75);

      Set<Object> values = new HashSet<>();
      int numBlocks = 0;
      combineOperator.start();
      try {
        BaseResultsBlock block = combineOperator.nextBlock();
        while (!(block instanceof MetadataResultsBlock)) {
          assertNull(block.getErrorMessages(), "Expected no errors but got: " + block.getErrorMessages());
          numBlocks++;
          for (Object[] row : block.getRows()) {
            values.add(row[0]);
          }
          block = combineOperator.nextBlock();
        }
      } finally {
        combineOperator.stop();
      }

      // 4 disjoint segments of 50 against a threshold of 75 means each flush covers two segments.
      assertEquals(numBlocks, 2, "Expected each flush window to span two merged segments");
      Set<Object> expected = new HashSet<>();
      for (int i = 0; i < NUM_SEGMENTS * NUM_DISTINCT_VALUES; i++) {
        expected.add(i);
      }
      assertEquals(values, expected);
    } finally {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
      FileUtils.deleteDirectory(disjointDir);
    }
  }

  /// maxRowsInDistinct bounds scanned docs for the whole query. Each flush starts from an empty accumulator, so
  /// the scanned-doc count has to be carried across windows or the bound silently becomes per-window and never
  /// trips. Also pins that the reason reaches the terminal metadata block: it is recorded on the accumulated block,
  /// which by then has already been streamed out, so without the carry-over the broker would report a truncated
  /// result as complete.
  ///
  /// The threshold here is above one segment's cardinality (50) on purpose. Below it every block flushes on
  /// adoption, `mergeResultsBlocks` never runs, and no distinct guardrail can fire at all - see the class Javadoc
  /// on [StreamingDistinctCombineOperator].
  @Test
  public void testMaxRowsInDistinctIsPerQueryAcrossFlushWindows() {
    // 4 segments x 100 docs = 400 scanned; bound well below that.
    FlushResult bounded = runStreaming(
        "SET maxRowsInDistinct = 150; SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 75);
    assertEquals(bounded._earlyTerminationReason, EarlyTerminationReason.DISTINCT_MAX_ROWS,
        "maxRowsInDistinct must terminate the query across flush windows, not per window");

    // Same query without the bound returns the full set and reports no early termination.
    FlushResult unbounded = runStreaming("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 75);
    assertEquals(unbounded._earlyTerminationReason, EarlyTerminationReason.NONE);
    assertEquals(distinctValues(unbounded), expectedIntValues());
    // Asserted on the unbounded run only. The bounded one stops polling as soon as the merger reports the query
    // satisfied, so its terminal metadata block is built while the worker threads are still being cancelled and the
    // scanned-doc total is whatever they happened to reach. See testStreamingDistinctProducesMultipleBlocks for the
    // multi-flush-window version of this assertion.
    assertEquals(unbounded._numDocsScanned, TOTAL_NUM_DOCS);
  }

  /// The threshold has to reach the leaf QueryContext through the real query-option path, and must be left alone
  /// for non-distinct queries.
  @Test
  public void testFlushThresholdQueryOptionWiring() {
    assertEquals(planned("SET streamingDistinctFlushThreshold = 100; SELECT DISTINCT intColumn FROM testTable"), 100);
    assertEquals(planned("SET streamingDistinctFlushThreshold = 0; SELECT DISTINCT intColumn FROM testTable"), 0);
    assertEquals(planned("SELECT DISTINCT intColumn FROM testTable"), 0);
    // A group-by aggregation is not a distinct query, so the distinct threshold must stay unset.
    assertEquals(planned(
        "SET streamingDistinctFlushThreshold = 100; SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn"), 0);
  }

  /// Goes through the real streaming-plan entry point (which is what applies the query options on the server)
  /// rather than setting the QueryContext field directly.
  private int planned(String query) {
    QueryContext queryContext = newQueryContext(query);
    List<SegmentContext> segmentContexts = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      segmentContexts.add(new SegmentContext(indexSegment));
    }
    PLAN_MAKER.makeStreamingInstancePlan(segmentContexts, queryContext, EXECUTOR, block -> {
    });
    return queryContext.getStreamingDistinctFlushThreshold();
  }

  /// The routing guard in CombinePlanNode is the real gate, but the operator is public, so the constructor keeps a
  /// backstop. Asserted directly because a slipped negation here would leave the backstop silently inert while every
  /// routing test still passed.
  @Test
  public void testConstructorRejectsLeafReturningFinalResults() {
    for (boolean keyUnpartitioned : new boolean[]{false, true}) {
      QueryContext queryContext = newQueryContext("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT);
      queryContext.setServerReturnFinalResult(!keyUnpartitioned);
      queryContext.setServerReturnFinalResultKeyUnpartitioned(keyUnpartitioned);
      assertThrows(IllegalStateException.class,
          () -> new StreamingDistinctCombineOperator(List.of(), queryContext, EXECUTOR, 10));
    }
    // Control: the same construction succeeds with neither flag set, so the assertions above cannot be passing
    // because of some unrelated IllegalStateException.
    QueryContext queryContext = newQueryContext("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT);
    assertNotNull(new StreamingDistinctCombineOperator(List.of(), queryContext, EXECUTOR, 10));
  }

  @Test
  public void testStreamingDistinctProducesMultipleBlocks() {
    // 50 distinct values with a flush threshold of 10 forces several flushes
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    assertTrue(result._numBlocks > 1, "Expected multiple data blocks but got " + result._numBlocks);
    assertEquals(distinctValues(result), expectedIntValues());
    // Guards the "carrying the count across flush windows cannot double count" claim in mergeBlock(): each segment's
    // docs must be counted exactly once no matter how many windows they were spread over. Holds because
    // attachExecutionStats() recomputes the total from the segment operators rather than from the accumulated block,
    // which is precisely the coupling a later refactor could break quietly.
    assertEquals(result._numDocsScanned, TOTAL_NUM_DOCS);
  }

  /// Pins the correctness argument the design rests on: the leaf may emit the same value in more than one flush
  /// window, and de-duplicating the union still yields the exact answer.
  @Test
  public void testDuplicateValuesAcrossFlushWindowsAreIdempotent() {
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    assertTrue(result._rows.size() > NUM_DISTINCT_VALUES,
        "Expected duplicate values across flush windows, but only " + result._rows.size() + " rows were emitted");
    assertEquals(distinctValues(result), expectedIntValues());
  }

  @Test
  public void testHighThresholdProducesSingleBlock() {
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10_000);

    assertEquals(result._numBlocks, 1, "Expected a single data block when the threshold exceeds the cardinality");
    assertEquals(result._rows.size(), NUM_DISTINCT_VALUES);
    assertEquals(distinctValues(result), expectedIntValues());
  }

  /// Single raw (non-dictionary-encoded) column — StringDistinctExecutor / StringDistinctTable.
  @Test
  public void testRawStringColumn() {
    FlushResult result = runStreaming("SELECT DISTINCT rawStringColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    assertTrue(result._numBlocks > 1);
    Set<Object> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add("r" + i);
    }
    assertEquals(distinctValues(result), expected);
  }

  /// Single dictionary-encoded column reached through the scan path — DictionaryBasedSingleColumnDistinctExecutor.
  /// Its getResult() materializes actual values from the segment dictionary, which is what makes the flushed block
  /// self-contained (no detachFromWorkerThreadState hook needed). Segment-local dict ids leaking out here would show
  /// up as values outside 0..49.
  @Test
  public void testDictionaryEncodedColumnMaterializesValues() {
    FlushResult result = runStreaming("SELECT DISTINCT dictStringColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    Set<Object> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add("d" + i);
    }
    assertEquals(distinctValues(result), expected);
  }

  /// All columns dictionary-encoded — DictionaryBasedMultiColumnDistinctExecutor.
  @Test
  public void testMultiColumnAllDictionaryEncoded() {
    FlushResult result =
        runStreaming("SELECT DISTINCT intColumn, dictStringColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    assertTrue(result._numBlocks > 1);
    Set<List<Object>> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add(List.of(i, "d" + i));
    }
    assertEquals(distinctRows(result), expected);
  }

  /// At least one raw column — RawMultiColumnDistinctExecutor / MultiColumnDistinctTable.
  @Test
  public void testMultiColumnWithRawColumn() {
    FlushResult result =
        runStreaming("SELECT DISTINCT intColumn, rawStringColumn FROM testTable" + MATCH_ALL + NO_LIMIT, 10);

    assertTrue(result._numBlocks > 1);
    Set<List<Object>> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add(List.of(i, "r" + i));
    }
    assertEquals(distinctRows(result), expected);
  }

  /// DistinctPlanNode can also select the index-based operators when useIndexBasedDistinctOperator is set. They
  /// build their DistinctTable differently from the scan path, so make sure their blocks flow through the streaming
  /// combine correctly (in particular that real values, not segment-local dict ids, come out).
  @Test
  public void testIndexBasedDistinctOperatorPath() {
    FlushResult result = runStreaming(
        "SET useIndexBasedDistinctOperator = true; SELECT DISTINCT dictStringColumn FROM testTable" + MATCH_ALL
            + NO_LIMIT, 10);

    Set<Object> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add("d" + i);
    }
    assertEquals(distinctValues(result), expected);
  }

  /// Without a filter, DistinctPlanNode picks DictionaryBasedDistinctOperator, which reads the dictionary directly
  /// rather than scanning. The streaming combine must handle those blocks too.
  @Test
  public void testDictionaryBasedDistinctOperatorPath() {
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT, 10);

    assertEquals(distinctValues(result), expectedIntValues());
  }

  /// When no rows match, every segment yields an empty table and the operator must return only the metadata block —
  /// never an empty data block, and never an EmptyDistinctTable as the accumulator (it throws on mergeDistinctTable).
  @Test
  public void testNoMatchingRowsReturnsOnlyMetadata() {
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable WHERE intColumn < 0" + NO_LIMIT, 10);

    assertEquals(result._numBlocks, 0, "Expected no data blocks when nothing matches");
    assertTrue(result._rows.isEmpty());
  }

  /// A finite LIMIT below the flush threshold still bounds the table, and the streaming operator must not lose rows
  /// when the per-segment executors short-circuit on DistinctTable.isSatisfied().
  @Test
  public void testFiniteLimitBelowThreshold() {
    FlushResult result = runStreaming("SELECT DISTINCT intColumn FROM testTable" + MATCH_ALL + " LIMIT 20", 10_000);

    Set<Object> values = distinctValues(result);
    assertEquals(values.size(), 20, "Expected exactly LIMIT distinct values");
    assertTrue(expectedIntValues().containsAll(values), "Returned values must be a subset of the real distinct set");
  }

  /// CombinePlanNode must only pick the streaming operator when every guard holds.
  @Test
  public void testCombinePlanNodeSelection() {
    // Streaming applies: distinct, no ORDER BY, unbounded limit, not returning final results
    assertTrue(buildCombineOperator("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT, 10, false)
        instanceof StreamingDistinctCombineOperator, "Expected the streaming operator for an unbounded distinct");

    // Threshold not set (0) — feature off
    assertTrue(buildCombineOperator("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT, 0, false)
        instanceof DistinctCombineOperator, "Expected the blocking operator when the threshold is 0");

    // LIMIT <= threshold already bounds the table and gives the isSatisfied() short-circuit
    assertTrue(buildCombineOperator("SELECT DISTINCT intColumn FROM testTable LIMIT 10", 10, false)
        instanceof DistinctCombineOperator, "Expected the blocking operator when LIMIT bounds the table");

    // ORDER BY keeps a bounded top-LIMIT heap
    assertTrue(
        buildCombineOperator("SELECT DISTINCT intColumn FROM testTable ORDER BY intColumn LIMIT 1000", 10, false)
            instanceof DistinctCombineOperator, "Expected the blocking operator with ORDER BY");

    // No aggregate above the leaf guaranteed to de-duplicate across flush windows
    assertTrue(buildCombineOperator("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT, 10, true, false)
        instanceof DistinctCombineOperator, "Expected the blocking operator when the server returns final results");
    assertTrue(buildCombineOperator("SELECT DISTINCT intColumn FROM testTable" + NO_LIMIT, 10, false, true)
        instanceof DistinctCombineOperator,
        "Expected the blocking operator when the leaf returns final results for unpartitioned keys");

    // A group-by aggregation must keep its own operator even with the distinct threshold set
    assertFalse(
        buildCombineOperator("SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn" + NO_LIMIT, 10, false)
            instanceof StreamingDistinctCombineOperator, "The distinct threshold must not capture group-by queries");
  }

  /// A null is tracked by a flag on the DistinctTable rather than as a set entry, and each flush window starts from a
  /// fresh accumulator. The null must therefore survive the flush boundary: it is counted by size(), emitted by
  /// getRows(), and re-emitted in later windows (harmless, since the downstream de-duplication is idempotent).
  @Test
  public void testNullHandling()
      throws Exception {
    File nullDir = new File(FileUtils.getTempDirectory(), "StreamingDistinctCombineOperatorTest_null");
    FileUtils.deleteDirectory(nullDir);
    String nullableColumn = "nullableColumn";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true).build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(nullableColumn, FieldSpec.DataType.STRING)
        .build();

    List<IndexSegment> segments = new ArrayList<>(NUM_SEGMENTS);
    try {
      for (int index = 0; index < NUM_SEGMENTS; index++) {
        List<GenericRow> records = new ArrayList<>(NUM_DISTINCT_VALUES);
        for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
          GenericRow record = new GenericRow();
          record.putValue(INT_COLUMN, i);
          // Half the rows are null, so every segment contributes both real values and the null marker.
          if (i % 2 == 0) {
            record.putValue(nullableColumn, "n" + i);
          } else {
            record.putValue(nullableColumn, null);
          }
          records.add(record);
        }
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
        String segmentName = "nullSegment_" + index;
        segmentGeneratorConfig.setSegmentName(segmentName);
        segmentGeneratorConfig.setOutDir(nullDir.getPath());
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
        driver.build();
        segments.add(ImmutableSegmentLoader.load(new File(nullDir, segmentName), ReadMode.mmap));
      }

      QueryContext queryContext =
          newQueryContext("SELECT DISTINCT nullableColumn FROM testTable" + MATCH_ALL + NO_LIMIT);
      queryContext.setNullHandlingEnabled(true);
      List<Operator> operators = new ArrayList<>(segments.size());
      for (IndexSegment segment : segments) {
        operators.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(segment), queryContext).run());
      }
      StreamingDistinctCombineOperator combineOperator =
          new StreamingDistinctCombineOperator(operators, queryContext, EXECUTOR, 5);

      Set<Object> values = new HashSet<>();
      int numBlocks = 0;
      combineOperator.start();
      try {
        BaseResultsBlock block = combineOperator.nextBlock();
        while (!(block instanceof MetadataResultsBlock)) {
          assertNull(block.getErrorMessages(), "Expected no errors but got: " + block.getErrorMessages());
          numBlocks++;
          for (Object[] row : block.getRows()) {
            values.add(row[0]);
          }
          block = combineOperator.nextBlock();
        }
      } finally {
        combineOperator.stop();
      }

      assertTrue(numBlocks > 1, "Expected multiple flush windows but got " + numBlocks);
      Set<Object> expected = new HashSet<>();
      expected.add(null);
      for (int i = 0; i < NUM_DISTINCT_VALUES; i += 2) {
        expected.add("n" + i);
      }
      assertEquals(values, expected, "The null must survive the flush boundary");
    } finally {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
      FileUtils.deleteDirectory(nullDir);
    }
  }

  private BaseCombineOperator<?> buildCombineOperator(String query, int flushThreshold,
      boolean serverReturnFinalResult) {
    return buildCombineOperator(query, flushThreshold, serverReturnFinalResult, false);
  }

  private BaseCombineOperator<?> buildCombineOperator(String query, int flushThreshold,
      boolean serverReturnFinalResult, boolean serverReturnFinalResultKeyUnpartitioned) {
    QueryContext queryContext = newQueryContext(query);
    queryContext.setStreamingDistinctFlushThreshold(flushThreshold);
    queryContext.setServerReturnFinalResult(serverReturnFinalResult);
    queryContext.setServerReturnFinalResultKeyUnpartitioned(serverReturnFinalResultKeyUnpartitioned);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    ResultsBlockStreamer streamer = block -> {
    };
    return new CombinePlanNode(planNodes, queryContext, EXECUTOR, streamer).run();
  }

  private FlushResult runStreaming(String query, int flushThreshold) {
    QueryContext queryContext = newQueryContext(query);
    List<Operator> operators = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      operators.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext).run());
    }
    StreamingDistinctCombineOperator combineOperator =
        new StreamingDistinctCombineOperator(operators, queryContext, EXECUTOR, flushThreshold);

    List<Object[]> rows = new ArrayList<>();
    int numBlocks = 0;
    EarlyTerminationReason earlyTerminationReason;
    long numDocsScanned;
    combineOperator.start();
    try {
      BaseResultsBlock block = combineOperator.nextBlock();
      while (!(block instanceof MetadataResultsBlock)) {
        assertNull(block.getErrorMessages(), "Expected no errors but got: " + block.getErrorMessages());
        assertTrue(block instanceof DistinctResultsBlock,
            "Expected DistinctResultsBlock but got: " + block.getClass());
        numBlocks++;
        rows.addAll(block.getRows());
        block = combineOperator.nextBlock();
      }
      // The reason is set on the accumulated block, which has already been streamed out by then, so the terminal
      // metadata block is the only place the broker can still learn the results were truncated.
      earlyTerminationReason = block.getEarlyTerminationReason();
      numDocsScanned = block.getNumDocsScanned();
    } finally {
      combineOperator.stop();
    }
    return new FlushResult(numBlocks, rows, earlyTerminationReason, numDocsScanned);
  }

  private static QueryContext newQueryContext(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    return queryContext;
  }

  private static Set<Object> expectedIntValues() {
    Set<Object> expected = new HashSet<>();
    for (int i = 0; i < NUM_DISTINCT_VALUES; i++) {
      expected.add(i);
    }
    return expected;
  }

  private static Set<Object> distinctValues(FlushResult result) {
    Set<Object> values = new HashSet<>();
    for (Object[] row : result._rows) {
      assertEquals(row.length, 1);
      values.add(row[0]);
    }
    return values;
  }

  private static Set<List<Object>> distinctRows(FlushResult result) {
    Set<List<Object>> distinct = new HashSet<>();
    for (Object[] row : result._rows) {
      distinct.add(Arrays.asList(row));
    }
    return distinct;
  }

  private static class FlushResult {
    final int _numBlocks;
    final List<Object[]> _rows;
    final EarlyTerminationReason _earlyTerminationReason;
    final long _numDocsScanned;

    FlushResult(int numBlocks, List<Object[]> rows, EarlyTerminationReason earlyTerminationReason,
        long numDocsScanned) {
      _numBlocks = numBlocks;
      _rows = rows;
      _earlyTerminationReason = earlyTerminationReason;
      _numDocsScanned = numDocsScanned;
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
