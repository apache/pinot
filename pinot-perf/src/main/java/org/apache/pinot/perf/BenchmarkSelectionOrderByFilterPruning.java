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
package org.apache.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.query.config.SegmentPrunerConfig;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * End-to-end benchmark for the min/max segment pruning of selection {@code ORDER BY <col> LIMIT n} queries with a
 * filter (issue apache/pinot#18685).
 * <p>The table is range-partitioned on a sorted, nullable {@code TS_COL}: segment {@code i} covers the contiguous range
 * {@code [i * numRows, (i + 1) * numRows)}. Each invocation runs the segment pruners (the default chain, which respects
 * {@link org.apache.pinot.core.query.pruner.SegmentPruner#isApplicableTo}) and then executes the surviving segments via
 * the single-stage engine.
 * <p>The comparison is self-contained (no need to diff against a base commit): the optimization is gated off when null
 * handling is active, so running the same filtered query <b>with</b> {@code enableNullHandling=true} reproduces the
 * pre-fix behavior (the filtered order-by is not limit-pruned and every matching segment is engaged), while running it
 * <b>without</b> null handling exercises the fix (only the top segment(s) survive). The {@code NO_FILTER} query is a
 * reference point that is limit-pruned regardless.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkSelectionOrderByFilterPruning extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "BenchmarkSelectionOrderByFilterPruning");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";
  private static final String TS_COL = "TS_COL";
  private static final String VAL_COL = "VAL_COL";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setSortedColumn(TS_COL)
      .build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(TS_COL, FieldSpec.DataType.LONG)
      .addSingleValueDimension(VAL_COL, FieldSpec.DataType.INT)
      .build();

  /** Which query shape to run; see {@link #buildQuery}. */
  public enum Mode {
    /** Filtered order-by, optimization eligible (limit-pruned to the segments the LIMIT needs). */
    FILTER,
    /** Same filtered query but with null handling on, which gates the optimization off (pre-fix behavior). */
    FILTER_NULL_HANDLING,
    /** No filter: limit-pruned regardless of the fix (reference point). */
    NO_FILTER
  }

  @Param({"50", "100", "200"})
  private int _numSegments;
  // Small segments so a larger LIMIT spans several of them.
  @Param({"20000"})
  private int _numRows;
  @Param({"10", "200000"})
  private int _limit;
  @Param({"FILTER", "FILTER_NULL_HANDLING", "NO_FILTER"})
  private Mode _mode;

  private String _query;
  private List<IndexSegment> _allSegments;
  private List<IndexSegment> _selectedSegments;
  private SegmentPrunerService _segmentPrunerService;

  private static String buildQuery(Mode mode, int limit) {
    String query = "SELECT * FROM MyTable "
        + (mode == Mode.NO_FILTER ? "" : "WHERE TS_COL > 0 ")
        + "ORDER BY TS_COL DESC LIMIT " + limit;
    return mode == Mode.FILTER_NULL_HANDLING ? "SET enableNullHandling=true; " + query : query;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkSelectionOrderByFilterPruning.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Setup
  public void setUp()
      throws Exception {
    _query = buildQuery(_mode, _limit);
    FileUtils.deleteQuietly(INDEX_DIR);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    _allSegments = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      String name = String.format(SEGMENT_NAME_TEMPLATE, i);
      buildSegment(name, i);
      _allSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, name), indexLoadingConfig));
    }
    _selectedSegments = _allSegments;
    _segmentPrunerService = new SegmentPrunerService(new SegmentPrunerConfig(new PinotConfiguration()));
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _allSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  private void buildSegment(String segmentName, int segmentIndex)
      throws Exception {
    long baseValue = (long) segmentIndex * _numRows;
    List<GenericRow> rows = new ArrayList<>(_numRows);
    for (int i = 0; i < _numRows; i++) {
      GenericRow row = new GenericRow();
      // Contiguous, non-overlapping, sorted range per segment.
      row.putValue(TS_COL, baseValue + i);
      row.putValue(VAL_COL, ThreadLocalRandom.current().nextInt());
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Benchmark
  public BrokerResponseNative query() {
    // Prune first (this is what the fix affects), then execute the surviving segments through the single-stage engine.
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(_query);
    queryContext.setSchema(SCHEMA);
    _selectedSegments = _segmentPrunerService.prune(_allSegments, queryContext);
    return getBrokerResponse(_query);
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _allSegments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _selectedSegments;
  }
}
