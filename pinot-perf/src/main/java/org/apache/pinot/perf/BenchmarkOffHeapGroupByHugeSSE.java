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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapGroupByBufferPool;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Huge-group SSE benchmark: ~10M distinct groups in a single 12M-row segment (numGroupsLimit raised to 21M),
/// measuring the per-segment group-by phase only — the phase the `groupByOffHeap` feature changes. The
/// cross-segment combine is deliberately excluded: merging 10M groups into the (mode-independent, still on-heap)
/// IndexedTable dominates and GC-thrashes both arms identically; it is the Milestone-4 work item.
///
/// Run with a large fixed heap and explicit direct-memory ceiling, e.g.
/// `-jvmArgs '-Xms12g -Xmx12g -XX:MaxDirectMemorySize=8g'`, and `-prof gc`: score (ms/op),
/// gc.alloc.rate.norm and gc.time are the interesting metrics. Pair with [OffHeapGroupByMemoryFootprint]
/// (which goes to 100M groups) for retained-memory numbers.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 4, time = 10)
@State(Scope.Benchmark)
public class BenchmarkOffHeapGroupByHugeSSE {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkOffHeapGroupByHugeSSE");
  private static final String TABLE_NAME = "MyTable";
  private static final int NUM_ROWS = 12_000_000;
  private static final int CARDINALITY = 10_000_000;
  private static final int RAISED_NUM_GROUPS_LIMIT = 21_000_000;

  private static final String DICT_INT_HUGE = "DICT_INT_HUGE";
  private static final String RAW_STRING_HUGE = "RAW_STRING_HUGE";
  private static final String METRIC = "METRIC";

  private static final Map<String, String> QUERIES = Map.of(
      "DICT_INT", "SELECT DICT_INT_HUGE, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY DICT_INT_HUGE LIMIT 10",
      "RAW_STRING",
      "SELECT RAW_STRING_HUGE, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY RAW_STRING_HUGE LIMIT 10");

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(java.util.List.of(RAW_STRING_HUGE))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(DICT_INT_HUGE, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING_HUGE, FieldSpec.DataType.STRING)
      .addMetric(METRIC, FieldSpec.DataType.LONG)
      .build();

  @Param({"DICT_INT", "RAW_STRING"})
  private String _scenario;
  @Param({"false", "true"})
  private String _groupByOffHeap;

  private InstancePlanMakerImplV2 _planMaker;
  private IndexSegment _indexSegment;
  private String _query;

  @Setup
  public void setUp()
      throws Exception {
    OffHeapGroupByBufferPool.setMaxBytesPerThread(2L << 30);
    _planMaker = new InstancePlanMakerImplV2();
    _planMaker.init(new PinotConfiguration(Map.of(
        CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, RAISED_NUM_GROUPS_LIMIT,
        CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT, RAISED_NUM_GROUPS_LIMIT)));
    FileUtils.deleteQuietly(INDEX_DIR);
    buildSegment();
    _indexSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, "testSegment"), new IndexLoadingConfig(TABLE_CONFIG, SCHEMA));
    _query = QUERIES.get(_scenario);
  }

  @TearDown
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void buildSegment()
      throws Exception {
    Random random = new Random(42);
    LazyDataGenerator rows = new LazyDataGenerator() {
      @Override
      public int size() {
        return NUM_ROWS;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        int group = random.nextInt(CARDINALITY);
        row.putValue(DICT_INT_HUGE, group);
        row.putValue(RAW_STRING_HUGE, makeKey(group));
        row.putValue(METRIC, (long) random.nextInt(1000));
        return null;
      }

      @Override
      public void rewind() {
        random.setSeed(42);
      }
    };
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName("testSegment");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private static String makeKey(int i) {
    char[] chars = {'k', 'e', 'y', '-', '0', '0', '0', '0', '0', '0', '0', '0', '0', '-',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    int value = i;
    for (int position = 12; position >= 4 && value > 0; position--) {
      chars[position] = (char) ('0' + (value % 10));
      value /= 10;
    }
    return new String(chars);
  }

  /// Single-segment group-by over ~10M distinct groups; the result block's generator (owning the off-heap state)
  /// is closed after each invocation, mirroring the combine operator's contract.
  @Benchmark
  public GroupByResultsBlock segmentGroupBy() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(_query);
    pinotQuery.setQueryOptions(new HashMap<>());
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    queryContext.setNumGroupsLimit(RAISED_NUM_GROUPS_LIMIT);
    queryContext.setNumGroupsWarningLimit(RAISED_NUM_GROUPS_LIMIT);
    queryContext.setGroupByOffHeap(Boolean.parseBoolean(_groupByOffHeap));
    GroupByOperator groupByOperator =
        (GroupByOperator) _planMaker.makeSegmentPlanNode(new SegmentContext(_indexSegment), queryContext).run();
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    if (aggregationGroupByResult != null) {
      aggregationGroupByResult.closeGroupKeyGenerator();
    }
    return resultsBlock;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(
        new OptionsBuilder().include(BenchmarkOffHeapGroupByHugeSSE.class.getSimpleName()).addProfiler("gc")
            .build()).run();
  }
}
