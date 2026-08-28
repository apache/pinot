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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapGroupByBufferPool;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
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
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.Mockito;
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


/// Very-large-group SSE benchmark: ~1M distinct groups per segment (numGroupsLimit raised to 2.1M), 2 segments x
/// 2M rows, on-heap vs off-heap group-by state. Two measurements per configuration:
/// <ul>
///   <li>[#query()]: the full flow (plan, 8-thread combine, serialize, broker reduce). NOTE: the combine
///   phase merges ~1M groups into the (mode-independent, on-heap) IndexedTable in both arms, so it dilutes the
///   per-segment difference — kept for the honest end-to-end picture.</li>
///   <li>[#segmentGroupBy()]: a single segment's GroupByOperator only — isolates the phase the off-heap
///   feature changes. The block's group key generator is closed after each invocation, mirroring the combine
///   operator's contract.</li>
/// </ul>
/// Run with `-prof gc`: score (ms/op), gc.alloc.rate.norm and gc.count are the interesting metrics; pair
/// with [OffHeapGroupByMemoryFootprint] for retained-heap numbers.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 5)
@State(Scope.Benchmark)
public class BenchmarkOffHeapGroupByLargeSSE {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkOffHeapGroupByLargeSSE");
  private static final String TABLE_NAME = "MyTable";
  private static final int NUM_SEGMENTS = 2;
  private static final int NUM_ROWS_PER_SEGMENT = 2_000_000;
  private static final int CARDINALITY = 1_000_000;
  private static final int RAISED_NUM_GROUPS_LIMIT = 2_100_000;

  private static final String DICT_INT_LARGE = "DICT_INT_LARGE";
  private static final String RAW_STRING_LARGE = "RAW_STRING_LARGE";
  private static final String METRIC = "METRIC";

  private static final Map<String, String> QUERIES = Map.of(
      "DICT_INT", "SELECT DICT_INT_LARGE, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY DICT_INT_LARGE LIMIT 10",
      "RAW_STRING",
      "SELECT RAW_STRING_LARGE, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY RAW_STRING_LARGE LIMIT 10");

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(RAW_STRING_LARGE))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(DICT_INT_LARGE, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING_LARGE, FieldSpec.DataType.STRING)
      .addMetric(METRIC, FieldSpec.DataType.LONG)
      .build();

  private static final BrokerMetrics BROKER_METRICS = Mockito.mock(BrokerMetrics.class);

  @Param({"DICT_INT", "RAW_STRING"})
  private String _scenario;
  @Param({"false", "true"})
  private String _groupByOffHeap;

  private InstancePlanMakerImplV2 _planMaker;
  private ExecutorService _executorService;
  private BrokerReduceService _brokerReduceService;
  private List<IndexSegment> _indexSegments;
  private String _query;

  @Setup
  public void setUp()
      throws Exception {
    // Recommended production configuration for off-heap group-by: pool buffers per thread
    OffHeapGroupByBufferPool.setMaxBytesPerThread(256L << 20);
    // Raise the group limits so ~1M groups per segment are never capped (and never spam the warn log)
    _planMaker = new InstancePlanMakerImplV2();
    _planMaker.init(new PinotConfiguration(Map.of(
        CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, RAISED_NUM_GROUPS_LIMIT,
        CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT, RAISED_NUM_GROUPS_LIMIT)));
    _executorService = Executors.newFixedThreadPool(8);
    _brokerReduceService = new BrokerReduceService(
        new PinotConfiguration(Map.of(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    FileUtils.deleteQuietly(INDEX_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "testSegment" + i;
      buildSegment(segmentName, i);
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), indexLoadingConfig));
    }
    _query = QUERIES.get(_scenario);
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
    _executorService.shutdownNow();
    _brokerReduceService.shutDown();
  }

  private void buildSegment(String segmentName, int segmentIndex)
      throws Exception {
    Random random = new Random(42 + segmentIndex);
    LazyDataGenerator rows = new LazyDataGenerator() {
      @Override
      public int size() {
        return NUM_ROWS_PER_SEGMENT;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        int group = random.nextInt(CARDINALITY);
        row.putValue(DICT_INT_LARGE, group);
        row.putValue(RAW_STRING_LARGE, String.format("key-%08d-abcdefgh", group));
        row.putValue(METRIC, (long) random.nextInt(1000));
        return null;
      }

      @Override
      public void rewind() {
        random.setSeed(42 + segmentIndex);
      }
    };
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private QueryContext buildQueryContext() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(_query);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("groupByOffHeap", _groupByOffHeap);
    queryOptions.put("numGroupsLimit", String.valueOf(RAISED_NUM_GROUPS_LIMIT));
    pinotQuery.setQueryOptions(queryOptions);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    return queryContext;
  }

  /// Full end-to-end flow over both segments (combine + serialize + reduce are mode-independent).
  @Benchmark
  public BrokerResponseNative query()
      throws TimeoutException {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(_query);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("groupByOffHeap", _groupByOffHeap);
    queryOptions.put("numGroupsLimit", String.valueOf(RAISED_NUM_GROUPS_LIMIT));
    pinotQuery.setQueryOptions(queryOptions);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    List<SegmentContext> segmentContexts = new ArrayList<>(_indexSegments.size());
    _indexSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    Plan plan = _planMaker.makeInstancePlan(segmentContexts, queryContext, _executorService);
    InstanceResponseBlock instanceResponse = plan.execute();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      byte[] serializedResponse = instanceResponse.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    return _brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap,
        CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, BROKER_METRICS);
  }

  /// Single-segment group-by only: isolates the phase the off-heap feature changes. The result block's group key
  /// generator (owning the off-heap state) is closed after each invocation, mirroring the combine operator.
  @Benchmark
  public GroupByResultsBlock segmentGroupBy() {
    QueryContext queryContext = buildQueryContext();
    // Apply the group-by limits the instance plan would apply
    queryContext.setNumGroupsLimit(RAISED_NUM_GROUPS_LIMIT);
    queryContext.setNumGroupsWarningLimit(RAISED_NUM_GROUPS_LIMIT);
    queryContext.setGroupByOffHeap(Boolean.parseBoolean(_groupByOffHeap));
    GroupByOperator groupByOperator =
        (GroupByOperator) _planMaker.makeSegmentPlanNode(new SegmentContext(_indexSegments.get(0)), queryContext)
            .run();
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
        new OptionsBuilder().include(BenchmarkOffHeapGroupByLargeSSE.class.getSimpleName()).addProfiler("gc")
            .build()).run();
  }
}
