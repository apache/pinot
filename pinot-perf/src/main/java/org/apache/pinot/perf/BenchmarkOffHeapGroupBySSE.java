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
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
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


/// End-to-end SSE group-by benchmark comparing on-heap vs off-heap group-by state (the `groupByOffHeap` query
/// option) over the generator tiers the off-heap feature covers:
/// <ul>
///   <li>`DICT_INT`: single dict-encoded high-cardinality INT — DictionaryBased IntMap tier</li>
///   <li>`DICT_TWO_COLS`: two dict-encoded columns whose cardinality product exceeds Integer.MAX_VALUE —
///   DictionaryBased LongMap tier</li>
///   <li>`RAW_INT`: single raw INT — NoDictionarySingleColumn long-key tier</li>
///   <li>`RAW_STRING`: single raw STRING — NoDictionarySingleColumn bytes tier</li>
///   <li>`RAW_MULTI`: raw INT + raw STRING — NoDictionaryMultiColumn packed-bytes tier</li>
/// </ul>
/// 4 segments x 300K rows, ~80K/30K distinct groups (below the default numGroupsLimit, so no capping). LIMIT 10
/// without ORDER BY keeps the (mode-independent) combine/reduce phases cheap relative to the per-segment phase this
/// feature changes. Run with `-prof gc`: the interesting metrics are score (ms/op) and gc.alloc.rate.norm.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkOffHeapGroupBySSE {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkOffHeapGroupBySSE");
  private static final String TABLE_NAME = "MyTable";
  private static final int NUM_SEGMENTS = 4;
  private static final int NUM_ROWS_PER_SEGMENT = 300_000;
  private static final int INT_HIGH_CARDINALITY = 80_000;
  private static final int INT_MED_CARDINALITY = 30_000;
  private static final int RAW_INT_CARDINALITY = 80_000;
  private static final int RAW_STRING_CARDINALITY = 30_000;

  private static final String INT_HIGH = "INT_HIGH";
  private static final String INT_MED = "INT_MED";
  private static final String RAW_INT = "RAW_INT";
  private static final String RAW_STRING = "RAW_STRING";
  private static final String METRIC = "METRIC";

  private static final Map<String, String> QUERIES = Map.of(
      "DICT_INT", "SELECT INT_HIGH, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY INT_HIGH LIMIT 10",
      "DICT_TWO_COLS",
      "SELECT INT_HIGH, INT_MED, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY INT_HIGH, INT_MED LIMIT 10",
      "RAW_INT", "SELECT RAW_INT, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY RAW_INT LIMIT 10",
      "RAW_STRING", "SELECT RAW_STRING, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY RAW_STRING LIMIT 10",
      "RAW_MULTI",
      "SELECT RAW_INT, RAW_STRING, COUNT(*), SUM(METRIC) FROM MyTable GROUP BY RAW_INT, RAW_STRING LIMIT 10");

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(RAW_INT, RAW_STRING))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(INT_HIGH, FieldSpec.DataType.INT)
      .addSingleValueDimension(INT_MED, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_INT, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING, FieldSpec.DataType.STRING)
      .addMetric(METRIC, FieldSpec.DataType.LONG)
      .build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final BrokerMetrics BROKER_METRICS = Mockito.mock(BrokerMetrics.class);

  @Param({"DICT_INT", "DICT_TWO_COLS", "RAW_INT", "RAW_STRING", "RAW_MULTI"})
  private String _scenario;
  @Param({"false", "true"})
  private String _groupByOffHeap;

  private ExecutorService _executorService;
  private List<IndexSegment> _indexSegments;
  private String _query;

  @Setup
  public void setUp()
      throws Exception {
    // Recommended production configuration for off-heap group-by: pool buffers per thread, mirroring the
    // on-heap thread-local map caching (bench measures steady-state reuse in both modes)
    OffHeapGroupByBufferPool.setMaxBytesPerThread(64L << 20);
    _executorService = Executors.newFixedThreadPool(8);
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
        row.putValue(INT_HIGH, random.nextInt(INT_HIGH_CARDINALITY));
        row.putValue(INT_MED, random.nextInt(INT_MED_CARDINALITY));
        row.putValue(RAW_INT, random.nextInt(RAW_INT_CARDINALITY));
        row.putValue(RAW_STRING, String.format("key-%08d-abcdefgh", random.nextInt(RAW_STRING_CARDINALITY)));
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

  @Benchmark
  public BrokerResponseNative query()
      throws TimeoutException {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(_query);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("groupByOffHeap", _groupByOffHeap);
    pinotQuery.setQueryOptions(queryOptions);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setGroupByOffHeap(Boolean.parseBoolean(_groupByOffHeap));
    queryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    // Server side
    List<SegmentContext> segmentContexts = new ArrayList<>(_indexSegments.size());
    _indexSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    Plan plan = PLAN_MAKER.makeInstancePlan(segmentContexts, queryContext, _executorService);
    InstanceResponseBlock instanceResponse = plan.execute();

    // Broker side
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      byte[] serializedResponse = instanceResponse.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    BrokerReduceService brokerReduceService = new BrokerReduceService(
        new PinotConfiguration(Map.of(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerResponseNative brokerResponse = brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest,
        dataTableMap, CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, BROKER_METRICS);
    brokerReduceService.shutDown();
    return brokerResponse;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(
        new OptionsBuilder().include(BenchmarkOffHeapGroupBySSE.class.getSimpleName()).addProfiler("gc").build())
        .run();
  }
}
