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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
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
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
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
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.intellij.lang.annotations.Language;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// JMH benchmark for GROUP BY GROUPING SETS / ROLLUP / CUBE in the single-stage engine.
///
/// Exercises the full server -> broker flow (segment scan, [GroupingSetsGroupKeyGenerator], per-set
/// segment trim, combine, and broker reduce). The number of grouping columns and the query shape
/// (ROLLUP vs CUBE vs explicit GROUPING SETS) drive how many groups each input row expands into, which
/// is the primary cost of the feature relative to a plain GROUP BY.
///
/// Run with:
/// `mvn -pl pinot-perf exec:java -Dexec.mainClass=org.apache.pinot.perf.BenchmarkGroupingSetsQueriesSSE`
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 5)
@State(Scope.Benchmark)
public class BenchmarkGroupingSetsQueriesSSE {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkGroupingSetsQueriesSSE.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  // Per-trial executor (8 threads to test combine parallelism). Created in @Setup and shut down in @TearDown so
  // its lifecycle matches each JMH trial -- a static executor shut down after the first @Param combination would
  // reject task submissions in subsequent trials within the same JVM.
  private ExecutorService _executorService;

  @Param({"50", "200"})
  private int _numSegments;
  @Param({"15000"})
  private int _numRows;
  // Skew of the dimension values: EXP(0.001) => high cardinality, EXP(0.5) => low cardinality.
  @Param({"EXP(0.001)", "EXP(0.5)"})
  private String _scenario;
  @Param({"10000"})
  private int _limit;

  /// The grouping-set query shapes under test. Plain GROUP BY on the same keys is included as a baseline
  /// so the expansion overhead of grouping sets is directly visible.
  public static final String ROLLUP_2 =
      "SELECT D1, D2, COUNT(*), SUM(M1) FROM MyTable GROUP BY ROLLUP(D1, D2)";
  public static final String ROLLUP_3 =
      "SELECT D1, D2, D3, COUNT(*), SUM(M1) FROM MyTable GROUP BY ROLLUP(D1, D2, D3)";
  public static final String CUBE_3 =
      "SELECT D1, D2, D3, COUNT(*), SUM(M1) FROM MyTable GROUP BY CUBE(D1, D2, D3)";
  public static final String GROUPING_SETS_3 =
      "SELECT D1, D2, D3, COUNT(*), SUM(M1) FROM MyTable "
          + "GROUP BY GROUPING SETS ((D1), (D2), (D3), (D1, D2), ())";
  public static final String PLAIN_GROUP_BY_3 =
      "SELECT D1, D2, D3, COUNT(*), SUM(M1) FROM MyTable GROUP BY D1, D2, D3";

  @Param({ROLLUP_2, ROLLUP_3, CUBE_3, GROUPING_SETS_3, PLAIN_GROUP_BY_3})
  private String _query;

  /// `true` (default engine behavior) aggregates the base grouping once per segment and derives the grouping
  /// sets; `false` forces the legacy per-row expansion path. Lets the benchmark compare the two directly.
  @Param({"true", "false"})
  private boolean _baseAggregation;

  @Benchmark
  public BrokerResponseNative query() {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      String query = "SET groupingSetsBaseAggregation = " + _baseAggregation + "; " + _query + " LIMIT " + _limit;
      return getBrokerResponse(query);
    }
  }

  // ---
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final BrokerMetrics BROKER_METRICS = Mockito.mock(BrokerMetrics.class);

  private List<IndexSegment> _indexSegments;
  private Distribution.DataSupplier _supplier;
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "GroupingSetsBenchmark");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";

  private static final String D1 = "D1";
  private static final String D2 = "D2";
  private static final String D3 = "D3";
  private static final String M1 = "M1";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(D1, FieldSpec.DataType.INT)
      .addSingleValueDimension(D2, FieldSpec.DataType.STRING)
      .addSingleValueDimension(D3, FieldSpec.DataType.INT)
      .addMetric(M1, FieldSpec.DataType.LONG)
      .build();

  @Setup
  public void setUp()
      throws Exception {
    _executorService = Executors.newFixedThreadPool(8);
    _supplier = Distribution.createSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    _indexSegments = new ArrayList<>();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    for (int i = 0; i < _numSegments; i++) {
      buildSegment(String.format(SEGMENT_NAME_TEMPLATE, i));
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, String.format(SEGMENT_NAME_TEMPLATE, i)),
          indexLoadingConfig));
    }
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
    _executorService.shutdownNow();
  }

  private LazyDataGenerator createTestData(int numRows, Distribution.DataSupplier supplier) {
    // create data lazily to prevent OOM and speed up setup
    return new LazyDataGenerator() {
      private final Map<Integer, UUID> _strings = new HashMap<>();
      private final String[] _d2Values = IntStream.range(0, 50).mapToObj(i -> "value" + i).toArray(String[]::new);
      private Distribution.DataSupplier _localSupplier = supplier;

      @Override
      public int size() {
        return numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        row.putValue(D1, (int) _localSupplier.getAsLong());
        row.putValue(D2, _d2Values[(int) (_localSupplier.getAsLong() % _d2Values.length + _d2Values.length)
            % _d2Values.length]);
        row.putValue(D3, (int) _localSupplier.getAsLong() % 1000);
        row.putValue(M1, _localSupplier.getAsLong());
        return null;
      }

      @Override
      public void rewind() {
        _strings.clear();
        _localSupplier.reset();
      }
    };
  }

  private void buildSegment(String segmentName)
      throws Exception {
    LazyDataGenerator rows = createTestData(_numRows, _supplier);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    // save generator state so that other segments are not identical to this one
    _supplier.snapshot();
  }

  private List<SegmentContext> getSegmentContexts(List<IndexSegment> indexSegments) {
    List<SegmentContext> segmentContexts = new ArrayList<>(indexSegments.size());
    indexSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    return segmentContexts;
  }

  private BrokerResponseNative getBrokerResponse(@Language("sql") String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    QueryContext serverQueryContext =
        serverPinotQuery == pinotQuery ? queryContext : QueryContextConverterUtils.getQueryContext(serverPinotQuery);

    // Server side
    serverQueryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan =
        PLAN_MAKER.makeInstancePlan(getSegmentContexts(_indexSegments), serverQueryContext, _executorService);
    InstanceResponseBlock instanceResponse;
    try {
      instanceResponse = plan.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }

    // Broker side
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      byte[] serializedResponse = instanceResponse.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse));
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME),
          DataTableFactory.getDataTable(serializedResponse));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    BrokerRequest serverBrokerRequest =
        serverPinotQuery == pinotQuery ? brokerRequest : CalciteSqlCompiler.convertToBrokerRequest(serverPinotQuery);
    return reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap);
  }

  private BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap) {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(
            new PinotConfiguration(Map.of(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, BROKER_METRICS);
    brokerReduceService.shutDown();
    return brokerResponse;
  }
}
