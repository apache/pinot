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
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.queries.StatisticalQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public class BenchmarkPartitionedGroupByQueriesSSE {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkPartitionedGroupByQueriesSSE.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String LOW_CARDINALITY_INT_COL_NAME = "LOW_CARDINALITY_INT_COL";
  private static final String RAW_LOW_CARDINALITY_INT_COL_NAME = "RAW_LOW_CARDINALITY_INT_COL";
  private static final String STRING_COL_NAME = "STRING_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL_NAME = "LOW_CARDINALITY_STRING_COL";
  private static final String RAW_LOW_CARDINALITY_STRING_COL_NAME = "RAW_LOW_CARDINALITY_STRING_COL";
  private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setFieldConfigList(FIELD_CONFIGS)
      .setNoDictionaryColumns(List.of(RAW_INT_COL_NAME, RAW_STRING_COL_NAME, RAW_LOW_CARDINALITY_INT_COL_NAME,
          RAW_LOW_CARDINALITY_STRING_COL_NAME))
      .setRangeIndexColumns(List.of(INT_COL_NAME, LOW_CARDINALITY_STRING_COL_NAME)).build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_LOW_CARDINALITY_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(LOW_CARDINALITY_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(RAW_LOW_CARDINALITY_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(LOW_CARDINALITY_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .build();

//  public static final String MULTI_GROUP_BY_ORDER_BY_WITH_RAW_QUERY =
//      "SELECT RAW_INT_COL,INT_COL,COUNT(*) FROM MyTable "
//          + "GROUP BY RAW_INT_COL,INT_COL";

  public static final String MULTI_GROUP_BY_WITH_RAW_QUERY_2 =
      "SELECT RAW_STRING_COL,STRING_COL,COUNT(*) "
          + "FROM MyTable GROUP BY RAW_STRING_COL,STRING_COL";

  public static final String MULTI_GROUP_BY_ORDER_BY_WITH_RAW_QUERY_2 =
      "SELECT RAW_STRING_COL,STRING_COL,COUNT(*) "
          + "FROM MyTable GROUP BY RAW_STRING_COL,STRING_COL ORDER BY COUNT(*)";

  // NOTE: currently only 200 seg, 10000 rows per seg, limit 100 works better
  // tried 0.0001 and 0.00001 scenario both works.
  // The problem is just reduce takes too long!!

//  @Param({"2", "50", "100"})
//  @Param({"50", "100", "200"})
  @Param({"50"})
//  @Param({"2", "200", "1000"})
//  @Param({"20"})
  private int _numSegments;
//  @Param({"15000"})
//@Param({"1000"})
  @Param({"10000"})
  private int _numRows;
//  @Param({"EXP(0.001)", "EXP(0.5)", "EXP(0.999)"})
  @Param({"EXP(0.0001)"})
  String _scenario;
  @Param({
//      MULTI_GROUP_BY_ORDER_BY_WITH_RAW_QUERY,
//      MULTI_GROUP_BY_WITH_RAW_QUERY_2,
      MULTI_GROUP_BY_ORDER_BY_WITH_RAW_QUERY_2
  })
  String _query;

//  @Param({"100", "1000", "10000", "100000"})
//  @Param({"100", "5000"})
//  @Param({"100"})
  @Param({"1000000000"})
  private String _limit;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private Distribution.DataSupplier _supplier;

  @Benchmark
  public BrokerResponseNative query() {
//    return getBrokerResponse(_query + " LIMIT 100000000");
    return getBrokerResponse(_query + " LIMIT " + _limit);
  }

  @Setup
  public void setUp()
      throws Exception {
    _supplier = Distribution.createSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    _indexSegments = new ArrayList<>();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    for (int i = 0; i < _numSegments; i++) {
      buildSegment(String.format(SEGMENT_NAME_TEMPLATE, i));
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, String.format(SEGMENT_NAME_TEMPLATE, i)),
          indexLoadingConfig));
    }
    _indexSegment = _indexSegments.get(0);
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  protected static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  protected static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  protected static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(8);
  protected static final BrokerMetrics BROKER_METRICS = Mockito.mock(BrokerMetrics.class);

  public final void shutdownExecutor() {
    EXECUTOR_SERVICE.shutdownNow();
  }

  protected List<List<IndexSegment>> getDistinctInstances() {
    return List.of(getIndexSegments());
  }

  /**
   * Run query on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected <T extends Operator> T getOperator(@Language("sql") String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(serverPinotQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(getIndexSegment()), queryContext).run();
  }

  /**
   * Run query with hard-coded filter on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings("rawtypes")
  protected <T extends Operator> T getOperatorWithFilter(@Language("sql") String query) {
    return getOperator(query + getFilter());
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  protected BrokerResponseNative getBrokerResponse(@Language("sql") String query) {
    return getBrokerResponse(query, PLAN_MAKER);
  }

  /**
   * Run query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  protected BrokerResponseNative getBrokerResponseWithFilter(@Language("sql") String query) {
    return getBrokerResponse(query + getFilter());
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  protected BrokerResponseNative getBrokerResponse(@Language("sql") String query, PlanMaker planMaker) {
    return getBrokerResponse(query, planMaker, null);
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  protected BrokerResponseNative getBrokerResponse(
      @Language("sql") String query, @Nullable Map<String, String> extraQueryOptions) {
    return getBrokerResponse(query, PLAN_MAKER, extraQueryOptions);
  }

  /**
   * Run query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  private BrokerResponseNative getBrokerResponse(@Language("sql") String query, PlanMaker planMaker,
      @Nullable Map<String, String> extraQueryOptions) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    if (extraQueryOptions != null) {
      Map<String, String> queryOptions = pinotQuery.getQueryOptions();
      if (queryOptions == null) {
        queryOptions = new HashMap<>();
        pinotQuery.setQueryOptions(queryOptions);
      }
      queryOptions.putAll(extraQueryOptions);
    }
    return getBrokerResponse(pinotQuery, planMaker);
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  private BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery, PlanMaker planMaker) {
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    QueryContext serverQueryContext =
        serverPinotQuery == pinotQuery ? queryContext : QueryContextConverterUtils.getQueryContext(serverPinotQuery);

    List<List<IndexSegment>> instances = getDistinctInstances();
    if (instances.size() == 2) {
      return getBrokerResponseDistinctInstances(pinotQuery, planMaker);
    }

    // Server side
    serverQueryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan =
        planMaker.makeInstancePlan(getSegmentContexts(getIndexSegments()), serverQueryContext, EXECUTOR_SERVICE, null);
    InstanceResponseBlock instanceResponse;
    try {
//    try (Recording recording = new Recording()) {
//      recording.setName("server-groupby");
//      recording.enable("jdk.ExecutionSample");
//      recording.start();
      long startTime = System.currentTimeMillis();
      instanceResponse = queryContext.isExplain()
          ? ServerQueryExecutorV1Impl.executeDescribeExplain(plan, queryContext)
          : plan.execute();
      long endTime = System.currentTimeMillis();
//      recording.stop();
//      recording.dump(Path.of("server-groupby.jfr"));
//      System.out.println("[Metric] Server time used: " + (endTime - startTime) / 1000);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
//    catch (IOException e) {
//      throw new RuntimeException(e);
//    }

    // Broker side
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      // For multi-threaded BrokerReduceService, we cannot reuse the same data-table
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

  private static List<SegmentContext> getSegmentContexts(List<IndexSegment> indexSegments) {
    List<SegmentContext> segmentContexts = new ArrayList<>(indexSegments.size());
    indexSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    return segmentContexts;
  }

  protected BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap) {
    BrokerReduceService brokerReduceService = new BrokerReduceService(
        new PinotConfiguration(Map.of(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, BROKER_METRICS);
    brokerReduceService.shutDown();
    return brokerResponse;
  }

  /**
   * Run optimized query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>Unless explicitly override getDistinctInstances or initialize 2 distinct index segments in test, the result
   * should be equivalent to querying 4 identical index segments.
   * In order to query 2 distinct instances, the caller of this function should handle initializing 2 instances with
   * different index segments in the test and overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  protected BrokerResponseNative getBrokerResponseForOptimizedQuery(
      @Language("sql") String query, @Nullable TableConfig config, @Nullable Schema schema) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, config, schema);
    return getBrokerResponse(pinotQuery, PLAN_MAKER);
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * This test is particularly useful for testing statistical aggregation functions such as COVAR_POP, COVAR_SAMP, etc.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result will be equivalent to querying 2 distinct instances.
   * The caller of this function should handle initializing 2 instances with different index segments in the test and
   * overriding getDistinctInstances.
   * This can be particularly useful to test statistical aggregation functions.
   * @see StatisticalQueriesTest for an example use case.
   */
  private BrokerResponseNative getBrokerResponseDistinctInstances(PinotQuery pinotQuery, PlanMaker planMaker) {
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    QueryContext serverQueryContext =
        serverPinotQuery == pinotQuery ? queryContext : QueryContextConverterUtils.getQueryContext(serverPinotQuery);

    List<List<IndexSegment>> instances = getDistinctInstances();
    // Server side
    serverQueryContext.setEndTimeMs(
        System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan1 =
        planMaker.makeInstancePlan(getSegmentContexts(instances.get(0)), serverQueryContext, EXECUTOR_SERVICE, null);
    Plan plan2 =
        planMaker.makeInstancePlan(getSegmentContexts(instances.get(1)), serverQueryContext, EXECUTOR_SERVICE, null);

    InstanceResponseBlock instanceResponse1;
    try {
      instanceResponse1 = queryContext.isExplain()
          ? ServerQueryExecutorV1Impl.executeDescribeExplain(plan1, queryContext)
          : plan1.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    InstanceResponseBlock instanceResponse2;
    try {
      instanceResponse2 = queryContext.isExplain()
          ? ServerQueryExecutorV1Impl.executeDescribeExplain(plan2, queryContext)
          : plan2.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }

    // Broker side
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      // For multi-threaded BrokerReduceService, we cannot reuse the same data-table
      byte[] serializedResponse1 = instanceResponse1.toDataTable().toBytes();
      byte[] serializedResponse2 = instanceResponse2.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse1));
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME),
          DataTableFactory.getDataTable(serializedResponse2));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    BrokerRequest serverBrokerRequest =
        serverPinotQuery == pinotQuery ? brokerRequest : CalciteSqlCompiler.convertToBrokerRequest(serverPinotQuery);
    return reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap);
  }

  static LazyDataGenerator createTestData(int numRows, Distribution.DataSupplier supplier, String segmentName) {
    //create data lazily to prevent OOM and speed up setup

    return new LazyDataGenerator() {
      private final Map<Integer, UUID> _strings = new HashMap<>();
      private final String[] _lowCardinalityValues =
          IntStream.range(0, 10).mapToObj(i -> "value" + i).toArray(String[]::new);
      private final int[] _lowCardinalityIntValues =
          IntStream.range(0, 10).toArray();
      private Distribution.DataSupplier _supplier = supplier;
      private String[] _jsons = generateJsons();

      @Override
      public int size() {
        return numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        row.putValue(INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(RAW_INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(LOW_CARDINALITY_INT_COL_NAME, _lowCardinalityIntValues[i % _lowCardinalityIntValues.length]);
        row.putValue(STRING_COL_NAME,
//            segmentName + // to make segments heterogeneous
            _strings.computeIfAbsent((int) _supplier.getAsLong(), k -> UUID.randomUUID()).toString());
        row.putValue(RAW_STRING_COL_NAME,
//            segmentName + // to make segments heterogeneous
            _strings.computeIfAbsent((int) _supplier.getAsLong(), k -> UUID.randomUUID()).toString());
        row.putValue(LOW_CARDINALITY_STRING_COL_NAME, _lowCardinalityValues[i % _lowCardinalityValues.length]);

        return null;
      }

      @Override
      public void rewind() {
        _strings.clear();
        _supplier.reset();
      }

      private String[] generateJsons() {
        String[] jsons = new String[1000];
        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < jsons.length; i++) {
          buffer.setLength(0);
          buffer.append("{ \"type\": \"type").append(i % 50).append("\"")
              .append(", \"changes\": [ ")
              .append("{ \"author\": { \"name\": \"author").append(i % 1000).append("\" } }");
          if (i % 2 == 0) {
            buffer.append(", { \"author\": { \"name\": \"author").append(i % 100).append("\" } }");
          }
          buffer.append(" ] }");
          jsons[i] = buffer.toString();
        }

        return jsons;
      }
    };
  }

  private void buildSegment(String segmentName)
      throws Exception {
    LazyDataGenerator rows = createTestData(_numRows, _supplier, segmentName);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    //save generator state so that other segments are not identical to this one
    _supplier.snapshot();
  }


  protected String getFilter() {
    return null;
  }

  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }
}
