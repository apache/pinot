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
package org.apache.pinot.queries;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
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
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Base class for queries tests.
 */
public abstract class BaseQueriesTest {
  protected static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  protected static final QueryOptimizer OPTIMIZER = new QueryOptimizer();

  protected static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);

  protected abstract String getFilter();

  protected abstract IndexSegment getIndexSegment();

  protected abstract List<IndexSegment> getIndexSegments();

  protected List<List<IndexSegment>> getDistinctInstances() {
    return Collections.singletonList(getIndexSegments());
  }

  public final void shutdownExecutor() {
    EXECUTOR_SERVICE.shutdownNow();
  }

  /**
   * Run query on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected <T extends Operator> T getOperator(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(serverPinotQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
  }

  /**
   * Run query with hard-coded filter on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings("rawtypes")
  protected <T extends Operator> T getOperatorWithFilter(String query) {
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
  protected BrokerResponseNative getBrokerResponse(String query) {
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
  protected BrokerResponseNative getBrokerResponseWithFilter(String query) {
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
  protected BrokerResponseNative getBrokerResponse(String query, PlanMaker planMaker) {
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
  protected BrokerResponseNative getBrokerResponse(String query, @Nullable Map<String, String> extraQueryOptions) {
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
  private BrokerResponseNative getBrokerResponse(String query, PlanMaker planMaker,
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
    serverQueryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan = planMaker.makeInstancePlan(getIndexSegments(), serverQueryContext, EXECUTOR_SERVICE, null);
    InstanceResponseBlock instanceResponse;
    try {
      instanceResponse =
          queryContext.isExplain() ? ServerQueryExecutorV1Impl.executeExplainQuery(plan, queryContext) : plan.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }

    // Broker side
    // Use 2 Threads for 2 data-tables
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(
        Collections.singletonMap(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
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
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, null);
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
  protected BrokerResponseNative getBrokerResponseForOptimizedQuery(String query, @Nullable TableConfig config,
      @Nullable Schema schema) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, config, schema);
    return getBrokerResponse(pinotQuery, PLAN_MAKER);
  }

  /**
   * Helper function to call reloadSegment on an existing index directory. The segment is preprocessed using the
   * config provided in indexLoadingConfig. It returns an immutable segment.
   */
  protected ImmutableSegment reloadSegment(File indexDir, IndexLoadingConfig indexLoadingConfig, Schema schema)
      throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    PinotConfiguration configuration = new PinotConfiguration(props);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      processor.process();
    }
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig);
    return immutableSegment;
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
    serverQueryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan1 = planMaker.makeInstancePlan(instances.get(0), serverQueryContext, EXECUTOR_SERVICE, null);
    Plan plan2 = planMaker.makeInstancePlan(instances.get(1), serverQueryContext, EXECUTOR_SERVICE, null);

    InstanceResponseBlock instanceResponse1;
    try {
      instanceResponse1 = queryContext.isExplain() ? ServerQueryExecutorV1Impl.executeExplainQuery(plan1, queryContext)
          : plan1.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    InstanceResponseBlock instanceResponse2;
    try {
      instanceResponse2 = queryContext.isExplain() ? ServerQueryExecutorV1Impl.executeExplainQuery(plan2, queryContext)
          : plan2.execute();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }

    // Broker side
    // Use 2 Threads for 2 data-tables
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(
        Collections.singletonMap(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
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
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, null);
    brokerReduceService.shutDown();
    return brokerResponse;
  }
}
