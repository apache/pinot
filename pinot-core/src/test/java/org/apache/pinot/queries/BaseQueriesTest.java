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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datatable.DataTableFactory;
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
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Server;
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
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponse(String query) {
    return getBrokerResponse(query, PLAN_MAKER);
  }

  /**
   * Run query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseWithFilter(String query) {
    return getBrokerResponse(query + getFilter());
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponse(String query, PlanMaker planMaker) {
    return getBrokerResponse(query, planMaker, null);
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponse(String query, @Nullable Map<String, String> extraQueryOptions) {
    return getBrokerResponse(query, PLAN_MAKER, extraQueryOptions);
  }

  /**
   * Run query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
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
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  private BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery, PlanMaker planMaker) {
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    QueryContext serverQueryContext =
        serverPinotQuery == pinotQuery ? queryContext : QueryContextConverterUtils.getQueryContext(serverPinotQuery);

    // Server side
    serverQueryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    Plan plan = planMaker.makeInstancePlan(getIndexSegments(), serverQueryContext, EXECUTOR_SERVICE, null);
    DataTable instanceResponse =
        queryContext.isExplain() ? ServerQueryExecutorV1Impl.processExplainPlanQueries(plan) : plan.execute();

    // Broker side
    // Use 2 Threads for 2 data-tables
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(
        Collections.singletonMap(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      // For multi-threaded BrokerReduceService, we cannot reuse the same data-table
      byte[] serializedResponse = instanceResponse.toBytes();
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
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForOptimizedQuery(String query, @Nullable TableConfig config,
      @Nullable Schema schema) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, config, schema);
    return getBrokerResponse(pinotQuery, PLAN_MAKER);
  }
}
