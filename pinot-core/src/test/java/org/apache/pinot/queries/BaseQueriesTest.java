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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


/**
 * Base class for queries tests.
 */
public abstract class BaseQueriesTest {
  protected static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();
  protected static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  protected static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  protected static final QueryOptimizer OPTIMIZER = new QueryOptimizer();

  protected static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);

  protected abstract String getFilter();

  protected abstract IndexSegment getIndexSegment();

  protected abstract List<IndexSegment> getIndexSegments();

  /**
   * Run PQL query on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected <T extends Operator> T getOperatorForPqlQuery(String pqlQuery) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromPQL(pqlQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
  }

  /**
   * Run PQL query with hard-coded filter on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings("rawtypes")
  protected <T extends Operator> T getOperatorForPqlQueryWithFilter(String pqlQuery) {
    return getOperatorForPqlQuery(pqlQuery + getFilter());
  }

  /**
   * Run SQL query on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected <T extends Operator> T getOperatorForSqlQuery(String sqlQuery) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(sqlQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
  }

  /**
   * Run SQL query with hard-coded filter on single index segment.
   * <p>Use this to test a single operator.
   */
  @SuppressWarnings("rawtypes")
  protected <T extends Operator> T getOperatorForSqlQueryWithFilter(String sqlQuery) {
    return getOperatorForSqlQuery(sqlQuery + getFilter());
  }

  /**
   * Run PQL query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String pqlQuery) {
    return getBrokerResponseForPqlQuery(pqlQuery, PLAN_MAKER);
  }

  /**
   * Run PQL query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQueryWithFilter(String pqlQuery) {
    return getBrokerResponseForPqlQuery(pqlQuery + getFilter());
  }

  /**
   * Run PQL query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String pqlQuery, PlanMaker planMaker) {
    return getBrokerResponseForPqlQuery(pqlQuery, planMaker, null);
  }

  /**
   * Run PQL query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String pqlQuery,
      @Nullable Map<String, String> extraQueryOptions) {
    return getBrokerResponseForPqlQuery(pqlQuery, PLAN_MAKER, extraQueryOptions);
  }

  /**
   * Run PQL query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  private BrokerResponseNative getBrokerResponseForPqlQuery(String pqlQuery, PlanMaker planMaker,
      @Nullable Map<String, String> extraQueryOptions) {
    BrokerRequest brokerRequest = PQL_COMPILER.compileToBrokerRequest(pqlQuery);
    if (extraQueryOptions != null) {
      Map<String, String> queryOptions = brokerRequest.getQueryOptions();
      if (queryOptions != null) {
        queryOptions.putAll(extraQueryOptions);
      } else {
        brokerRequest.setQueryOptions(extraQueryOptions);
      }
    }
    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    return getBrokerResponse(queryContext, planMaker);
  }

  /**
   * Run SQL query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForSqlQuery(String sqlQuery) {
    return getBrokerResponseForSqlQuery(sqlQuery, PLAN_MAKER);
  }

  /**
   * Run SQL query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForSqlQueryWithFilter(String sqlQuery) {
    return getBrokerResponseForSqlQuery(sqlQuery + getFilter());
  }

  /**
   * Run SQL query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  @SuppressWarnings("SameParameterValue")
  protected BrokerResponseNative getBrokerResponseForSqlQuery(String sqlQuery, PlanMaker planMaker) {
    BrokerRequest brokerRequest = SQL_COMPILER.compileToBrokerRequest(sqlQuery);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions == null) {
      queryOptions = new HashMap<>();
      brokerRequest.getPinotQuery().setQueryOptions(queryOptions);
    }
    queryOptions.put(Request.QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(Request.QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    return getBrokerResponse(queryContext, planMaker);
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  private BrokerResponseNative getBrokerResponse(QueryContext queryContext, PlanMaker planMaker) {
    // Server side.
    Plan plan = planMaker.makeInstancePlan(getIndexSegments(), queryContext, EXECUTOR_SERVICE,
        System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    DataTable instanceResponse = plan.execute();

    // Broker side.
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2); // 2 Threads for 2 Data-tables.
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(properties));
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();

    try {

      // For multi-threaded BrokerReduceService, we cannot reuse the same data-table.
      byte[] serializedResponse = instanceResponse.toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse));
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME),
          DataTableFactory.getDataTable(serializedResponse));
    } catch (Exception e) {
      Utils.rethrowException(e);
    }

    BrokerResponseNative brokerResponse = brokerReduceService
        .reduceOnDataTable(queryContext.getBrokerRequest(), dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, null);
    brokerReduceService.shutDown();
    return brokerResponse;
  }

  /**
   * Run optimized SQL query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForOptimizedSqlQuery(String sqlQuery, Schema schema) {
    return getBrokerResponseForOptimizedSqlQuery(sqlQuery, schema, PLAN_MAKER);
  }

  /**
   * Run optimized SQL query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   */
  protected BrokerResponseNative getBrokerResponseForOptimizedSqlQuery(String sqlQuery, Schema schema, PlanMaker planMaker) {
    BrokerRequest brokerRequest = SQL_COMPILER.compileToBrokerRequest(sqlQuery);
    OPTIMIZER.optimize(brokerRequest.getPinotQuery(), schema);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions == null) {
      queryOptions = new HashMap<>();
      brokerRequest.getPinotQuery().setQueryOptions(queryOptions);
    }
    queryOptions.put(Request.QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(Request.QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    return getBrokerResponse(queryContext, planMaker);
  }
}
