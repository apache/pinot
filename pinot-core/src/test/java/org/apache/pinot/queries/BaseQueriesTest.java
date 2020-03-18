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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Server;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


/**
 * Base class for queries tests.
 */
public abstract class BaseQueriesTest {
  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);

  protected abstract String getFilter();

  protected abstract IndexSegment getIndexSegment();

  protected abstract List<SegmentDataManager> getSegmentDataManagers();

  /**
   * Run query on single index segment.
   * <p>Use this to test a single operator.
   *
   * @param query PQL query.
   * @return query operator.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Operator> T getOperatorForQuery(String query) {
    return (T) PLAN_MAKER.makeInnerSegmentPlan(getIndexSegment(), PQL_COMPILER.compileToBrokerRequest(query)).run();
  }

  /**
   * Run query with hard-coded filter on single index segment.
   * <p>Use this to test a single operator.
   *
   * @param query PQL query without any filter.
   * @return query operator.
   */
  protected <T extends Operator> T getOperatorForQueryWithFilter(String query) {
    return getOperatorForQuery(query + getFilter());
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query.
   * @param planMaker Plan maker.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String query, PlanMaker planMaker) {
    return getBrokerResponseForPqlQuery(query, planMaker, null);
  }

  /**
   * Run query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query.
   * @param planMaker Plan maker.
   * @return broker response.
   */
  private BrokerResponseNative getBrokerResponseForPqlQuery(String query, PlanMaker planMaker,
      Map<String, String> queryOptions) {
    BrokerRequest brokerRequest = PQL_COMPILER.compileToBrokerRequest(query);
    return getBrokerResponseForBrokerRequest(brokerRequest, planMaker, queryOptions);
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query SQL query.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForSqlQuery(String query) {
    return getBrokerResponseForSqlQuery(query, PLAN_MAKER);
  }

  /**
   * Run query on multiple index segments with custom plan maker.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query SQL query.
   * @param planMaker Plan maker.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForSqlQuery(String query, PlanMaker planMaker) {
    HashMap<String, String> queryOptions = new HashMap<>();
    queryOptions.put("groupByMode", "sql");
    queryOptions.put("responseFormat", "sql");
    return getBrokerResponseForSqlQuery(query, planMaker, queryOptions);
  }

  /**
   * Run query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query SQL query.
   * @param planMaker Plan maker.
   * @return broker response.
   */
  private BrokerResponseNative getBrokerResponseForSqlQuery(String query, PlanMaker planMaker,
      Map<String, String> queryOptions) {
    BrokerRequest brokerRequest = SQL_COMPILER.compileToBrokerRequest(query);
    return getBrokerResponseForBrokerRequest(brokerRequest, planMaker, queryOptions);
  }

  /**
   * Run query on multiple index segments with custom plan maker and queryOptions.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param brokerRequest broker request.
   * @param planMaker Plan maker.
   * @return broker response.
   */
  private BrokerResponseNative getBrokerResponseForBrokerRequest(BrokerRequest brokerRequest, PlanMaker planMaker,
      Map<String, String> queryOptions) {
    Map<String, String> allQueryOptions = new HashMap<>();

    if (queryOptions != null) {
      allQueryOptions.putAll(queryOptions);
    }
    if (brokerRequest.getQueryOptions() != null) {
      allQueryOptions.putAll(brokerRequest.getQueryOptions());
    }
    if (!allQueryOptions.isEmpty()) {
      brokerRequest.setQueryOptions(allQueryOptions);
    }

    // Server side.
    Plan plan = planMaker.makeInterSegmentPlan(getSegmentDataManagers(), brokerRequest, EXECUTOR_SERVICE,
        Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    DataTable instanceResponse = plan.execute();

    // Broker side.
    BrokerReduceService brokerReduceService = new BrokerReduceService();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE), instanceResponse);
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME), instanceResponse);
    return brokerReduceService.reduceOnDataTable(brokerRequest, dataTableMap, null);
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String query) {
    return getBrokerResponseForPqlQuery(query, PLAN_MAKER);
  }

  /**
   * Run query on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQuery(String query, Map<String, String> queryOptions) {
    return getBrokerResponseForPqlQuery(query, PLAN_MAKER, queryOptions);
  }

  /**
   * Run query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query without any filter.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForPqlQueryWithFilter(String query) {
    return getBrokerResponseForPqlQuery(query + getFilter());
  }

  /**
   * Run query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query SQL query without any filter.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForSqlQueryWithFilter(String query) {
    return getBrokerResponseForSqlQuery(query + getFilter());
  }
}
