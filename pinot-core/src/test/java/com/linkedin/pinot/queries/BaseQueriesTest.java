/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Base class for queries tests.
 */
public abstract class BaseQueriesTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
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
    return (T) PLAN_MAKER.makeInnerSegmentPlan(getIndexSegment(), COMPILER.compileToBrokerRequest(query)).run();
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
  protected BrokerResponseNative getBrokerResponseForQuery(String query, PlanMaker planMaker) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    // Server side.
    Plan plan = planMaker.makeInterSegmentPlan(getSegmentDataManagers(), brokerRequest, EXECUTOR_SERVICE, 10_000);
    DataTable instanceResponse = plan.execute();

    // Broker side.
    BrokerReduceService brokerReduceService = new BrokerReduceService();
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    dataTableMap.put(new ServerInstance("localhost:1111"), instanceResponse);
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
  protected BrokerResponseNative getBrokerResponseForQuery(String query) {
    return getBrokerResponseForQuery(query, PLAN_MAKER);
  }

  /**
   * Run query with hard-coded filter on multiple index segments.
   * <p>Use this to test the whole flow from server to broker.
   * <p>The result should be equivalent to querying 4 identical index segments.
   *
   * @param query PQL query without any filter.
   * @return broker response.
   */
  protected BrokerResponseNative getBrokerResponseForQueryWithFilter(String query) {
    return getBrokerResponseForQuery(query + getFilter());
  }
}
