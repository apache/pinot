/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.util.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApproximateQueryTestUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApproximateQueryTestUtil.class);

  private static final ReduceService<BrokerResponseNative> REDUCE_SERVICE = new BrokerReduceService();
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
  private static long counter = 0;

  public static Object runQuery(QueryExecutor queryExecutor, String segmentName,
      AvroQueryGenerator.TestAggreationQuery query, ServerMetrics metrics) {
    LOGGER.info("\nRunning: " + query.getPql());
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query.getPql());
    InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    QueryRequest queryRequest = new QueryRequest(instanceRequest, metrics);
    final DataTable instanceResponse = queryExecutor.processQuery(queryRequest);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);

    AggregationResult result = brokerResponse.getAggregationResults().get(0);
    Assert.assertNotNull(result);
    if (result.getValue() != null) {
      LOGGER.info("Aggregation Result is " + result.getValue().toString());
    } else if (result.getGroupByResult() != null) {
      LOGGER.info("GroupBy Result is " + result.getGroupByResult().toString());
    } else {
      throw new RuntimeException("Aggregation and GroupBy Results both null.");
    }

    // compute value
    Object val;
    if (query instanceof AvroQueryGenerator.TestSimpleAggreationQuery) {
      val = Double.parseDouble(brokerResponse.getAggregationResults().get(0).getValue().toString());
    } else {
      val = brokerResponse.getAggregationResults().get(0).getGroupByResult();
    }

    return val;
  }

  public static void runApproximationQueries(QueryExecutor queryExecutor, String segmentName,
      List<? extends AvroQueryGenerator.TestAggreationQuery> queries, double precision, ServerMetrics metrics) throws Exception {
    boolean isAccurate = true;
    Object accurateValue = null;

    for (final AvroQueryGenerator.TestAggreationQuery query : queries) {
      Object val = runQuery(queryExecutor, segmentName, query, metrics);
      if (isAccurate) {
        // store accurate value
        accurateValue = val;
        isAccurate = false;
      } else {
        // compare value with accurate value
        // it's estimation so we need to test its result within error bound
        if (query instanceof AvroQueryGenerator.TestSimpleAggreationQuery) {
          TestUtils.assertApproximation((Double) val, (Double) accurateValue, precision);
        } else {
          TestUtils.assertGroupByResultsApproximation(
              (List<GroupByResult>) val, (List<GroupByResult>) accurateValue, precision);
        }
        isAccurate = true;
      }
    }
  }
}
