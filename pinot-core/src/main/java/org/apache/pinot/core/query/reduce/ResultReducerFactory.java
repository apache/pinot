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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Factory class to construct the right result reducer based on the query context.
 */
@SuppressWarnings("rawtypes")
public final class ResultReducerFactory {
  private ResultReducerFactory() {
  }

  /**
   * Constructs the right result reducer based on the given query context.
   */
  public static DataTableReducer getResultReducer(QueryContext queryContext) {
    BrokerRequest brokerRequest = queryContext.getBrokerRequest();
    if (brokerRequest != null && brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().isExplain()) {
      return new ExplainPlanDataTableReducer(queryContext);
    }

    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    if (aggregationFunctions == null) {
      // Selection query
      return new SelectionDataTableReducer(queryContext);
    } else {
      // Aggregation query
      if (queryContext.getGroupByExpressions() == null) {
        // Aggregation only query
        if (aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == AggregationFunctionType.DISTINCT) {
          // Distinct query
          return new DistinctDataTableReducer(queryContext, (DistinctAggregationFunction) aggregationFunctions[0]);
        } else {
          return new AggregationDataTableReducer(queryContext);
        }
      } else {
        // Aggregation group-by query
        return new GroupByDataTableReducer(queryContext);
      }
    }
  }
}
