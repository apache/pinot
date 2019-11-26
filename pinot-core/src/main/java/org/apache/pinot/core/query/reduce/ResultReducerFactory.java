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

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.util.QueryOptions;


/**
 * Factory class to construct the right result reducer based on the broker request
 */
public final class ResultReducerFactory {

  /**
   * Constructs the right result reducer based on the broker request
   */
  public static ResultReducer getResultReducer(BrokerRequest brokerRequest) {
    ResultReducer resultReducer;
    QueryOptions queryOptions = new QueryOptions(brokerRequest.getQueryOptions());
    if (brokerRequest.getSelections() != null) {
      // Selection query
      resultReducer = new SelectionResultReducer(brokerRequest, queryOptions);
    } else {
      // Aggregation query
      AggregationFunction[] aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(brokerRequest);
      if (!brokerRequest.isSetGroupBy()) {
        // Aggregation only query
        if (aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == AggregationFunctionType.DISTINCT) {
          // Distinct query
          resultReducer = new DistinctResultReducer(brokerRequest, aggregationFunctions[0], queryOptions);
        } else {
          resultReducer = new AggregationResultReducer(brokerRequest, aggregationFunctions, queryOptions);
        }
      } else {
        // Aggregation group-by query
        resultReducer = new GroupByResultReducer(brokerRequest, aggregationFunctions, queryOptions);
      }
    }
    return resultReducer;
  }
}
