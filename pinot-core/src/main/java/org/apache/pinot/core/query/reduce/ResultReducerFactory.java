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

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;


/**
 * Factory class to construct the right result reducer based on the query context.
 */
public final class ResultReducerFactory {
  private ResultReducerFactory() {
  }

  /**
   * Constructs the right result reducer based on the given query context.
   */
  public static DataTableReducer getResultReducer(QueryContext queryContext) {
    if (queryContext.isExplain()) {
      return new ExplainPlanDataTableReducer(queryContext);
    }
    if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return new SelectionDataTableReducer(queryContext);
    }
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      if (queryContext.getGroupByExpressions() == null) {
        return new AggregationDataTableReducer(queryContext);
      } else {
        return new GroupByDataTableReducer(queryContext);
      }
    }
    assert QueryContextUtils.isDistinctQuery(queryContext);
    return new DistinctDataTableReducer(queryContext);
  }

  public static StreamingReducer getStreamingReducer(QueryContext queryContext) {
    if (!QueryContextUtils.isSelectionQuery(queryContext) || queryContext.getOrderByExpressions() != null) {
      throw new UnsupportedOperationException("Only selection queries are supported");
    } else {
      // Selection query
      return new SelectionOnlyStreamingReducer(queryContext);
    }
  }
}
