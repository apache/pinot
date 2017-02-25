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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import java.util.Arrays;
import java.util.List;


public class BrokerRequestOptimizer {
  private static final List<? extends FilterQueryTreeOptimizer> FILTER_QUERY_TREE_OPTIMIZERS = Arrays.asList(
      new FlattenNestedPredicatesFilterQueryTreeOptimizer(),
      new MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer(),
      new RangeMergeOptimizer()
  );

  /**
   * Optimizes the given broker request.
   *
   * @param brokerRequest BrokerRequest that is to be optimized
   * @param timeColumn Time column for the table
   * @return An optimized request
   */
  public BrokerRequest optimize(BrokerRequest brokerRequest, String timeColumn) {
    OptimizationFlags optimizationFlags = OptimizationFlags.getOptimizationFlags(brokerRequest);
    optimizeFilterQueryTree(brokerRequest, timeColumn, optimizationFlags);

    return brokerRequest;
  }

  /**
   * Optimizes the filter query tree of a broker request in place.
   * @param brokerRequest The broker request to optimize
   * @param timeColumn time column
   */
  private void optimizeFilterQueryTree(BrokerRequest brokerRequest, String timeColumn,
      OptimizationFlags optimizationFlags) {
    FilterQueryTree filterQueryTree = null;
    FilterQuery q = brokerRequest.getFilterQuery();

    if (q == null || brokerRequest.getFilterSubQueryMap() == null) {
      return;
    }

    filterQueryTree = RequestUtils.buildFilterQuery(q.getId(), brokerRequest.getFilterSubQueryMap().getFilterQueryMap());
    FilterQueryOptimizerRequest.FilterQueryOptimizerRequestBuilder builder =
        new FilterQueryOptimizerRequest.FilterQueryOptimizerRequestBuilder();

    FilterQueryOptimizerRequest request = builder.setFilterQueryTree(filterQueryTree).setTimeColumn(timeColumn).build();
    if (optimizationFlags == null) {
      for (FilterQueryTreeOptimizer filterQueryTreeOptimizer : FILTER_QUERY_TREE_OPTIMIZERS) {
        filterQueryTree = filterQueryTreeOptimizer.optimize(request);
        request.setFilterQueryTree(filterQueryTree); // Optimizers may return a new tree instead of in-place optimization
      }
    } else {
      if (optimizationFlags.isOptimizationEnabled("filterQueryTree")) {
        for (FilterQueryTreeOptimizer filterQueryTreeOptimizer : FILTER_QUERY_TREE_OPTIMIZERS) {
          if (optimizationFlags.isOptimizationEnabled(filterQueryTreeOptimizer.getOptimizationName())) {
            filterQueryTree = filterQueryTreeOptimizer.optimize(request);
            request.setFilterQueryTree(filterQueryTree); // Optimizers may return a new tree instead of in-place optimization
          }
        }
      }
    }

    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
  }
}
