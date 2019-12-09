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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;


/**
 * Interface for optimizations that can be done on the FilterQuery of a query.
 */
public abstract class FilterQueryOptimizer {
  private final String _optimizationName = OptimizationFlags.optimizationName(this.getClass());

  public abstract FilterQueryOptimizationResult optimize(FilterQuery filterQuery, FilterQueryMap filterQueryMap);

  public String getOptimizationName() {
    return _optimizationName;
  }

  static class FilterQueryOptimizationResult {
    FilterQuery _filterQuery;
    FilterQueryMap _filterQueryMap;

    FilterQueryOptimizationResult(FilterQuery filterQuery, FilterQueryMap filterQueryMap) {
      _filterQuery = filterQuery;
      _filterQueryMap = filterQueryMap;
    }

    public FilterQuery getFilterQuery() {
      return _filterQuery;
    }

    public FilterQueryMap getFilterQueryMap() {
      return _filterQueryMap;
    }
  }

}
