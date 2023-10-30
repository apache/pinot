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
package org.apache.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


public class CombinedFilteredAggregationContext {
  private final BaseFilterOperator _baseFilterOperator;
  private final FilterContext _filterContext;
  private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators;
  private final List<AggregationFunction> _aggregationFunctions;

  public CombinedFilteredAggregationContext(BaseFilterOperator baseFilterOperator,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators, FilterContext filterContext, List<AggregationFunction> aggregationFunctions) {
    _baseFilterOperator = baseFilterOperator;
    _predicateEvaluators = predicateEvaluators;
    _filterContext = filterContext;
    _aggregationFunctions = aggregationFunctions;
  }

  public CombinedFilteredAggregationContext(BaseFilterOperator baseFilterOperator,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators, FilterContext filterContext) {
    this(baseFilterOperator, predicateEvaluators, filterContext, new ArrayList<>());
  }


  public BaseFilterOperator getBaseFilterOperator() {
    return _baseFilterOperator;
  }

  public List<Pair<Predicate, PredicateEvaluator>> getPredicateEvaluatorMap() {
    return _predicateEvaluators;
  }

  public FilterContext getFilterContext() {
    return _filterContext;
  }

  public List<AggregationFunction> getAggregationFunctions() {
    return _aggregationFunctions;
  }

  public void add(AggregationFunction aggregationFunction) {
    _aggregationFunctions.add(aggregationFunction);
  }
}
