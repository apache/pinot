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
package org.apache.pinot.core.plan;

import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.FilteredGroupByOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>GroupByPlanNode</code> class provides the execution plan for group-by query on a single segment.
 */
public class GroupByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public GroupByPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<GroupByResultsBlock> run() {
    assert _queryContext.getAggregationFunctions() != null && _queryContext.getGroupByExpressions() != null;
    return _queryContext.hasFilteredAggregations() ? buildFilteredGroupByPlan() : buildNonFilteredGroupByPlan();
  }

  private FilteredGroupByOperator buildFilteredGroupByPlan() {
    return new FilteredGroupByOperator(_queryContext,
        AggregationFunctionUtils.buildFilteredAggregationInfos(_indexSegment, _queryContext),
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  private GroupByOperator buildNonFilteredGroupByPlan() {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();
    AggregationFunctionUtils.AggregationInfo aggregationInfo =
        AggregationFunctionUtils.buildAggregationInfo(_indexSegment, _queryContext,
            _queryContext.getAggregationFunctions(), _queryContext.getFilter(), filterOperator,
            filterPlanNode.getPredicateEvaluators());
    return new GroupByOperator(_queryContext, aggregationInfo, _indexSegment.getSegmentMetadata().getTotalDocs());
  }
}
