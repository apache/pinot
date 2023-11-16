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

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.FilteredGroupByOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>GroupByPlanNode</code> class provides the execution plan for group-by query on a single segment.
 */
@SuppressWarnings("rawtypes")
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
    // TODO(egalpin): maybe change this to use ProjectionPlanNode instead of BaseProjectOperator
    List<Pair<AggregationFunction[], Pair<BaseProjectOperator<?>, Boolean>>> projectOperators =
        AggregationFunctionUtils.buildFilteredAggregateProjectOperators(_indexSegment, _queryContext);
    return new FilteredGroupByOperator(_queryContext, projectOperators,
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  private GroupByOperator buildNonFilteredGroupByPlan() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    List<ExpressionContext> groupByExpressionsList = _queryContext.getGroupByExpressions();
    assert aggregationFunctions != null && groupByExpressionsList != null;
    ExpressionContext[] groupByExpressions = groupByExpressionsList.toArray(new ExpressionContext[0]);

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    Pair<BaseProjectOperator<?>, Boolean> projectOperatorStPair =
        AggregationFunctionUtils.createProjectOperatorStPair(_indexSegment, _queryContext,
            _queryContext.getFilter(), aggregationFunctions,
            filterPlanNode.getPredicateEvaluators(), filterOperator);

    return new GroupByOperator(_queryContext, groupByExpressions, projectOperatorStPair.getLeft(), numTotalDocs,
        projectOperatorStPair.getRight());
  }
}
