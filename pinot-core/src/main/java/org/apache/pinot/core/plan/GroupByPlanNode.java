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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.query.FilteredGroupByOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.operator.query.IndexedGroupByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeProjectPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


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
    List<Pair<AggregationFunction[], BaseProjectOperator<?>>> projectOperators =
        AggregationFunctionUtils.buildFilteredAggregateProjectOperators(_indexSegment, _queryContext);
    return new FilteredGroupByOperator(_queryContext, projectOperators,
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  private Operator<GroupByResultsBlock> buildNonFilteredGroupByPlan() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    List<ExpressionContext> groupByExpressionsList = _queryContext.getGroupByExpressions();
    assert aggregationFunctions != null && groupByExpressionsList != null;
    ExpressionContext[] groupByExpressions = groupByExpressionsList.toArray(new ExpressionContext[0]);

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    if (canOptimize(filterOperator, groupByExpressions, aggregationFunctions)) {
      Map<String, DataSource> dataSourceMap = new HashMap<>(1);
      String columName = groupByExpressions[0].getIdentifier();
      dataSourceMap.put(columName, _indexSegment.getDataSource(columName));
      return new IndexedGroupByOperator(_queryContext, groupByExpressions, filterOperator, dataSourceMap);
    }

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !_queryContext.isSkipStarTree()) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter(),
                filterPlanNode.getPredicateEvaluators());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              BaseProjectOperator<?> projectOperator =
                  new StarTreeProjectPlanNode(_queryContext, starTreeV2, aggregationFunctionColumnPairs,
                      groupByExpressions, predicateEvaluatorsMap).run();
              return new GroupByOperator(_queryContext, groupByExpressions, projectOperator, numTotalDocs, true);
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressionsList);
    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator).run();
    return new GroupByOperator(_queryContext, groupByExpressions, projectOperator, numTotalDocs, false);
  }

  private boolean canOptimize(BaseFilterOperator filterOperator, ExpressionContext[] groupByExpressions,
      AggregationFunction[] aggregationFunctions) {
    if (filterOperator instanceof MatchAllFilterOperator) {
      if (groupByExpressions != null && groupByExpressions.length == 1) {
        if (groupByExpressions[0].getType() == ExpressionContext.Type.IDENTIFIER) {
          if (aggregationFunctions != null && aggregationFunctions.length == 1) {
            return aggregationFunctions[0] instanceof CountAggregationFunction;
          }
        }
      }
    }
    return false;
  }
}
