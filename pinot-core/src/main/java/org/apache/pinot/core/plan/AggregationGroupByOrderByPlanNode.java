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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * The <code>AggregationGroupByOrderByPlanNode</code> class provides the execution plan for aggregation group-by order-by query on a
 * single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOrderByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final int _minSegmentTrimSize;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final List<OrderByExpressionContext> _orderByExpressionContexts;
  private final TransformPlanNode _transformPlanNode;
  private final StarTreeTransformPlanNode _starTreeTransformPlanNode;
  private final QueryContext _queryContext;
  private final boolean _enableGroupByOpt;

  public AggregationGroupByOrderByPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      int maxInitialResultHolderCapacity, int numGroupsLimit, int minSegmentTrimSize) {
    _indexSegment = indexSegment;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _groupByExpressions = groupByExpressions.toArray(new ExpressionContext[0]);
    _queryContext = queryContext;
    _minSegmentTrimSize = minSegmentTrimSize;
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    _orderByExpressionContexts = new ArrayList<>();

    if (orderByExpressionContexts != null) {
      _orderByExpressionContexts.addAll(orderByExpressionContexts);
    }

    _enableGroupByOpt = checkOrderByOptimization();

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(queryContext)) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(_aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(indexSegment, queryContext.getFilter());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils
                .isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs, _groupByExpressions,
                    predicateEvaluatorsMap.keySet())) {
              _transformPlanNode = null;
              _starTreeTransformPlanNode =
                  new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, _groupByExpressions,
                      predicateEvaluatorsMap, queryContext.getDebugOptions());
              return;
            }
          }
        }
      }
    }


    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(_aggregationFunctions, _groupByExpressions);
    if (_enableGroupByOpt) {
      // Add docIds
      expressionsToTransform.add(ExpressionContext.forIdentifier(CommonConstants.Segment.BuiltInVirtualColumn.DOCID));
    }
    _transformPlanNode =
        new TransformPlanNode(_indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _starTreeTransformPlanNode = null;
  }

  @Override
  public AggregationGroupByOrderByOperator run() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_transformPlanNode != null) {
      // Do not use star-tree
      return new AggregationGroupByOrderByOperator(_indexSegment, _aggregationFunctions, _groupByExpressions,
          _orderByExpressionContexts.toArray(new OrderByExpressionContext[0]), _maxInitialResultHolderCapacity,
          _numGroupsLimit, _minSegmentTrimSize, _transformPlanNode.run(), numTotalDocs, _queryContext,
          _enableGroupByOpt, false);
    } else {
      // Use star-tree
      return new AggregationGroupByOrderByOperator(_indexSegment, _aggregationFunctions, _groupByExpressions,
          _orderByExpressionContexts.toArray(new OrderByExpressionContext[0]), _maxInitialResultHolderCapacity,
          _numGroupsLimit, _minSegmentTrimSize, _starTreeTransformPlanNode.run(), numTotalDocs, _queryContext,
          _enableGroupByOpt, true);
    }
  }

  private boolean checkOrderByOptimization() {
    // Check HAVING
    if (_queryContext.getHavingFilter() != null) {
      return false;
    }
    Set<ExpressionContext> orderByExpressionsSet = new HashSet<>();
    // Check function expressions
    for (OrderByExpressionContext orderByExpressionContext : _orderByExpressionContexts) {
      orderByExpressionsSet.add(orderByExpressionContext.getExpression());
    }
    // Add group by expressions to order by expressions
    for (ExpressionContext groupByExpression : _groupByExpressions) {
      if (!orderByExpressionsSet.contains(groupByExpression)) {
        _orderByExpressionContexts.add(new OrderByExpressionContext(groupByExpression, true));
      }
    }
    boolean longOverflow = false;
    long cardinalityProduct = 1L;
    for (OrderByExpressionContext orderByExpressionContext : _orderByExpressionContexts) {
      ExpressionContext expression = orderByExpressionContext.getExpression();
      if (expression.getType() == ExpressionContext.Type.FUNCTION) {
        return false;
      }
      DataSource dataSource = _indexSegment.getDataSource(expression.getIdentifier());
      // Check single value
      if (!dataSource.getDataSourceMetadata().isSingleValue()) {
        return false;
      }
      // Check cardinality threshold
      Dictionary dictionary = dataSource.getDictionary();
      if (dictionary != null) {
        int cardinality = dictionary.length();
        if (!longOverflow) {
          if (cardinalityProduct > Long.MAX_VALUE / cardinality) {
            longOverflow = true;
          } else {
            cardinalityProduct *= cardinality;
          }
        }
      }
    }
    // Only apply optimization when cardinality is high
    return (longOverflow || cardinalityProduct >= _queryContext.getLimit()) && cardinalityProduct >= 500000;
  }
}
