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
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * The <code>AggregationGroupByOrderByPlanNode</code> class provides the execution plan for aggregation group-by order-by query on a
 * single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOrderByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final boolean _enableSegmentGroupTrim;
  private final int _inSegmentTrimLimit;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final TransformPlanNode _transformPlanNode;
  private final StarTreeTransformPlanNode _starTreeTransformPlanNode;
  private final QueryContext _queryContext;

  public AggregationGroupByOrderByPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      int maxInitialResultHolderCapacity, int numGroupsLimit, boolean enableSegmentGroupTrim, int inSegmentTrimLimit) {
    _indexSegment = indexSegment;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _enableSegmentGroupTrim = enableSegmentGroupTrim;
    _inSegmentTrimLimit = inSegmentTrimLimit;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _groupByExpressions = groupByExpressions.toArray(new ExpressionContext[0]);
    _queryContext = queryContext;

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
    _transformPlanNode =
        new TransformPlanNode(_indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _starTreeTransformPlanNode = null;
  }

  @Override
  public AggregationGroupByOrderByOperator run() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_transformPlanNode != null) {
      // Do not use star-tree
      return new AggregationGroupByOrderByOperator(_aggregationFunctions, _groupByExpressions,
          _maxInitialResultHolderCapacity, _numGroupsLimit, _inSegmentTrimLimit, _transformPlanNode.run(), numTotalDocs,
          _queryContext, false, _enableSegmentGroupTrim);
    } else {
      // Use star-tree
      return new AggregationGroupByOrderByOperator(_aggregationFunctions, _groupByExpressions,
          _maxInitialResultHolderCapacity, _numGroupsLimit, _inSegmentTrimLimit, _starTreeTransformPlanNode.run(),
          numTotalDocs, _queryContext, true, _enableSegmentGroupTrim);
    }
  }
}
