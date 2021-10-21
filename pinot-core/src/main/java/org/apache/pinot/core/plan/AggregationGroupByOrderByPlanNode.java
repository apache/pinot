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
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * The <code>AggregationGroupByOrderByPlanNode</code> class provides the execution plan for aggregation group-by
 * order-by query on a single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOrderByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public AggregationGroupByOrderByPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public AggregationGroupByOrderByOperator run() {
    assert _queryContext.getAggregationFunctions() != null;
    assert _queryContext.getGroupByExpressions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(_queryContext)) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter(), null);
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              TransformOperator transformOperator =
                  new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, groupByExpressions,
                      predicateEvaluatorsMap, _queryContext.getDebugOptions()).run();
              return new AggregationGroupByOrderByOperator(aggregationFunctions, groupByExpressions, transformOperator,
                  numTotalDocs, _queryContext, true);
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressions);
    TransformOperator transformPlanNode = new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    return new AggregationGroupByOrderByOperator(aggregationFunctions, groupByExpressions, transformPlanNode,
        numTotalDocs, _queryContext, false);
  }
}
