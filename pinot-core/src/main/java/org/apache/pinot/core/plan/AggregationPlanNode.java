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
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * The <code>AggregationPlanNode</code> class provides the execution plan for aggregation only query on a single
 * segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final AggregationFunction[] _aggregationFunctions;
  private final TransformPlanNode _transformPlanNode;
  private final StarTreeTransformPlanNode _starTreeTransformPlanNode;

  public AggregationPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(queryContext)) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(_aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(indexSegment, queryContext.getFilter());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs, null,
                predicateEvaluatorsMap.keySet())) {
              _transformPlanNode = null;
              _starTreeTransformPlanNode =
                  new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, null,
                      predicateEvaluatorsMap, queryContext.getDebugOptions());
              return;
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(_aggregationFunctions, null);
    _transformPlanNode =
        new TransformPlanNode(_indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _starTreeTransformPlanNode = null;
  }

  @Override
  public AggregationOperator run() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_transformPlanNode != null) {
      // Do not use star-tree
      return new AggregationOperator(_aggregationFunctions, _transformPlanNode.run(), numTotalDocs, false);
    } else {
      // Use star-tree
      return new AggregationOperator(_aggregationFunctions, _starTreeTransformPlanNode.run(), numTotalDocs, true);
    }
  }
}
