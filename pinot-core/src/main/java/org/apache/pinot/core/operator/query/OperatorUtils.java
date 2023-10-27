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
package org.apache.pinot.core.operator.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeProjectPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class OperatorUtils {
  private OperatorUtils() {
    // Prevent instantiation, make checkstyle happy
  }

  public static BaseProjectOperator<?> getProjectionOperatorBad(
      QueryContext queryContext,
      IndexSegment indexSegment,
      AggregationFunction[] aggregationFunctions,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators,
      BaseFilterOperator filterOperator,
      @Nullable List<ExpressionContext> groupByExpressionsList) {

    ExpressionContext[] groupByExpressions = null;
    if (groupByExpressionsList != null) {
      groupByExpressions = groupByExpressionsList.toArray(new ExpressionContext[0]);
    }

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null && !queryContext.isSkipStarTree()) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(indexSegment, queryContext.getFilter(), predicateEvaluators);
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              return new StarTreeProjectPlanNode(queryContext, starTreeV2, aggregationFunctionColumnPairs,
                  groupByExpressions, predicateEvaluatorsMap).run();
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressionsList);
    return new ProjectPlanNode(indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterOperator).run();
  }

  public static BaseProjectOperator<?> getProjectionOperator(
      QueryContext queryContext,
      IndexSegment indexSegment,
      AggregationFunction[] aggregationFunctions,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators,
      BaseFilterOperator filterOperator,
      @Nullable List<ExpressionContext> groupByExpressionsList) {

    ExpressionContext[] groupByExpressions = null;
    if (groupByExpressionsList != null) {
      groupByExpressions = groupByExpressionsList.toArray(new ExpressionContext[0]);
    }

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null && !queryContext.isSkipStarTree() && !queryContext.isNullHandlingEnabled()) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(indexSegment, queryContext.getFilter(), predicateEvaluators);
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              return new StarTreeProjectPlanNode(queryContext, starTreeV2, aggregationFunctionColumnPairs,
                  groupByExpressions, predicateEvaluatorsMap).run();
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressionsList);
    return new ProjectPlanNode(indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterOperator).run();
  }
}
