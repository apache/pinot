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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.operator.query.MetadataBasedAggregationOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * The <code>AggregationPlanNode</code> class provides the execution plan for aggregation only query on a single
 * segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public AggregationPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    assert _queryContext.getAggregationFunctions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

    BaseFilterOperator preComputedFilterOperator = null;

    // Use metadata/dictionary to solve the query if possible
    // NOTE: Skip the segment with valid doc index because the valid doc index is equivalent to a filter
    if (_queryContext.getFilter() != null) {
      // Check if the filter is always true. If true, check if metadata or dictionary based plans can be used
      preComputedFilterOperator = FilterPlanNode
              .constructPhysicalOperator(_queryContext.getFilter(), _indexSegment,
                  _indexSegment.getSegmentMetadata().getTotalDocs(), _queryContext.getDebugOptions());


      if (preComputedFilterOperator.isResultMatchingAll() || preComputedFilterOperator.isResultEmpty()) {
        Operator<IntermediateResultsBlock> resultsBlockOperator = getAggregationOperator(aggregationFunctions, numTotalDocs);

        if (resultsBlockOperator != null) {
          return resultsBlockOperator;
        }
      }
    }

    // TODO: Use the same operator for both of them so that COUNT(*), MAX(col) can be optimized
    if (_queryContext.getFilter() == null && _indexSegment.getValidDocIds() == null) {
      Operator<IntermediateResultsBlock> resultsBlockOperator = getAggregationOperator(aggregationFunctions, numTotalDocs);

      if (resultsBlockOperator != null) {
        return resultsBlockOperator;
      }
    }

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(_queryContext)) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs, null,
                predicateEvaluatorsMap.keySet())) {
              TransformOperator transformOperator =
                  new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, null,
                      predicateEvaluatorsMap, _queryContext.getDebugOptions()).run();
              return new AggregationOperator(aggregationFunctions, transformOperator, numTotalDocs, true);
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);
    TransformOperator transformOperator = null;
    if (preComputedFilterOperator != null) {
      //TODO: Add a per segment context
      transformOperator =
          new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
              preComputedFilterOperator)
              .run();
    } else {
      transformOperator = new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform,
          DocIdSetPlanNode.MAX_DOC_PER_CALL)
          .run();
    }
    return new AggregationOperator(aggregationFunctions, transformOperator, numTotalDocs, false);
  }

  private Operator<IntermediateResultsBlock> getAggregationOperator(AggregationFunction[] aggregationFunctions, int numTotalDocs) {
    if (isFitForMetadataBasedPlan(aggregationFunctions)) {
      return new MetadataBasedAggregationOperator(aggregationFunctions, _indexSegment.getSegmentMetadata(),
          Collections.emptyMap());
    } else if (isFitForDictionaryBasedPlan(aggregationFunctions, _indexSegment)) {
      Map<String, Dictionary> dictionaryMap = new HashMap<>();
      for (AggregationFunction aggregationFunction : aggregationFunctions) {
        String column = ((ExpressionContext) aggregationFunction.getInputExpressions().get(0)).getIdentifier();
        dictionaryMap.computeIfAbsent(column, k -> _indexSegment.getDataSource(k).getDictionary());
      }
      return new DictionaryBasedAggregationOperator(aggregationFunctions, dictionaryMap, numTotalDocs);
    }

    return null;
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with segment metadata, {@code false} otherwise.
   * <p>Aggregations supported: COUNT
   */
  private static boolean isFitForMetadataBasedPlan(AggregationFunction[] aggregationFunctions) {
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (aggregationFunction.getType() != AggregationFunctionType.COUNT) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with dictionary, {@code false} otherwise.
   * <p>Aggregations supported: MIN, MAX, MIN_MAX_RANGE, DISTINCT_COUNT, SEGMENT_PARTITIONED_DISTINCT_COUNT
   */
  private static boolean isFitForDictionaryBasedPlan(AggregationFunction[] aggregationFunctions,
      IndexSegment indexSegment) {
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      AggregationFunctionType functionType = aggregationFunction.getType();
      if (functionType != AggregationFunctionType.MIN && functionType != AggregationFunctionType.MAX
          && functionType != AggregationFunctionType.MINMAXRANGE
          && functionType != AggregationFunctionType.DISTINCTCOUNT
          && functionType != AggregationFunctionType.SEGMENTPARTITIONEDDISTINCTCOUNT) {
        return false;
      }
      ExpressionContext argument = (ExpressionContext) aggregationFunction.getInputExpressions().get(0);
      if (argument.getType() != ExpressionContext.Type.IDENTIFIER) {
        return false;
      }
      String column = argument.getIdentifier();
      Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();
      if (dictionary == null) {
        return false;
      }
    }
    return true;
  }
}
