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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BlockDrivenAndFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.operator.query.MetadataBasedAggregationOperator;
import org.apache.pinot.core.operator.transform.CombinedTransformOperator;
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

    boolean hasFilteredPredicates = _queryContext.getFilteredAggregationContexts()
        .size() != 0;

    Pair<Pair<BaseFilterOperator, TransformOperator>,
        BaseOperator<IntermediateResultsBlock>> pair =
        buildOperators(_queryContext.getFilter(), false);

    if (hasFilteredPredicates) {
      int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
      AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

      Set<ExpressionContext> expressionsToTransform =
          AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);
      BaseOperator<IntermediateResultsBlock> aggregationOperator = pair.getRight();

      assert pair != null && aggregationOperator != null;

      //TODO: For non star tree, non filtered aggregation, share the main filter transform operator
      TransformOperator filteredTransformOperator = buildOperatorForFilteredAggregations(pair.getLeft()
              .getRight(), pair.getLeft().getLeft(), expressionsToTransform);
      return new AggregationOperator(aggregationFunctions, filteredTransformOperator, numTotalDocs,
          false, true);
    }

    return pair.getRight();
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

  private TransformOperator buildOperatorForFilteredAggregations(TransformOperator nonFilteredTransformOperator,
      BaseFilterOperator filterOperator,
      Set<ExpressionContext> expressionsToTransform) {
    List<TransformOperator> transformOperatorList = new ArrayList<>();
    Map<ExpressionContext, FilterContext> aggregationFunctionFilterContextsMap =
        _queryContext.getFilteredAggregationContexts();

    Iterator<Map.Entry<ExpressionContext, FilterContext>> iterator =
        aggregationFunctionFilterContextsMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<ExpressionContext, FilterContext> entry = iterator.next();
      Pair<Pair<BaseFilterOperator, TransformOperator>,
          BaseOperator<IntermediateResultsBlock>> pair =
          buildOperators(entry.getValue(), true);

      transformOperatorList.add(pair.getLeft().getRight());
    }

    return new CombinedTransformOperator(transformOperatorList, nonFilteredTransformOperator,
        filterOperator, expressionsToTransform);
  }

  private Pair<Pair<BaseFilterOperator, TransformOperator>,
      BaseOperator<IntermediateResultsBlock>> buildOperators(FilterContext filterContext,
      boolean isFilterPredicate) {
    assert _queryContext.getAggregationFunctions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext, filterContext);
    BaseFilterOperator filterOperator;

    if (isFilterPredicate) {
      filterOperator = new BlockDrivenAndFilterOperator(filterPlanNode.run(), numTotalDocs);
    } else {
      filterOperator = filterPlanNode.run();
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);
    boolean hasFilteredPredicates = _queryContext.getFilteredAggregationContexts()
        .size() != 0;

    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();

    // Use metadata/dictionary to solve the query if possible
    // TODO: Use the same operator for both of them so that COUNT(*), MAX(col) can be optimized
    if (filterOperator.isResultMatchingAll() && !hasFilteredPredicates) {
      if (isFitForMetadataBasedPlan(aggregationFunctions)) {
        return Pair.of(null, new MetadataBasedAggregationOperator(aggregationFunctions,
            _indexSegment.getSegmentMetadata(), Collections.emptyMap()));
      } else if (isFitForDictionaryBasedPlan(aggregationFunctions, _indexSegment)) {
        Map<String, Dictionary> dictionaryMap = new HashMap<>();
        for (AggregationFunction aggregationFunction : aggregationFunctions) {
          String column = ((ExpressionContext) aggregationFunction.getInputExpressions().get(0)).getIdentifier();
          dictionaryMap.computeIfAbsent(column, k -> _indexSegment.getDataSource(k).getDictionary());
        }
        return Pair.of(Pair.of(filterOperator, null),
            new DictionaryBasedAggregationOperator(aggregationFunctions, dictionaryMap,
            numTotalDocs));
      }
    }

    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(_queryContext)) {
      // Use star-tree to solve the query if possible

      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter(),
                filterPlanNode.getPredicateEvaluatorMap());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs, null,
                predicateEvaluatorsMap.keySet())) {

              TransformOperator transformOperator = new StarTreeTransformPlanNode(starTreeV2,
                  aggregationFunctionColumnPairs, null,
                      predicateEvaluatorsMap, null, _queryContext.getDebugOptions()).run();
              AggregationOperator aggregationOperator = new AggregationOperator(aggregationFunctions,
                  transformOperator, numTotalDocs,
                  true, false);

              return Pair.of(Pair.of(filterOperator, transformOperator), aggregationOperator);
            }
          }
        }
      }
    }

    TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext,
          expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL, filterOperator).run();
    AggregationOperator aggregationOperator = new AggregationOperator(aggregationFunctions,
        transformOperator, numTotalDocs, false, false);

    return Pair.of(Pair.of(filterOperator, transformOperator), aggregationOperator);
  }
}
