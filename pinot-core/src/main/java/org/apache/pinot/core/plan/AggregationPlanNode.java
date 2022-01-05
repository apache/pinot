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
import org.apache.pinot.core.operator.filter.CombinedFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.MetadataBasedAggregationOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.FilterableAggregationFunction;
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

    boolean hasFilteredPredicates = _queryContext.isHasFilteredAggregations();

    Pair<FilterPlanNode, BaseFilterOperator> filterOperatorPair =
        buildFilterOperator(_queryContext.getFilter());

    Pair<TransformOperator,
        BaseOperator<IntermediateResultsBlock>> pair =
        buildOperators(filterOperatorPair.getRight(), filterOperatorPair.getLeft());

    if (hasFilteredPredicates) {
      int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
      AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

      return buildOperatorForFilteredAggregations(filterOperatorPair.getRight(), pair.getLeft(),
          aggregationFunctions,
          numTotalDocs);
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

  /**
   * Build a FilteredAggregationOperator given the parameters.
   * @param mainPredicateFilterOperator Filter operator corresponding to the main predicate
   * @param mainTransformOperator Transform operator corresponding to the main predicate
   * @param aggregationFunctions Aggregation functions in the query
   * @param numTotalDocs Number of total docs
   */
  private BaseOperator<IntermediateResultsBlock> buildOperatorForFilteredAggregations(
      BaseFilterOperator mainPredicateFilterOperator,
      TransformOperator mainTransformOperator,
      AggregationFunction[] aggregationFunctions, int numTotalDocs) {
    Map<ExpressionContext, Pair<List<AggregationFunction>, TransformOperator>> expressionContextToAggFuncsMap =
        new HashMap<>();
    List<AggregationFunction> nonFilteredAggregationFunctions = new ArrayList<>();

    // For each aggregation function, check if the aggregation function is a filtered agg.
    // If it is, populate the corresponding filter operator and corresponding transform operator
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (aggregationFunction instanceof FilterableAggregationFunction) {
        FilterableAggregationFunction filterableAggregationFunction =
            (FilterableAggregationFunction) aggregationFunction;

        ExpressionContext currentFilterExpression = filterableAggregationFunction
            .getAssociatedExpressionContext();

        if (expressionContextToAggFuncsMap.containsKey(currentFilterExpression)) {
          expressionContextToAggFuncsMap.get(currentFilterExpression).getLeft().add(aggregationFunction);
          continue;
        }

        Pair<FilterPlanNode, BaseFilterOperator> pair =
            buildFilterOperator(filterableAggregationFunction.getFilterContext());

        BaseFilterOperator wrappedFilterOperator = new CombinedFilterOperator(mainPredicateFilterOperator,
            pair.getRight());

        Pair<TransformOperator,
            BaseOperator<IntermediateResultsBlock>> innerPair =
            buildOperators(wrappedFilterOperator, pair.getLeft());


        // For each transform operator, associate it with the underlying expression. This allows
        // fetching the relevant TransformOperator when resolving blocks during aggregation
        // execution

        List aggFunctionList = new ArrayList<>();

        aggFunctionList.add(aggregationFunction);

        expressionContextToAggFuncsMap.put(currentFilterExpression,
            Pair.of(aggFunctionList, innerPair.getLeft()));
      } else {
        nonFilteredAggregationFunctions.add(aggregationFunction);
      }
    }

    List<Pair<AggregationFunction[], TransformOperator>> aggToTransformOpList =
        new ArrayList<>();

    // Convert to array since FilteredAggregationOperator expects it
    for (Pair<List<AggregationFunction>, TransformOperator> pair
        : expressionContextToAggFuncsMap.values()) {
      List<AggregationFunction> aggregationFunctionList = pair.getLeft();

      if (aggregationFunctionList == null) {
        throw new IllegalStateException("Null aggregation list seen");
      }

      aggToTransformOpList.add(Pair.of(aggregationFunctionList.toArray(new AggregationFunction[0]),
          pair.getRight()));
    }

    aggToTransformOpList.add(Pair.of(nonFilteredAggregationFunctions.toArray(new AggregationFunction[0]),
        mainTransformOperator));

    return new FilteredAggregationOperator(aggregationFunctions, aggToTransformOpList,
        numTotalDocs);
  }

  /**
   * Build a filter operator from the given FilterContext.
   *
   * It returns the FilterPlanNode to allow reusing plan level components such as predicate
   * evaluator map
   */
  private Pair<FilterPlanNode, BaseFilterOperator> buildFilterOperator(FilterContext filterContext) {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext, filterContext);

    return Pair.of(filterPlanNode, filterPlanNode.run());
  }

  /**
   * Build transform and aggregation operators for the given bottom level plan
   * @param filterOperator Filter operator to be used in the corresponding chain
   * @param filterPlanNode Plan node associated with the filter operator
   * @return Pair, consisting of the built TransformOperator and Aggregation operator for chain
   */
  private Pair<TransformOperator,
      BaseOperator<IntermediateResultsBlock>> buildOperators(BaseFilterOperator filterOperator,
      FilterPlanNode filterPlanNode) {
    assert _queryContext.getAggregationFunctions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);
    boolean hasFilteredPredicates = _queryContext.isHasFilteredAggregations();

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
        return Pair.of(null,
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
                  transformOperator, numTotalDocs, true);

              return Pair.of(transformOperator, aggregationOperator);
            }
          }
        }
      }
    }

    TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext,
          expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL, filterOperator).run();
    AggregationOperator aggregationOperator = new AggregationOperator(aggregationFunctions,
        transformOperator, numTotalDocs, false);

    return Pair.of(transformOperator, aggregationOperator);
  }
}
