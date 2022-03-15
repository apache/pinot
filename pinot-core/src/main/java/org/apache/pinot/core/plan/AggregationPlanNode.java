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
import java.util.EnumSet;
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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;

import static org.apache.pinot.segment.spi.AggregationFunctionType.*;


/**
 * The <code>AggregationPlanNode</code> class provides the execution plan for aggregation only query on a single
 * segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationPlanNode implements PlanNode {
  private static final EnumSet<AggregationFunctionType> DICTIONARY_BASED_FUNCTIONS =
      EnumSet.of(MIN, MINMV, MAX, MAXMV, MINMAXRANGE, MINMAXRANGEMV, DISTINCTCOUNT, DISTINCTCOUNTMV, DISTINCTCOUNTHLL,
          DISTINCTCOUNTHLLMV, DISTINCTCOUNTRAWHLL, DISTINCTCOUNTRAWHLLMV, SEGMENTPARTITIONEDDISTINCTCOUNT,
          DISTINCTCOUNTSMARTHLL);

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public AggregationPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    assert _queryContext.getAggregationFunctions() != null;
    return _queryContext.isHasFilteredAggregations() ? buildFilteredAggOperator() : buildNonFilteredAggOperator();
  }

  /**
   * Build the operator to be used for filtered aggregations
   */
  private BaseOperator<IntermediateResultsBlock> buildFilteredAggOperator() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    // Build the operator chain for the main predicate
    Pair<FilterPlanNode, BaseFilterOperator> filterOperatorPair = buildFilterOperator(_queryContext.getFilter());
    TransformOperator transformOperator = buildTransformOperatorForFilteredAggregates(filterOperatorPair.getRight());

    return buildFilterOperatorInternal(filterOperatorPair.getRight(), transformOperator, numTotalDocs);
  }

  /**
   * Build a FilteredAggregationOperator given the parameters.
   * @param mainPredicateFilterOperator Filter operator corresponding to the main predicate
   * @param mainTransformOperator Transform operator corresponding to the main predicate
   * @param numTotalDocs Number of total docs
   */
  private BaseOperator<IntermediateResultsBlock> buildFilterOperatorInternal(
      BaseFilterOperator mainPredicateFilterOperator, TransformOperator mainTransformOperator, int numTotalDocs) {
    Map<FilterContext, Pair<List<AggregationFunction>, TransformOperator>> filterContextToAggFuncsMap = new HashMap<>();
    List<AggregationFunction> nonFilteredAggregationFunctions = new ArrayList<>();
    List<Pair<AggregationFunction, FilterContext>> aggregationFunctions =
        _queryContext.getFilteredAggregationFunctions();

    // For each aggregation function, check if the aggregation function is a filtered agg.
    // If it is, populate the corresponding filter operator and corresponding transform operator
    for (Pair<AggregationFunction, FilterContext> inputPair : aggregationFunctions) {
      if (inputPair.getLeft() != null) {
        FilterContext currentFilterExpression = inputPair.getRight();
        if (filterContextToAggFuncsMap.get(currentFilterExpression) != null) {
          filterContextToAggFuncsMap.get(currentFilterExpression).getLeft().add(inputPair.getLeft());
          continue;
        }
        Pair<FilterPlanNode, BaseFilterOperator> pair = buildFilterOperator(currentFilterExpression);
        BaseFilterOperator wrappedFilterOperator =
            new CombinedFilterOperator(mainPredicateFilterOperator, pair.getRight());
        TransformOperator newTransformOperator = buildTransformOperatorForFilteredAggregates(wrappedFilterOperator);
        // For each transform operator, associate it with the underlying expression. This allows
        // fetching the relevant TransformOperator when resolving blocks during aggregation
        // execution
        List<AggregationFunction> aggFunctionList = new ArrayList<>();
        aggFunctionList.add(inputPair.getLeft());
        filterContextToAggFuncsMap.put(currentFilterExpression, Pair.of(aggFunctionList, newTransformOperator));
      } else {
        nonFilteredAggregationFunctions.add(inputPair.getLeft());
      }
    }
    List<Pair<AggregationFunction[], TransformOperator>> aggToTransformOpList = new ArrayList<>();
    // Convert to array since FilteredAggregationOperator expects it
    for (Pair<List<AggregationFunction>, TransformOperator> pair : filterContextToAggFuncsMap.values()) {
      List<AggregationFunction> aggregationFunctionList = pair.getLeft();
      if (aggregationFunctionList == null) {
        throw new IllegalStateException("Null aggregation list seen");
      }
      aggToTransformOpList.add(Pair.of(aggregationFunctionList.toArray(new AggregationFunction[0]), pair.getRight()));
    }
    aggToTransformOpList.add(
        Pair.of(nonFilteredAggregationFunctions.toArray(new AggregationFunction[0]), mainTransformOperator));

    return new FilteredAggregationOperator(_queryContext.getAggregationFunctions(), aggToTransformOpList, numTotalDocs);
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

  private TransformOperator buildTransformOperatorForFilteredAggregates(BaseFilterOperator filterOperator) {
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);

    return new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform,
        DocIdSetPlanNode.MAX_DOC_PER_CALL, filterOperator).run();
  }

  /**
   * Processing workhorse for non filtered aggregates. Note that this code path is invoked only
   * if the query has no filtered aggregates at all. If a query has mixed aggregates, filtered
   * aggregates code will be invoked
   */
  public Operator<IntermediateResultsBlock> buildNonFilteredAggOperator() {
    assert _queryContext.getAggregationFunctions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    // Use metadata/dictionary to solve the query if possible
    // TODO: Use the same operator for both of them so that COUNT(*), MAX(col) can be optimized
    if (filterOperator.isResultMatchingAll()) {
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
    }

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(_queryContext)) {
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
    TransformOperator transformOperator =
        new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator).run();
    return new AggregationOperator(aggregationFunctions, transformOperator, numTotalDocs, false);
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with segment metadata, {@code false} otherwise.
   * <p>Aggregations supported: COUNT
   */
  private static boolean isFitForMetadataBasedPlan(AggregationFunction[] aggregationFunctions) {
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (aggregationFunction.getType() != COUNT) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with dictionary, {@code false} otherwise.
   */
  private static boolean isFitForDictionaryBasedPlan(AggregationFunction[] aggregationFunctions,
      IndexSegment indexSegment) {
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (!DICTIONARY_BASED_FUNCTIONS.contains(aggregationFunction.getType())) {
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
