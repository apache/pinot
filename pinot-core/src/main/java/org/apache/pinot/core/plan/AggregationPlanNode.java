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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeProjectPlanNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
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
          DISTINCTCOUNTSMARTHLL, DISTINCTSUM, DISTINCTAVG, DISTINCTSUMMV, DISTINCTAVGMV, DISTINCTCOUNTHLLPLUS,
          DISTINCTCOUNTHLLPLUSMV, DISTINCTCOUNTRAWHLLPLUS, DISTINCTCOUNTRAWHLLPLUSMV);

  // DISTINCTCOUNT excluded because consuming segment metadata contains unknown cardinality when there is no dictionary
  private static final EnumSet<AggregationFunctionType> METADATA_BASED_FUNCTIONS =
      EnumSet.of(COUNT, MIN, MINMV, MAX, MAXMV, MINMAXRANGE, MINMAXRANGEMV);

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public AggregationPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<AggregationResultsBlock> run() {
    assert _queryContext.getAggregationFunctions() != null;
    return _queryContext.hasFilteredAggregations() ? buildFilteredAggOperator() : buildNonFilteredAggOperator();
  }

  /**
   * Build the operator to be used for filtered aggregations
   */
  private FilteredAggregationOperator buildFilteredAggOperator() {
    List<Pair<AggregationFunction[], BaseProjectOperator<?>>> projectOperators =
        AggregationFunctionUtils.buildFilteredAggregateProjectOperators(_indexSegment, _queryContext);
    return new FilteredAggregationOperator(_queryContext, projectOperators,
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  /**
   * Processing workhorse for non filtered aggregates. Note that this code path is invoked only
   * if the query has no filtered aggregates at all. If a query has mixed aggregates, filtered
   * aggregates code will be invoked
   */
  public Operator<AggregationResultsBlock> buildNonFilteredAggOperator() {
    assert _queryContext.getAggregationFunctions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    if (canOptimizeFilteredCount(filterOperator, aggregationFunctions) && !_queryContext.isNullHandlingEnabled()) {
      return new FastFilteredCountOperator(_queryContext, filterOperator, _indexSegment.getSegmentMetadata());
    }

    if (filterOperator.isResultMatchingAll() && !_queryContext.isNullHandlingEnabled()) {
      if (isFitForNonScanBasedPlan(aggregationFunctions, _indexSegment)) {
        DataSource[] dataSources = new DataSource[aggregationFunctions.length];
        for (int i = 0; i < aggregationFunctions.length; i++) {
          List<?> inputExpressions = aggregationFunctions[i].getInputExpressions();
          if (!inputExpressions.isEmpty()) {
            String column = ((ExpressionContext) inputExpressions.get(0)).getIdentifier();
            dataSources[i] = _indexSegment.getDataSource(column);
          }
        }
        return new NonScanBasedAggregationOperator(_queryContext, dataSources, numTotalDocs);
      }
    }

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !_queryContext.isSkipStarTree() && !_queryContext.isNullHandlingEnabled()) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter(),
                filterPlanNode.getPredicateEvaluators());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs, null,
                predicateEvaluatorsMap.keySet())) {
              BaseProjectOperator<?> projectOperator =
                  new StarTreeProjectPlanNode(_queryContext, starTreeV2, aggregationFunctionColumnPairs, null,
                      predicateEvaluatorsMap).run();
              return new AggregationOperator(_queryContext, projectOperator, numTotalDocs, true);
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, null);
    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator).run();
    return new AggregationOperator(_queryContext, projectOperator, numTotalDocs, false);
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with dictionary or column metadata, {@code false}
   * otherwise.
   */
  private static boolean isFitForNonScanBasedPlan(AggregationFunction[] aggregationFunctions,
      IndexSegment indexSegment) {
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (aggregationFunction.getType() == COUNT) {
        continue;
      }
      ExpressionContext argument = (ExpressionContext) aggregationFunction.getInputExpressions().get(0);
      if (argument.getType() != ExpressionContext.Type.IDENTIFIER) {
        return false;
      }
      DataSource dataSource = indexSegment.getDataSource(argument.getIdentifier());
      if (DICTIONARY_BASED_FUNCTIONS.contains(aggregationFunction.getType())) {
        if (dataSource.getDictionary() != null) {
          continue;
        }
      }
      if (METADATA_BASED_FUNCTIONS.contains(aggregationFunction.getType())) {
        if (dataSource.getDataSourceMetadata().getMaxValue() != null
            && dataSource.getDataSourceMetadata().getMinValue() != null) {
          continue;
        }
      }
      return false;
    }
    return true;
  }

  private static boolean canOptimizeFilteredCount(BaseFilterOperator filterOperator,
      AggregationFunction[] aggregationFunctions) {
    return (aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == COUNT)
        && filterOperator.canOptimizeCount();
  }
}
