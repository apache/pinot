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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;

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
    return new FilteredAggregationOperator(_queryContext,
        AggregationFunctionUtils.buildFilteredAggregationInfos(_indexSegment, _queryContext),
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  /**
   * Processing workhorse for non filtered aggregates. Note that this code path is invoked only
   * if the query has no filtered aggregates at all. If a query has mixed aggregates, filtered
   * aggregates code will be invoked
   */
  public Operator<AggregationResultsBlock> buildNonFilteredAggOperator() {
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    if (!_queryContext.isNullHandlingEnabled()) {
      if (canOptimizeFilteredCount(filterOperator, aggregationFunctions)) {
        return new FastFilteredCountOperator(_queryContext, filterOperator, _indexSegment.getSegmentMetadata());
      }

      if (filterOperator.isResultMatchingAll() && isFitForNonScanBasedPlan(aggregationFunctions, _indexSegment)) {
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

    AggregationInfo aggregationInfo =
        AggregationFunctionUtils.buildAggregationInfo(_indexSegment, _queryContext, aggregationFunctions,
            _queryContext.getFilter(), filterOperator, filterPlanNode.getPredicateEvaluators());
    return new AggregationOperator(_queryContext, aggregationInfo, numTotalDocs);
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
