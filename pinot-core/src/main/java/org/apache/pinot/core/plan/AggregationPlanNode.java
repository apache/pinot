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
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.EmptyAggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;

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
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;

  public AggregationPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
  }

  @Override
  public Operator<AggregationResultsBlock> run() {
    assert _queryContext.getAggregationFunctions() != null;

    if (_queryContext.getLimit() == 0) {
      return new EmptyAggregationOperator(_queryContext, _indexSegment.getSegmentMetadata().getTotalDocs());
    }

    return _queryContext.hasFilteredAggregations() ? buildFilteredAggOperator() : buildNonFilteredAggOperator();
  }

  /**
   * Build the operator to be used for filtered aggregations
   */
  private FilteredAggregationOperator buildFilteredAggOperator() {
    return new FilteredAggregationOperator(_queryContext,
        AggregationFunctionUtils.buildFilteredAggregationInfos(_segmentContext, _queryContext),
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
    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    boolean hasNullValues = _queryContext.isNullHandlingEnabled() && hasNullValues(aggregationFunctions);
    if (!hasNullValues) {
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

      if (!QueryOptionsUtils.isDisableFastFilteredCount(_queryContext.getQueryOptions()) &&
          canOptimizeFilteredCount(filterOperator, aggregationFunctions)) {
        return new FastFilteredCountOperator(_queryContext, filterOperator, _indexSegment.getSegmentMetadata());
      }
    }

    AggregationInfo aggregationInfo =
        AggregationFunctionUtils.buildAggregationInfo(_segmentContext, _queryContext, aggregationFunctions,
            _queryContext.getFilter(), filterOperator, filterPlanNode.getPredicateEvaluators());
    return new AggregationOperator(_queryContext, aggregationInfo, numTotalDocs);
  }

  /**
   * Returns {@code true} if any of the aggregation functions have null values, {@code false} otherwise.
   *
   * The current implementation is pessimistic and returns {@code true} if any of the arguments to the aggregation
   * functions is of function type. This is because we do not have a way to determine if the function will return null
   * values without actually evaluating it.
   */
  private boolean hasNullValues(AggregationFunction[] aggregationFunctions) {
    for (AggregationFunction<?, ?> aggregationFunction : aggregationFunctions) {
      for (ExpressionContext argument : aggregationFunction.getInputExpressions()) {
        switch (argument.getType()) {
          case IDENTIFIER:
            DataSource dataSource = _indexSegment.getDataSource(argument.getIdentifier());
            NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
            if (nullValueVector != null && !nullValueVector.getNullBitmap().isEmpty()) {
              return true;
            }
            break;
          case LITERAL:
            if (argument.getLiteral().isNull()) {
              return true;
            }
            break;
          case FUNCTION:
          default:
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if the given aggregations can be solved with dictionary or column metadata, {@code false}
   * otherwise.
   */
  private static boolean isFitForNonScanBasedPlan(AggregationFunction[] aggregationFunctions,
      IndexSegment indexSegment) {
    for (AggregationFunction<?, ?> aggregationFunction : aggregationFunctions) {
      if (aggregationFunction.getType() == COUNT) {
        continue;
      }
      ExpressionContext argument = aggregationFunction.getInputExpressions().get(0);
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
