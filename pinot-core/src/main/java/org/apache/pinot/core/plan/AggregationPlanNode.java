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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.EmptyAggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.operator.transform.function.ItemTransformFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;

import static org.apache.pinot.segment.spi.AggregationFunctionType.*;


/**
 * The <code>AggregationPlanNode</code> class provides the execution plan for aggregation only query on a single
 * segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationPlanNode implements PlanNode {
  private static final EnumSet<AggregationFunctionType> DICTIONARY_BASED_FUNCTIONS =
      EnumSet.of(MIN, MINMV, MINLONG, MINSTRING, MAX, MAXMV, MAXLONG, MAXSTRING, MINMAXRANGE, MINMAXRANGEMV,
          DISTINCTCOUNT, DISTINCTCOUNTMV, DISTINCTSUM, DISTINCTSUMMV, DISTINCTAVG, DISTINCTAVGMV, DISTINCTCOUNTOFFHEAP,
          DISTINCTCOUNTHLL, DISTINCTCOUNTHLLMV, DISTINCTCOUNTRAWHLL, DISTINCTCOUNTRAWHLLMV, DISTINCTCOUNTHLLPLUS,
          DISTINCTCOUNTHLLPLUSMV, DISTINCTCOUNTRAWHLLPLUS, DISTINCTCOUNTRAWHLLPLUSMV, DISTINCTCOUNTULL,
          DISTINCTCOUNTRAWULL, SEGMENTPARTITIONEDDISTINCTCOUNT, DISTINCTCOUNTSMARTHLL, DISTINCTCOUNTSMARTHLLPLUS,
          DISTINCTCOUNTSMARTULL);

  // DISTINCTCOUNT excluded because consuming segment metadata contains unknown cardinality when there is no dictionary
  // MINSTRING / MAXSTRING excluded because of string column metadata issues (see discussion in
  // https://github.com/apache/pinot/pull/16983)
  private static final EnumSet<AggregationFunctionType> METADATA_BASED_FUNCTIONS =
      EnumSet.of(COUNT, MIN, MINMV, MINLONG, MAX, MAXMV, MAXLONG, MINMAXRANGE, MINMAXRANGEMV);

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

    // Priority 1: Check if star-tree based aggregation is feasible
    AggregationInfo aggregationInfo = AggregationFunctionUtils.buildAggregationInfoWithStarTree(_segmentContext,
        _queryContext, aggregationFunctions, _queryContext.getFilter(), filterOperator,
        filterPlanNode.getPredicateEvaluators());
    if (aggregationInfo != null) {
      return new AggregationOperator(_queryContext, aggregationInfo, numTotalDocs);
    }

    boolean hasNullValues = _queryContext.isNullHandlingEnabled() && hasNullValues(aggregationFunctions);
    if (!hasNullValues) {
      // Priority 2: Check if non-scan based aggregation is feasible
      if (filterOperator.isResultMatchingAll() && isFitForNonScanBasedPlan()) {
        DataSource[] dataSources = new DataSource[aggregationFunctions.length];
        for (int i = 0; i < aggregationFunctions.length; i++) {
          List<?> inputExpressions = aggregationFunctions[i].getInputExpressions();
          if (!inputExpressions.isEmpty()) {
            ExpressionContext expr = (ExpressionContext) inputExpressions.get(0);
            DataSource ds = resolveDataSource(expr);
            if (ds != null) {
              dataSources[i] = ds;
            }
          }
        }
        return new NonScanBasedAggregationOperator(_queryContext, dataSources, numTotalDocs);
      }

      // Priority 3: Check if fast filtered count can be used
      if (canOptimizeFilteredCount(filterOperator, aggregationFunctions)) {
        return new FastFilteredCountOperator(_queryContext, filterOperator, _indexSegment.getSegmentMetadata());
      }
    }

    // Default:
    aggregationInfo = AggregationFunctionUtils.buildAggregationInfoWithoutStarTree(_segmentContext, _queryContext,
        aggregationFunctions, filterOperator);
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
            DataSource dataSource = _indexSegment.getDataSource(argument.getIdentifier(), _queryContext.getSchema());
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
            DataSource resolvedDs = resolveDataSource(argument);
            if (resolvedDs == null) {
              return true;
            }
            NullValueVectorReader resolvedNullVector = resolvedDs.getNullValueVector();
            if (resolvedNullVector != null && !resolvedNullVector.getNullBitmap().isEmpty()) {
              return true;
            }
            break;
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
  private boolean isFitForNonScanBasedPlan() {
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    for (AggregationFunction<?, ?> aggregationFunction : aggregationFunctions) {
      if (aggregationFunction.getType() == COUNT) {
        continue;
      }
      ExpressionContext argument = aggregationFunction.getInputExpressions().get(0);
      DataSource dataSource = resolveDataSource(argument);
      if (dataSource == null) {
        return false;
      }
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

  @Nullable
  private DataSource resolveDataSource(ExpressionContext expression) {
    return resolveDataSource(expression, _indexSegment, _queryContext.getSchema());
  }

  @Nullable
  static DataSource resolveDataSource(ExpressionContext expression, IndexSegment segment,
      @Nullable org.apache.pinot.spi.data.Schema schema) {
    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      return segment.getDataSource(expression.getIdentifier(), schema);
    }
    if (expression.getType() == ExpressionContext.Type.FUNCTION) {
      return tryResolveKeyedDataSource(expression, segment, schema);
    }
    return null;
  }

  @Nullable
  static DataSource tryResolveKeyedDataSource(ExpressionContext expression, IndexSegment segment,
      @Nullable org.apache.pinot.spi.data.Schema schema) {
    FunctionContext function = expression.getFunction();
    if (function == null
        || !ItemTransformFunction.FUNCTION_NAME.equals(function.getFunctionName())) {
      return null;
    }
    List<ExpressionContext> args = function.getArguments();
    if (args.size() != 2
        || args.get(0).getType() != ExpressionContext.Type.IDENTIFIER
        || args.get(1).getType() != ExpressionContext.Type.LITERAL) {
      return null;
    }
    String columnName = args.get(0).getIdentifier();
    String key = args.get(1).getLiteral().getStringValue();
    DataSource columnDs = segment.getDataSource(columnName, schema);
    if (columnDs instanceof MapDataSource) {
      return ((MapDataSource) columnDs).getDataSource(key);
    }
    if (columnDs instanceof OpenStructDataSource) {
      OpenStructDataSource osDs = (OpenStructDataSource) columnDs;
      return osDs.isMaterialized(key) ? osDs.getDataSource(key) : null;
    }
    return null;
  }

  private static boolean canOptimizeFilteredCount(BaseFilterOperator filterOperator,
      AggregationFunction[] aggregationFunctions) {
    return (aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == COUNT)
        && filterOperator.canOptimizeCount();
  }
}
