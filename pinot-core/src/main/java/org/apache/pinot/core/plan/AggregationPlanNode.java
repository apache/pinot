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

  // MIN/MAX/MINMAXRANGE derive their result numerically from the column min/max, so they can only be resolved from
  // metadata/dictionary for numeric columns. Non-numeric columns (e.g. BYTES) store min/max as raw values that cannot
  // be parsed as numbers.
  private static final EnumSet<AggregationFunctionType> NUMERIC_METADATA_FUNCTIONS =
      EnumSet.of(MIN, MINMV, MINLONG, MAX, MAXMV, MAXLONG, MINMAXRANGE, MINMAXRANGEMV);

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
      // when the filter matches all documents, resolve as many functions as possible from the column
      // dictionary/metadata without scanning the segment. Eligibility is evaluated once per function here
      // and reused for both the fully non-scan path (all functions resolvable) and
      // the partial path (some functions resolvable).
      if (filterOperator.isResultMatchingAll()) {
        boolean[] metadataResolvable = new boolean[aggregationFunctions.length];
        DataSource[] dataSources = new DataSource[aggregationFunctions.length];
        int numResolved = 0;
        for (int i = 0; i < aggregationFunctions.length; i++) {
          DataSource dataSource = getDataSourceForAggregationFunction(aggregationFunctions[i]);
          if (isFitForNonScanBasedPlan(aggregationFunctions[i], dataSource)) {
            metadataResolvable[i] = true;
            dataSources[i] = dataSource;
            numResolved++;
          }
        }

        if (numResolved == aggregationFunctions.length) {
          // Priority 2: all functions can be resolved from dictionary/metadata -> fully non-scan based execution
          return new NonScanBasedAggregationOperator(_queryContext, dataSources, numTotalDocs);
        }
        if (numResolved > 0) {
          // some functions can be resolved from dictionary/metadata; the rest fall back to scan-based
          // execution in the AggregationOperator.
          aggregationInfo = AggregationFunctionUtils.buildAggregationInfoWithoutStarTree(_segmentContext, _queryContext,
              aggregationFunctions, filterOperator);
          return new AggregationOperator(_queryContext, aggregationInfo, numTotalDocs, metadataResolvable, dataSources);
        }
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
          default:
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if the given aggregation function can be resolved from the column dictionary or metadata
   * (without scanning the segment), {@code false} otherwise. {@code COUNT} is always eligible. Functions whose result
   * is derived numerically from the column min/max (e.g. MIN, MAX, MINMAXRANGE) are only eligible for numeric columns,
   * since non-numeric columns (e.g. BYTES) store min/max as raw values that cannot be parsed as numbers.
   *
   * @param aggregationFunction aggregation function to test
   * @param dataSource the function argument's data source (see {@link #getDataSourceForAggregationFunction})
   */
  private boolean isFitForNonScanBasedPlan(AggregationFunction<?, ?> aggregationFunction,
      @Nullable DataSource dataSource) {
    AggregationFunctionType functionType = aggregationFunction.getType();
    if (functionType == COUNT) {
      return true;
    }

    if (dataSource == null) {
      // Aggregation function does not have a single identifier argument (e.g. COUNT(*) or COUNT(1)),
      // so it cannot be resolved from metadata
      return false;
    }

    // MIN/MAX/MINMAXRANGE derive their result numerically from the column min/max, which is only valid for numeric
    // columns. Non-numeric columns (e.g. BYTES) store min/max as raw values that cannot be parsed as numbers.
    if (NUMERIC_METADATA_FUNCTIONS.contains(functionType)
        && !dataSource.getDataSourceMetadata().getDataType().getStoredType().isNumeric()) {
      return false;
    }

    if (dataSource.getDictionary() != null && DICTIONARY_BASED_FUNCTIONS.contains(functionType)) {
      return true;
    }

    return METADATA_BASED_FUNCTIONS.contains(functionType)
        && dataSource.getDataSourceMetadata().getMaxValue() != null
        && dataSource.getDataSourceMetadata().getMinValue() != null;
  }

  private static boolean canOptimizeFilteredCount(BaseFilterOperator filterOperator,
      AggregationFunction[] aggregationFunctions) {
    return (aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == COUNT)
        && filterOperator.canOptimizeCount();
  }

  /**
   * Returns the data source for the given aggregation function's argument, or {@code null} if the function has no
   * argument (e.g. {@code COUNT(*)}) or its argument is not a single column identifier (e.g. {@code COUNT(1)} or a
   * transform expression), in which case it cannot be resolved from dictionary/metadata.
   *
   * @param aggregationFunction aggregation function whose argument data source is resolved
   * @return the argument's data source, or {@code null} if it has no single identifier argument
   */
  @Nullable
  private DataSource getDataSourceForAggregationFunction(AggregationFunction<?, ?> aggregationFunction) {
    List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
    if (!inputExpressions.isEmpty()) {
      ExpressionContext argument = inputExpressions.get(0);
      if (argument.getType() != ExpressionContext.Type.IDENTIFIER) {
        return null;
      }
      return _indexSegment.getDataSource(argument.getIdentifier(), _queryContext.getSchema());
    }

    return null;
  }
}
