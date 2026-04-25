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
package org.apache.pinot.core.query.aggregation.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.CombinedFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationFunctionUtils {

  private AggregationFunctionUtils() {
  }

  /**
   * (For Star-Tree) Creates an {@link AggregationFunctionColumnPair} in stored type from the
   * {@link AggregationFunction}. Returns {@code null} if the {@link AggregationFunction} cannot be represented as an
   * {@link AggregationFunctionColumnPair} (e.g. has multiple arguments, argument is not column etc.).
   */
  @Nullable
  public static AggregationFunctionColumnPair getStoredFunctionColumnPair(AggregationFunction aggregationFunction) {
    AggregationFunctionType functionType = aggregationFunction.getType();
    if (functionType == AggregationFunctionType.COUNT) {
      return AggregationFunctionColumnPair.COUNT_STAR;
    }
    List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
    if (inputExpressions.size() == 1) {
      ExpressionContext inputExpression = inputExpressions.get(0);
      if (inputExpression.getType() == ExpressionContext.Type.IDENTIFIER) {
        return new AggregationFunctionColumnPair(AggregationFunctionColumnPair.getStoredType(functionType),
            inputExpression.getIdentifier());
      }
    }
    return null;
  }

  /**
   * Collects all transform expressions required for aggregation/group-by queries.
   * <p>NOTE: We don't need to consider order-by columns here as the ordering is only allowed for aggregation functions
   *          or group-by expressions.
   */
  public static Set<ExpressionContext> collectExpressionsToTransform(AggregationFunction[] aggregationFunctions,
      @Nullable List<ExpressionContext> groupByExpressions) {
    Set<ExpressionContext> expressions = new HashSet<>();
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      expressions.addAll(aggregationFunction.getInputExpressions());
    }
    if (groupByExpressions != null) {
      expressions.addAll(groupByExpressions);
    }
    return expressions;
  }

  /**
   * Creates a map from expression required by the {@link AggregationFunction} to {@link BlockValSet} fetched from the
   * {@link ValueBlock}.
   */
  public static Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggregationFunction,
      ValueBlock valueBlock) {
    //noinspection unchecked
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Map.of();
    }
    if (numExpressions == 1) {
      ExpressionContext expression = expressions.get(0);
      return Map.of(expression, valueBlock.getBlockValueSet(expression));
    }
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (ExpressionContext expression : expressions) {
      blockValSetMap.put(expression, valueBlock.getBlockValueSet(expression));
    }
    return blockValSetMap;
  }

  /**
   * (For Star-Tree) Creates a map from expression required by the {@link AggregationFunctionColumnPair} to
   * {@link BlockValSet} fetched from the {@link ValueBlock}.
   * <p>NOTE: We construct the map with original column name as the key but fetch BlockValSet with the aggregation
   *          function pair so that the aggregation result column name is consistent with or without star-tree.
   */
  public static Map<ExpressionContext, BlockValSet> getBlockValSetMap(
      AggregationFunctionColumnPair aggregationFunctionColumnPair, ValueBlock valueBlock) {
    ExpressionContext expression = ExpressionContext.forIdentifier(aggregationFunctionColumnPair.getColumn());
    BlockValSet blockValSet = valueBlock.getBlockValueSet(aggregationFunctionColumnPair.toColumnName());
    return Map.of(expression, blockValSet);
  }

  /**
   * Reads the intermediate result from the {@link DataTable}.
   */
  @Nullable
  public static Object getIntermediateResult(AggregationFunction aggregationFunction, DataTable dataTable,
      ColumnDataType columnDataType, int rowId, int colId) {
    switch (columnDataType.getStoredType()) {
      case INT:
        return dataTable.getInt(rowId, colId);
      case LONG:
        return dataTable.getLong(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case STRING:
        return dataTable.getString(rowId, colId);
      case FLOAT:
        return dataTable.getFloat(rowId, colId);
      case BIG_DECIMAL:
        return dataTable.getBigDecimal(rowId, colId);
      case BYTES:
        return dataTable.getBytes(rowId, colId);
      case OBJECT:
        CustomObject customObject = dataTable.getCustomObject(rowId, colId);
        return customObject != null ? aggregationFunction.deserializeIntermediateResult(customObject) : null;
      default:
        throw new IllegalStateException("Illegal column data type in intermediate result: " + columnDataType);
    }
  }

  /**
   * Writes a non-OBJECT intermediate result into the {@link DataTableBuilder} at the given column.
   * Counterpart of {@link #getIntermediateResult}. OBJECT columns are handled by the caller via
   * {@link AggregationFunction#serializeIntermediateResult}, since they need the aggregation function.
   */
  public static void setIntermediateResult(DataTableBuilder dataTableBuilder, ColumnDataType columnDataType, int colId,
      Object result)
      throws IOException {
    switch (columnDataType) {
      case INT:
        dataTableBuilder.setColumn(colId, (int) result);
        break;
      case LONG:
        dataTableBuilder.setColumn(colId, (long) result);
        break;
      case FLOAT:
        dataTableBuilder.setColumn(colId, (float) result);
        break;
      case DOUBLE:
        dataTableBuilder.setColumn(colId, (double) result);
        break;
      case BIG_DECIMAL:
        dataTableBuilder.setColumn(colId, (BigDecimal) result);
        break;
      case STRING:
        dataTableBuilder.setColumn(colId, result.toString());
        break;
      case BYTES:
        dataTableBuilder.setColumn(colId, (ByteArray) result);
        break;
      default:
        throw new IllegalStateException("Illegal column data type in intermediate result: " + columnDataType);
    }
  }

  /**
   * Reads the final result from the {@link DataTable}.
   */
  public static Comparable getFinalResult(DataTable dataTable, ColumnDataType columnDataType, int rowId, int colId) {
    switch (columnDataType.getStoredType()) {
      case INT:
        return dataTable.getInt(rowId, colId);
      case LONG:
        return dataTable.getLong(rowId, colId);
      case FLOAT:
        return dataTable.getFloat(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case BIG_DECIMAL:
        return dataTable.getBigDecimal(rowId, colId);
      case STRING:
        return dataTable.getString(rowId, colId);
      case BYTES:
        return dataTable.getBytes(rowId, colId);
      case INT_ARRAY:
        return IntArrayList.wrap(dataTable.getIntArray(rowId, colId));
      case LONG_ARRAY:
        return LongArrayList.wrap(dataTable.getLongArray(rowId, colId));
      case FLOAT_ARRAY:
        return FloatArrayList.wrap(dataTable.getFloatArray(rowId, colId));
      case DOUBLE_ARRAY:
        return DoubleArrayList.wrap(dataTable.getDoubleArray(rowId, colId));
      case BIG_DECIMAL_ARRAY:
        return ObjectArrayList.wrap(dataTable.getBigDecimalArray(rowId, colId));
      case STRING_ARRAY:
        return ObjectArrayList.wrap(dataTable.getStringArray(rowId, colId));
      case BYTES_ARRAY:
        return ObjectArrayList.wrap(dataTable.getBytesArray(rowId, colId));
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }

  /**
   * Reads the converted final result from the {@link DataTable}. It should be equivalent to running
   * {@link #getFinalResult} and {@link ColumnDataType#convert}.
   */
  public static Object getConvertedFinalResult(DataTable dataTable, ColumnDataType columnDataType, int rowId,
      int colId) {
    switch (columnDataType) {
      case INT:
        return dataTable.getInt(rowId, colId);
      case LONG:
        return dataTable.getLong(rowId, colId);
      case FLOAT:
        return dataTable.getFloat(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case BIG_DECIMAL:
        return dataTable.getBigDecimal(rowId, colId);
      case BOOLEAN:
        return dataTable.getInt(rowId, colId) == 1;
      case TIMESTAMP:
        return new Timestamp(dataTable.getLong(rowId, colId));
      case STRING:
      case JSON:
        return dataTable.getString(rowId, colId);
      case BYTES:
        return dataTable.getBytes(rowId, colId).getBytes();
      case INT_ARRAY:
        return dataTable.getIntArray(rowId, colId);
      case LONG_ARRAY:
        return dataTable.getLongArray(rowId, colId);
      case FLOAT_ARRAY:
        return dataTable.getFloatArray(rowId, colId);
      case DOUBLE_ARRAY:
        return dataTable.getDoubleArray(rowId, colId);
      case BIG_DECIMAL_ARRAY:
        return dataTable.getBigDecimalArray(rowId, colId);
      case BOOLEAN_ARRAY: {
        int[] intValues = dataTable.getIntArray(rowId, colId);
        int numValues = intValues.length;
        boolean[] booleanValues = new boolean[numValues];
        for (int i = 0; i < numValues; i++) {
          booleanValues[i] = intValues[i] == 1;
        }
        return booleanValues;
      }
      case TIMESTAMP_ARRAY: {
        long[] longValues = dataTable.getLongArray(rowId, colId);
        int numValues = longValues.length;
        Timestamp[] timestampValues = new Timestamp[numValues];
        for (int i = 0; i < numValues; i++) {
          timestampValues[i] = new Timestamp(longValues[i]);
        }
        return timestampValues;
      }
      case STRING_ARRAY:
        return dataTable.getStringArray(rowId, colId);
      case BYTES_ARRAY:
        return dataTable.getBytesArray(rowId, colId);
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }

  public static class AggregationInfo {
    private final AggregationFunction[] _functions;
    private final BaseProjectOperator<?> _projectOperator;
    private final boolean _useStarTree;

    public AggregationInfo(AggregationFunction[] functions, BaseProjectOperator<?> projectOperator,
        boolean useStarTree) {
      _functions = functions;
      _projectOperator = projectOperator;
      _useStarTree = useStarTree;
    }

    public AggregationFunction[] getFunctions() {
      return _functions;
    }

    public BaseProjectOperator<?> getProjectOperator() {
      return _projectOperator;
    }

    public boolean isUseStarTree() {
      return _useStarTree;
    }
  }

  /**
   * Builds {@link AggregationInfo} for aggregations.
   */
  public static AggregationInfo buildAggregationInfo(SegmentContext segmentContext, QueryContext queryContext,
      AggregationFunction[] aggregationFunctions, @Nullable FilterContext filter, BaseFilterOperator filterOperator,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators) {
    // TODO: Create a short-circuit ProjectOperator when filter result is empty
    AggregationInfo aggregationInfo =
        buildAggregationInfoWithStarTree(segmentContext, queryContext, aggregationFunctions, filter, filterOperator,
            predicateEvaluators);
    return aggregationInfo != null ? aggregationInfo
        : buildAggregationInfoWithoutStarTree(segmentContext, queryContext, aggregationFunctions, filterOperator);
  }

  /**
   * Builds {@link AggregationInfo} for aggregations using star-tree index. Returns {@code null} if star-tree index
   * cannot be used.
   */
  @Nullable
  public static AggregationInfo buildAggregationInfoWithStarTree(SegmentContext segmentContext,
      QueryContext queryContext, AggregationFunction[] aggregationFunctions, @Nullable FilterContext filter,
      BaseFilterOperator filterOperator, List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators) {
    /// Star-tree stores pre-aggregated values per group key and cannot expand a row across multiple grouping
    /// sets, so it cannot serve GROUP BY GROUPING SETS / ROLLUP / CUBE queries. Fall back to the regular path.
    if (queryContext.isGroupingSets()) {
      return null;
    }
    if (!filterOperator.isResultEmpty()) {
      BaseProjectOperator<?> projectOperator =
          StarTreeUtils.createStarTreeBasedProjectOperator(segmentContext.getIndexSegment(), queryContext,
              aggregationFunctions, filter, predicateEvaluators);
      if (projectOperator != null) {
        return new AggregationInfo(aggregationFunctions, projectOperator, true);
      }
    }
    return null;
  }

  /**
   * Builds {@link AggregationInfo} for aggregations without using star-tree index.
   */
  public static AggregationInfo buildAggregationInfoWithoutStarTree(SegmentContext segmentContext,
      QueryContext queryContext, AggregationFunction[] aggregationFunctions, BaseFilterOperator filterOperator) {
    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions,
            queryContext.getGroupByExpressions());
    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(segmentContext, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator).run();
    return new AggregationInfo(aggregationFunctions, projectOperator, false);
  }

  /**
   * Builds swim-lanes (list of {@link AggregationInfo}) for filtered aggregations.
   */
  public static List<AggregationInfo> buildFilteredAggregationInfos(SegmentContext segmentContext,
      QueryContext queryContext) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getFilteredAggregationFunctions() != null;

    FilterPlanNode mainFilterPlan = new FilterPlanNode(segmentContext, queryContext);
    BaseFilterOperator mainFilterOperator = mainFilterPlan.run();
    List<Pair<Predicate, PredicateEvaluator>> mainPredicateEvaluators = mainFilterPlan.getPredicateEvaluators();

    // No need to process sub-filters when main filter has empty result
    if (mainFilterOperator.isResultEmpty()) {
      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      Set<ExpressionContext> expressions =
          collectExpressionsToTransform(aggregationFunctions, queryContext.getGroupByExpressions());
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(segmentContext, queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL,
              mainFilterOperator).run();
      return List.of(new AggregationInfo(aggregationFunctions, projectOperator, false));
    }

    // For each aggregation function, check if the aggregation function is a filtered aggregate. If so, populate the
    // corresponding filter operator.
    Map<FilterContext, FilteredAggregationContext> filteredAggregationContexts = new HashMap<>();
    List<AggregationFunction> nonFilteredFunctions = new ArrayList<>();
    FilterContext mainFilter = queryContext.getFilter();
    for (Pair<AggregationFunction, FilterContext> functionFilterPair : queryContext.getFilteredAggregationFunctions()) {
      AggregationFunction aggregationFunction = functionFilterPair.getLeft();
      FilterContext filter = functionFilterPair.getRight();
      if (filter != null) {
        filteredAggregationContexts.computeIfAbsent(filter, k -> {
          FilterContext combinedFilter;
          if (mainFilter == null) {
            combinedFilter = filter;
          } else {
            combinedFilter = FilterContext.forAnd(List.of(mainFilter, filter));
          }

          FilterPlanNode subFilterPlan = new FilterPlanNode(segmentContext, queryContext, filter);
          BaseFilterOperator subFilterOperator = subFilterPlan.run();
          BaseFilterOperator combinedFilterOperator;
          if (mainFilterOperator.isResultMatchingAll() || subFilterOperator.isResultEmpty()) {
            combinedFilterOperator = subFilterOperator;
          } else if (subFilterOperator.isResultMatchingAll()) {
            combinedFilterOperator = mainFilterOperator;
          } else {
            combinedFilterOperator =
                new CombinedFilterOperator(mainFilterOperator, subFilterOperator, queryContext.getQueryOptions());
          }

          List<Pair<Predicate, PredicateEvaluator>> subPredicateEvaluators = subFilterPlan.getPredicateEvaluators();
          List<Pair<Predicate, PredicateEvaluator>> combinedPredicateEvaluators =
              new ArrayList<>(mainPredicateEvaluators.size() + subPredicateEvaluators.size());
          combinedPredicateEvaluators.addAll(mainPredicateEvaluators);
          combinedPredicateEvaluators.addAll(subPredicateEvaluators);

          return new FilteredAggregationContext(combinedFilter, combinedFilterOperator, combinedPredicateEvaluators);
        })._aggregationFunctions.add(aggregationFunction);
      } else {
        nonFilteredFunctions.add(aggregationFunction);
      }
    }

    List<AggregationInfo> aggregationInfos = new ArrayList<>();
    for (FilteredAggregationContext filteredAggregationContext : filteredAggregationContexts.values()) {
      BaseFilterOperator filterOperator = filteredAggregationContext._filterOperator;
      if (filterOperator == mainFilterOperator) {
        // This can happen when the sub filter matches all documents, and we can treat the function as non-filtered
        nonFilteredFunctions.addAll(filteredAggregationContext._aggregationFunctions);
      } else {
        AggregationFunction[] aggregationFunctions =
            filteredAggregationContext._aggregationFunctions.toArray(new AggregationFunction[0]);
        aggregationInfos.add(
            buildAggregationInfo(segmentContext, queryContext, aggregationFunctions, filteredAggregationContext._filter,
                filteredAggregationContext._filterOperator, filteredAggregationContext._predicateEvaluators));
      }
    }

    if (!nonFilteredFunctions.isEmpty() || ((queryContext.getGroupByExpressions() != null)
        && !QueryOptionsUtils.isFilteredAggregationsSkipEmptyGroups(queryContext.getQueryOptions()))) {
      // If there are no non-filtered aggregation functions for a group by query, we still add a new AggregationInfo
      // with an empty AggregationFunction array and the main query filter so that the GroupByExecutor will compute all
      // the groups (from the result of applying the main query filter) but no unnecessary additional aggregation will
      // be done since the AggregationFunction array is empty. However, if the query option to skip empty groups is
      // enabled, we don't do this in order to avoid unnecessary computation of empty groups (which can be very
      // expensive if the main filter has high selectivity).
      AggregationFunction[] aggregationFunctions = nonFilteredFunctions.toArray(new AggregationFunction[0]);
      aggregationInfos.add(
          buildAggregationInfo(segmentContext, queryContext, aggregationFunctions, mainFilter, mainFilterOperator,
              mainPredicateEvaluators));
    }

    return aggregationInfos;
  }

  private static class FilteredAggregationContext {
    final FilterContext _filter;
    final BaseFilterOperator _filterOperator;
    final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators;
    final List<AggregationFunction> _aggregationFunctions = new ArrayList<>();

    public FilteredAggregationContext(FilterContext filter, BaseFilterOperator filterOperator,
        List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators) {
      _filter = filter;
      _filterOperator = filterOperator;
      _predicateEvaluators = predicateEvaluators;
    }
  }

  public static String getResultColumnName(AggregationFunction aggregationFunction, @Nullable FilterContext filter) {
    String columnName = aggregationFunction.getResultColumnName();
    if (filter != null) {
      columnName += " FILTER(WHERE " + filter + ")";
    }
    return columnName;
  }

  /**
   * Gets the aggregation result without scanning the segment.
   * This is used for non-scan based aggregation operator.
   * @param aggregationFunction
   * @param dataSource
   * @param numTotalDocs
   * @return
   */
  public static Object getAggregationResult(AggregationFunction aggregationFunction, DataSource dataSource,
      int numTotalDocs, String explainName) {
    Object result;
    switch (aggregationFunction.getType()) {
      case COUNT:
        result = (long) numTotalDocs;
        break;
      case MIN:
      case MINMV:
        result = getMinValueNumeric(dataSource);
        break;
      case MINLONG:
        result = getMinValueLong(dataSource);
        break;
      case MINSTRING:
        assert dataSource.getDictionary() != null;
        result = dataSource.getDictionary().getMinVal();
        break;
      case MAX:
      case MAXMV:
        result = getMaxValueNumeric(dataSource);
        break;
      case MAXLONG:
        result = getMaxValueLong(dataSource);
        break;
      case MAXSTRING:
        assert dataSource.getDictionary() != null;
        result = dataSource.getDictionary().getMaxVal();
        break;
      case MINMAXRANGE:
      case MINMAXRANGEMV:
        result = new MinMaxRangePair(getMinValueNumeric(dataSource), getMaxValueNumeric(dataSource));
        break;
      case DISTINCTCOUNT:
      case DISTINCTSUM:
      case DISTINCTAVG:
      case DISTINCTCOUNTMV:
      case DISTINCTSUMMV:
      case DISTINCTAVGMV:
        result = getDistinctValueSet(Objects.requireNonNull(dataSource.getDictionary()), explainName);
        break;
      case DISTINCTCOUNTOFFHEAP:
        result = ((DistinctCountOffHeapAggregationFunction) aggregationFunction).extractAggregationResult(
            Objects.requireNonNull(dataSource.getDictionary()));
        break;
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTHLLMV:
        result = getDistinctCountHLLResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountHLLAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTRAWHLL:
      case DISTINCTCOUNTRAWHLLMV:
        result = getDistinctCountHLLResult(Objects.requireNonNull(dataSource.getDictionary()),
            ((DistinctCountRawHLLAggregationFunction) aggregationFunction).getDistinctCountHLLAggregationFunction(),
            explainName);
        break;
      case DISTINCTCOUNTHLLPLUS:
      case DISTINCTCOUNTHLLPLUSMV:
        result = getDistinctCountHLLPlusResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountHLLPlusAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTRAWHLLPLUS:
      case DISTINCTCOUNTRAWHLLPLUSMV:
        result = getDistinctCountHLLPlusResult(Objects.requireNonNull(dataSource.getDictionary()),
            ((DistinctCountRawHLLPlusAggregationFunction) aggregationFunction)
                .getDistinctCountHLLPlusAggregationFunction(), explainName);
        break;
      case SEGMENTPARTITIONEDDISTINCTCOUNT:
        result = (long) Objects.requireNonNull(dataSource.getDictionary()).length();
        break;
      case DISTINCTCOUNTSMARTHLL:
        result = getDistinctCountSmartHLLResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountSmartHLLAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTSMARTHLLPLUS:
        result = getDistinctCountSmartHLLPlusResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountSmartHLLPlusAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTULL:
        result = getDistinctCountULLResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountULLAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTSMARTULL:
        result = getDistinctCountSmartULLResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountSmartULLAggregationFunction) aggregationFunction, explainName);
        break;
      case DISTINCTCOUNTRAWULL:
        result = getDistinctCountULLResult(Objects.requireNonNull(dataSource.getDictionary()),
            (DistinctCountULLAggregationFunction) aggregationFunction, explainName);
        break;
      default:
        throw new IllegalStateException(
            "Non-scan based aggregation operator does not support function type: " + aggregationFunction.getType());
    }

    return result;
  }

  private static Double getMinValueNumeric(DataSource dataSource) {
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary != null) {
      return toDouble(dictionary.getMinVal());
    }
    return toDouble(dataSource.getDataSourceMetadata().getMinValue());
  }

  private static Long getMinValueLong(DataSource dataSource) {
    FieldSpec.DataType dataType = dataSource.getDataSourceMetadata().getDataType().getStoredType();
    Preconditions.checkArgument(
        dataType == FieldSpec.DataType.LONG || dataType == FieldSpec.DataType.INT,
        "MINLONG aggregation function can only be applied to columns of integer types");
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary != null) {
      return ((Number) dictionary.getMinVal()).longValue();
    }
    return ((Number) dataSource.getDataSourceMetadata().getMinValue()).longValue();
  }

  private static Double getMaxValueNumeric(DataSource dataSource) {
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary != null) {
      return toDouble(dictionary.getMaxVal());
    }
    return toDouble(dataSource.getDataSourceMetadata().getMaxValue());
  }

  private static Long getMaxValueLong(DataSource dataSource) {
    FieldSpec.DataType dataType = dataSource.getDataSourceMetadata().getDataType().getStoredType();
    Preconditions.checkArgument(
        dataType == FieldSpec.DataType.LONG || dataType == FieldSpec.DataType.INT,
        "MAXLONG aggregation function can only be applied to columns of integer types");
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary != null) {
      return ((Number) dictionary.getMaxVal()).longValue();
    }
    return ((Number) dataSource.getDataSourceMetadata().getMaxValue()).longValue();
  }

  private static Double toDouble(Comparable<?> value) {
    if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else {
      return Double.parseDouble(value.toString());
    }
  }

  private static Set getDistinctValueSet(Dictionary dictionary, String explainName) {
    int dictionarySize = dictionary.length();
    switch (dictionary.getValueType()) {
      case INT:
        IntOpenHashSet intSet = new IntOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          intSet.add(dictionary.getIntValue(dictId));
        }
        return intSet;
      case LONG:
        LongOpenHashSet longSet = new LongOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          longSet.add(dictionary.getLongValue(dictId));
        }
        return longSet;
      case FLOAT:
        FloatOpenHashSet floatSet = new FloatOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          floatSet.add(dictionary.getFloatValue(dictId));
        }
        return floatSet;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          doubleSet.add(dictionary.getDoubleValue(dictId));
        }
        return doubleSet;
      case BIG_DECIMAL:
        ObjectOpenHashSet<BigDecimal> bigDecimalSet = new ObjectOpenHashSet<>(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          bigDecimalSet.add(dictionary.getBigDecimalValue(dictId));
        }
        return bigDecimalSet;
      case STRING:
        ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          stringSet.add(dictionary.getStringValue(dictId));
        }
        return stringSet;
      case BYTES:
        ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, explainName);
          bytesSet.add(new ByteArray(dictionary.getBytesValue(dictId)));
        }
        return bytesSet;
      default:
        throw new IllegalStateException();
    }
  }

  private static HyperLogLog getDistinctValueHLL(Dictionary dictionary, int log2m, String explainName) {
    HyperLogLog hll = new HyperLogLog(log2m);
    int length = dictionary.length();
    for (int i = 0; i < length; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
      hll.offer(dictionary.get(i));
    }
    return hll;
  }

  private static UltraLogLog getDistinctValueULL(Dictionary dictionary, int p, String explainName) {
    UltraLogLog ull = UltraLogLog.create(p);
    int length = dictionary.length();
    for (int i = 0; i < length; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
      Object value = dictionary.get(i);
      UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
    }
    return ull;
  }

  private static HyperLogLogPlus getDistinctValueHLLPlus(Dictionary dictionary, int p, int sp, String explainName) {
    HyperLogLogPlus hllPlus = new HyperLogLogPlus(p, sp);
    int length = dictionary.length();
    for (int i = 0; i < length; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
      hllPlus.offer(dictionary.get(i));
    }
    return hllPlus;
  }

  private static HyperLogLog getDistinctCountHLLResult(Dictionary dictionary,
      DistinctCountHLLAggregationFunction function, String explainName) {
    if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
      // Treat BYTES value as serialized HyperLogLog
      try {
        QueryThreadContext.checkTerminationAndSampleUsage(explainName);
        HyperLogLog hll = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(dictionary.getBytesValue(0));
        int length = dictionary.length();
        for (int i = 1; i < length; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
          hll.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(dictionary.getBytesValue(i)));
        }
        return hll;
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
    } else {
      return getDistinctValueHLL(dictionary, function.getLog2m(), explainName);
    }
  }

  private static HyperLogLogPlus getDistinctCountHLLPlusResult(Dictionary dictionary,
      DistinctCountHLLPlusAggregationFunction function, String explainName) {
    if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
      // Treat BYTES value as serialized HyperLogLogPlus
      try {
        QueryThreadContext.checkTerminationAndSampleUsage(explainName);
        HyperLogLogPlus hllplus = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(dictionary.getBytesValue(0));
        int length = dictionary.length();
        for (int i = 1; i < length; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
          hllplus.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(dictionary.getBytesValue(i)));
        }
        return hllplus;
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPluses", e);
      }
    } else {
      return getDistinctValueHLLPlus(dictionary, function.getP(), function.getSp(), explainName);
    }
  }

  private static Object getDistinctCountSmartHLLResult(Dictionary dictionary,
      DistinctCountSmartHLLAggregationFunction function, String explainPlanName) {
    if (dictionary.length() > function.getThreshold()) {
      // Store values into a HLL when the dictionary size exceeds the conversion threshold
      return getDistinctValueHLL(dictionary, function.getLog2m(), explainPlanName);
    } else {
      return getDistinctValueSet(dictionary, explainPlanName);
    }
  }

  private static Object getDistinctCountSmartHLLPlusResult(Dictionary dictionary,
      DistinctCountSmartHLLPlusAggregationFunction function, String explainName) {
    if (dictionary.length() > function.getThreshold()) {
      // Store values into a HLLPlus when the dictionary size exceeds the conversion threshold
      return getDistinctValueHLLPlus(dictionary, function.getP(), function.getSp(), explainName);
    } else {
      return getDistinctValueSet(dictionary, explainName);
    }
  }

  private static UltraLogLog getDistinctCountULLResult(Dictionary dictionary,
      DistinctCountULLAggregationFunction function, String explainName) {
    if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
      // Treat BYTES value as serialized UltraLogLog and merge
      try {
        QueryThreadContext.checkTerminationAndSampleUsage(explainName);
        UltraLogLog ull = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(dictionary.getBytesValue(0));
        int length = dictionary.length();
        for (int i = 1; i < length; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, explainName);
          ull.add(ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(dictionary.getBytesValue(i)));
        }
        return ull;
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging UltraLogLogs", e);
      }
    } else {
      return getDistinctValueULL(dictionary, function.getP(), explainName);
    }
  }

  private static Object getDistinctCountSmartULLResult(Dictionary dictionary,
      DistinctCountSmartULLAggregationFunction function, String explainName) {
    if (dictionary.length() > function.getThreshold()) {
      return getDistinctValueULL(dictionary, function.getP(), explainName);
    } else {
      return getDistinctValueSet(dictionary, explainName);
    }
  }
}
