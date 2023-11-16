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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
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
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  /**
   * (For Star-Tree) Creates an {@link AggregationFunctionColumnPair} from the {@link AggregationFunction}. Returns
   * {@code null} if the {@link AggregationFunction} cannot be represented as an {@link AggregationFunctionColumnPair}
   * (e.g. has multiple arguments, argument is not column etc.).
   */
  @Nullable
  public static AggregationFunctionColumnPair getAggregationFunctionColumnPair(
      AggregationFunction aggregationFunction) {
    AggregationFunctionType aggregationFunctionType = aggregationFunction.getType();
    if (aggregationFunctionType == AggregationFunctionType.COUNT) {
      return AggregationFunctionColumnPair.COUNT_STAR;
    }
    List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
    if (inputExpressions.size() == 1) {
      ExpressionContext inputExpression = inputExpressions.get(0);
      if (inputExpression.getType() == ExpressionContext.Type.IDENTIFIER) {
        return new AggregationFunctionColumnPair(aggregationFunctionType, inputExpression.getIdentifier());
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
      return Collections.emptyMap();
    }
    if (numExpressions == 1) {
      ExpressionContext expression = expressions.get(0);
      return Collections.singletonMap(expression, valueBlock.getBlockValueSet(expression));
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
    return Collections.singletonMap(expression, blockValSet);
  }

  /**
   * Reads the intermediate result from the {@link DataTable}.
   *
   * TODO: Move ser/de into AggregationFunction interface
   */
  public static Object getIntermediateResult(DataTable dataTable, ColumnDataType columnDataType, int rowId, int colId) {
    switch (columnDataType) {
      case INT:
        return dataTable.getInt(rowId, colId);
      case LONG:
        return dataTable.getLong(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case OBJECT:
        CustomObject customObject = dataTable.getCustomObject(rowId, colId);
        return customObject != null ? ObjectSerDeUtils.deserialize(customObject) : null;
      default:
        throw new IllegalStateException("Illegal column data type in intermediate result: " + columnDataType);
    }
  }

  /**
   * Reads the converted final result from the {@link DataTable}. It should be equivalent to running
   * {@link AggregationFunction#extractFinalResult(Object)} and {@link ColumnDataType#convert(Object)}.
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
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }


  public static class CombinedFilteredAggregationContext {
    private final BaseFilterOperator _baseFilterOperator;
    private final FilterContext _mainFilterContext;
    private final FilterContext _subFilterContext;
    private final FilterContext _combinedFilterContext;
    private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators;
    private final List<AggregationFunction> _aggregationFunctions;

    public CombinedFilteredAggregationContext(BaseFilterOperator baseFilterOperator,
        List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators, @Nullable FilterContext mainFilterContext,
        FilterContext subFilterContext, List<AggregationFunction> aggregationFunctions) {
      _baseFilterOperator = baseFilterOperator;
      _predicateEvaluators = predicateEvaluators;
      _mainFilterContext = mainFilterContext;
      _subFilterContext = subFilterContext;
      _aggregationFunctions = aggregationFunctions;
      if (_mainFilterContext == null) {
        _combinedFilterContext = _subFilterContext;
      } else {
        _combinedFilterContext = FilterContext.forAnd(List.of(_mainFilterContext, _subFilterContext));
      }
    }

    public CombinedFilteredAggregationContext(BaseFilterOperator baseFilterOperator,
        List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators, @Nullable FilterContext mainFilterContext,
        FilterContext subFilterContext) {
      this(baseFilterOperator, predicateEvaluators, mainFilterContext, subFilterContext, new ArrayList<>());
    }
  }

  /**
   * Build pairs of filtered aggregation functions and corresponding project operator.
   */
  public static List<Pair<AggregationFunction[], Pair<BaseProjectOperator<?>, Boolean>>>
      buildFilteredAggregateProjectOperators(IndexSegment indexSegment, QueryContext queryContext) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getFilteredAggregationFunctions() != null;

    FilterPlanNode mainFilterPlan = new FilterPlanNode(indexSegment, queryContext);
    BaseFilterOperator mainFilterOperator = mainFilterPlan.run();

    // No need to process sub-filters when main filter has empty result
    if (mainFilterOperator.isResultEmpty()) {
      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      Set<ExpressionContext> expressions =
          collectExpressionsToTransform(aggregationFunctions, queryContext.getGroupByExpressions());
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(indexSegment, queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL,
              mainFilterOperator).run();
      return Collections.singletonList(Pair.of(aggregationFunctions, Pair.of(projectOperator, false)));
    }

    // For each aggregation function, check if the aggregation function is a filtered aggregate. If so, populate the
    // corresponding filter operator.
    Map<FilterContext, CombinedFilteredAggregationContext> filterOperators = new HashMap<>();
    List<AggregationFunction> nonFilteredFunctions = new ArrayList<>();
    for (Pair<AggregationFunction, FilterContext> functionFilterPair : queryContext.getFilteredAggregationFunctions()) {
      AggregationFunction aggregationFunction = functionFilterPair.getLeft();
      FilterContext filter = functionFilterPair.getRight();
      if (filter != null) {
        filterOperators.computeIfAbsent(filter, k -> {
          FilterPlanNode subFilterPlan = new FilterPlanNode(indexSegment, queryContext, filter);
          BaseFilterOperator subFilterOperator = subFilterPlan.run();
          // TODO(egalpin): Possibly just use a single Set of all predicate evaluators across all filterContexts as
          //  it will have a smaller overall memory footprint
          List<Pair<Predicate, PredicateEvaluator>> combinedPredicateEvaluators =
              new ArrayList<>(mainFilterPlan.getPredicateEvaluators());
          // N.B. subFilterPlan object will only have predicateEvaluators populated through the act of calling .run()
          // on the plan
          combinedPredicateEvaluators.addAll(subFilterPlan.getPredicateEvaluators());

          BaseFilterOperator combinedFilterOperator;
          if (mainFilterOperator.isResultMatchingAll() || subFilterOperator.isResultEmpty()) {
            combinedFilterOperator = subFilterOperator;
          } else if (subFilterOperator.isResultMatchingAll()) {
              combinedFilterOperator = mainFilterOperator;
          } else {
            combinedFilterOperator =
                new CombinedFilterOperator(mainFilterOperator, subFilterOperator, queryContext.getQueryOptions());
          }
          return new CombinedFilteredAggregationContext(combinedFilterOperator, combinedPredicateEvaluators,
              queryContext.getFilter(), filter);
        })._aggregationFunctions.add(aggregationFunction);
      } else {
        nonFilteredFunctions.add(aggregationFunction);
      }
    }

    List<Pair<AggregationFunction[], Pair<BaseProjectOperator<?>, Boolean>>> projectOperators = new ArrayList<>();
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    for (CombinedFilteredAggregationContext combinedFilteredAggregationContext : filterOperators.values()) {
      BaseFilterOperator filterOperator = combinedFilteredAggregationContext._baseFilterOperator;
      if (filterOperator == mainFilterOperator) {
        // This can happen when the sub filter matches all documents, and we can treat the function as non-filtered
        nonFilteredFunctions.addAll(combinedFilteredAggregationContext._aggregationFunctions);
      } else {
        AggregationFunction[] aggregationFunctions =
            combinedFilteredAggregationContext._aggregationFunctions.toArray(new AggregationFunction[0]);
        Pair<BaseProjectOperator<?>, Boolean> projectOperatorStPair =
            createProjectOperatorStPair(indexSegment, queryContext,
                combinedFilteredAggregationContext._combinedFilterContext, aggregationFunctions,
                combinedFilteredAggregationContext._predicateEvaluators,
                combinedFilteredAggregationContext._baseFilterOperator);
        projectOperators.add(Pair.of(aggregationFunctions, projectOperatorStPair));
      }
    }

    if (!nonFilteredFunctions.isEmpty()) {
      AggregationFunction[] aggregationFunctions = nonFilteredFunctions.toArray(new AggregationFunction[0]);
      Pair<BaseProjectOperator<?>, Boolean> projectOperatorStPair =
          createProjectOperatorStPair(indexSegment, queryContext,
              queryContext.getFilter(), aggregationFunctions,
              mainFilterPlan.getPredicateEvaluators(), mainFilterOperator);
      projectOperators.add(Pair.of(aggregationFunctions, projectOperatorStPair));
    }

    return projectOperators;
  }

  public static String getResultColumnName(AggregationFunction aggregationFunction, @Nullable FilterContext filter) {
    String columnName = aggregationFunction.getResultColumnName();
    if (filter != null) {
      columnName += " FILTER(WHERE " + filter + ")";
    }
    return columnName;
  }

  public static Pair<BaseProjectOperator<?>, Boolean> createProjectOperatorStPair(IndexSegment indexSegment,
      QueryContext queryContext, FilterContext filterContext, AggregationFunction[] aggregationFunctions,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators, BaseFilterOperator baseFilterOperator) {

    boolean canUseStarTree = false;
    BaseProjectOperator<?> projectOperator =
        StarTreeUtils.createStarTreeBasedProjectOperator(indexSegment, queryContext, filterContext,
            aggregationFunctions, predicateEvaluators);

    if (projectOperator != null) {
      canUseStarTree = true;
    } else {
      Set<ExpressionContext> expressionsToTransform =
          AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions,
              queryContext.getGroupByExpressions());
      projectOperator =
          new ProjectPlanNode(indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
              baseFilterOperator).run();
    }

    return Pair.of(projectOperator, canUseStarTree);
  }
}
