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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.CombinedFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.TransformPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
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
      @Nullable ExpressionContext[] groupByExpressions) {
    Set<ExpressionContext> expressions = new HashSet<>();
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      expressions.addAll(aggregationFunction.getInputExpressions());
    }
    if (groupByExpressions != null) {
      expressions.addAll(Arrays.asList(groupByExpressions));
    }
    return expressions;
  }

  /**
   * Creates a map from expression required by the {@link AggregationFunction} to {@link BlockValSet} fetched from the
   * {@link TransformBlock}.
   */
  public static Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggregationFunction,
      TransformBlock transformBlock) {
    //noinspection unchecked
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    if (numExpressions == 1) {
      ExpressionContext expression = expressions.get(0);
      return Collections.singletonMap(expression, transformBlock.getBlockValueSet(expression));
    }
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (ExpressionContext expression : expressions) {
      blockValSetMap.put(expression, transformBlock.getBlockValueSet(expression));
    }
    return blockValSetMap;
  }

  /**
   * (For Star-Tree) Creates a map from expression required by the {@link AggregationFunctionColumnPair} to
   * {@link BlockValSet} fetched from the {@link TransformBlock}.
   * <p>NOTE: We construct the map with original column name as the key but fetch BlockValSet with the aggregation
   *          function pair so that the aggregation result column name is consistent with or without star-tree.
   */
  public static Map<ExpressionContext, BlockValSet> getBlockValSetMap(
      AggregationFunctionColumnPair aggregationFunctionColumnPair, TransformBlock transformBlock) {
    ExpressionContext expression = ExpressionContext.forIdentifier(aggregationFunctionColumnPair.getColumn());
    BlockValSet blockValSet = transformBlock.getBlockValueSet(aggregationFunctionColumnPair.toColumnName());
    return Collections.singletonMap(expression, blockValSet);
  }

  /**
   * Reads the intermediate result from the {@link DataTable}.
   *
   * TODO: Move ser/de into AggregationFunction interface
   */
  public static Object getIntermediateResult(DataTable dataTable, ColumnDataType columnDataType, int rowId, int colId) {
    switch (columnDataType) {
      case LONG:
        return dataTable.getLong(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case OBJECT:
        DataTable.CustomObject customObject = dataTable.getCustomObject(rowId, colId);
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
      case STRING:
        return dataTable.getString(rowId, colId);
      case BYTES:
        return dataTable.getBytes(rowId, colId).getBytes();
      case DOUBLE_ARRAY:
        return dataTable.getDoubleArray(rowId, colId);
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }

  /**
   * Build a filter operator from the given FilterContext.
   *
   * It returns the FilterPlanNode to allow reusing plan level components such as predicate
   * evaluator map
   */
  public static Pair<FilterPlanNode, BaseFilterOperator> buildFilterOperator(IndexSegment indexSegment,
      QueryContext queryContext, FilterContext filterContext) {
    FilterPlanNode filterPlanNode = new FilterPlanNode(indexSegment, queryContext, filterContext);
    return Pair.of(filterPlanNode, filterPlanNode.run());
  }

  public static Pair<FilterPlanNode, BaseFilterOperator> buildFilterOperator(IndexSegment indexSegment,
      QueryContext queryContext) {
    return buildFilterOperator(indexSegment, queryContext, queryContext.getFilter());
  }

  public static TransformOperator buildTransformOperatorForFilteredAggregates(IndexSegment indexSegment,
      QueryContext queryContext, BaseFilterOperator filterOperator, @Nullable ExpressionContext[] groupByExpressions) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    Set<ExpressionContext> expressionsToTransform =
        collectExpressionsToTransform(aggregationFunctions, groupByExpressions);
    return new TransformPlanNode(indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterOperator).run();
  }

  /**
   * Build pairs of filtered aggregation functions and corresponding transform operator
   * @param mainPredicateFilterOperator Filter operator corresponding to the main predicate
   * @param mainTransformOperator Transform operator corresponding to the main predicate
   */
  public static List<Pair<AggregationFunction[], TransformOperator>> buildFilteredAggTransformPairs(
      IndexSegment indexSegment, QueryContext queryContext, BaseFilterOperator mainPredicateFilterOperator,
      TransformOperator mainTransformOperator, @Nullable ExpressionContext[] groupByExpressions) {
    Map<FilterContext, Pair<List<AggregationFunction>, TransformOperator>> filterContextToAggFuncsMap = new HashMap<>();
    List<AggregationFunction> nonFilteredAggregationFunctions = new ArrayList<>();
    List<Pair<AggregationFunction, FilterContext>> aggregationFunctions =
        queryContext.getFilteredAggregationFunctions();
    List<Pair<AggregationFunction[], TransformOperator>> aggToTransformOpList = new ArrayList<>();

    // For each aggregation function, check if the aggregation function is a filtered agg.
    // If it is, populate the corresponding filter operator and corresponding transform operator
    assert aggregationFunctions != null;
    for (Pair<AggregationFunction, FilterContext> inputPair : aggregationFunctions) {
      if (inputPair.getLeft() != null) {
        FilterContext currentFilterExpression = inputPair.getRight();
        if (filterContextToAggFuncsMap.get(currentFilterExpression) != null) {
          filterContextToAggFuncsMap.get(currentFilterExpression).getLeft().add(inputPair.getLeft());
          continue;
        }
        Pair<FilterPlanNode, BaseFilterOperator> filterPlanOpPair =
            buildFilterOperator(indexSegment, queryContext, currentFilterExpression);
        BaseFilterOperator wrappedFilterOperator =
            new CombinedFilterOperator(mainPredicateFilterOperator, filterPlanOpPair.getRight(),
                queryContext.getQueryOptions());
        TransformOperator newTransformOperator =
            buildTransformOperatorForFilteredAggregates(indexSegment, queryContext, wrappedFilterOperator,
                groupByExpressions);
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
    // Convert to array since FilteredGroupByOperator expects it
    for (Pair<List<AggregationFunction>, TransformOperator> pair : filterContextToAggFuncsMap.values()) {
      List<AggregationFunction> aggregationFunctionList = pair.getLeft();
      if (aggregationFunctionList == null) {
        throw new IllegalStateException("Null aggregation list seen");
      }
      aggToTransformOpList.add(Pair.of(aggregationFunctionList.toArray(new AggregationFunction[0]), pair.getRight()));
    }
    aggToTransformOpList.add(
        Pair.of(nonFilteredAggregationFunctions.toArray(new AggregationFunction[0]), mainTransformOperator));

    return aggToTransformOpList;
  }

  public static String getResultColumnName(AggregationFunction aggregationFunction, @Nullable FilterContext filter) {
      String columnName = aggregationFunction.getResultColumnName();
      if (filter != null) {
        columnName += " FILTER(WHERE " + filter + ")";
      }
      return columnName;
  }
}
