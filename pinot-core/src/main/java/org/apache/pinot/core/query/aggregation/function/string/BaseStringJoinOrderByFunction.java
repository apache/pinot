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
package org.apache.pinot.core.query.aggregation.function.string;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseStringJoinOrderByFunction<I>
    extends BaseSingleInputAggregationFunction<I, String> {
  protected final boolean _nullHandlingEnabled;
  protected final String _separator;
  protected final List<OrderByExpressionContext> _orderByExpressionContext;
  protected final List<FieldSpec.DataType> _orderByDataTypes;

  public BaseStringJoinOrderByFunction(ExpressionContext expression, String separator,
      List<OrderByExpressionContext> orderByExpressionContext, List<FieldSpec.DataType> orderByDataTypes,
      boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _separator = separator;
    _orderByExpressionContext = orderByExpressionContext;
    _orderByDataTypes = orderByDataTypes;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.STRINGJOIN;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    ArrayList<ExpressionContext> inputExpressions = new ArrayList<>();
    inputExpressions.add(_expression);
    _orderByExpressionContext.stream()
        .forEach(orderByExpressionContext -> inputExpressions.add(orderByExpressionContext.getExpression()));
    return inputExpressions;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.STRING;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    List<BlockValSet> orderByBlockValSets =
        _orderByExpressionContext.stream()
            .map(expressionContext -> blockValSetMap.get(expressionContext.getExpression())).collect(
                Collectors.toList());
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayWithNull(length, aggregationResultHolder, blockValSet, orderByBlockValSets, nullBitmap);
        return;
      }
    }
    aggregateArray(length, aggregationResultHolder, blockValSet, orderByBlockValSets);
  }

  protected abstract void aggregateArray(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, List<BlockValSet> orderByBlockValSets);

  protected abstract void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, List<BlockValSet> orderByBlockValSets, RoaringBitmap nullBitmap);

  protected List<Object> extractOrderByValues(List<BlockValSet> orderByBlockValSets) {
    int orderByExprSize = orderByBlockValSets.size();
    List<Object> results = new ArrayList<>(orderByExprSize);
    for (int i = 0; i < orderByExprSize; i++) {
      FieldSpec.DataType dataType = _orderByDataTypes.get(i);
      switch (dataType) {
        case INT:
          results.add(orderByBlockValSets.get(i).getIntValuesSV());
          break;
        case LONG:
        case TIMESTAMP:
          results.add(orderByBlockValSets.get(i).getLongValuesSV());
          break;
        case FLOAT:
          results.add(orderByBlockValSets.get(i).getFloatValuesSV());
          break;
        case DOUBLE:
          results.add(orderByBlockValSets.get(i).getDoubleValuesSV());
          break;
        case STRING:
          results.add(orderByBlockValSets.get(i).getStringValuesSV());
          break;
        case BIG_DECIMAL:
          results.add(orderByBlockValSets.get(i).getBigDecimalValuesSV());
          break;
        case BYTES:
          results.add(orderByBlockValSets.get(i).getBytesValuesSV());
          break;
        default:
          throw new RuntimeException("Unsupported data type for StringJoin OrderBy: " + dataType);
      }
    }
    return results;
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    List<BlockValSet> orderByBlockValSets =
        _orderByExpressionContext.stream()
            .map(expressionContext -> blockValSetMap.get(expressionContext.getExpression())).collect(
                Collectors.toList());
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        String[] valueExpr = blockValSet.getStringValuesSV();
        List<Object> orderByExprs = extractOrderByValues(orderByBlockValSets);
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            Object[] values = extractValues(valueExpr, orderByExprs, i);
            setGroupByResult(groupByResultHolder, groupKeyArray[i], values);
          }
        }
        return;
      }
    }
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExprs = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      Object[] values = extractValues(valueExpr, orderByExprs, i);
      setGroupByResult(groupByResultHolder, groupKeyArray[i], values);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    List<BlockValSet> orderByBlockValSets =
        _orderByExpressionContext.stream()
            .map(expressionContext -> blockValSetMap.get(expressionContext.getExpression())).collect(
                Collectors.toList());
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        String[] valueExpr = blockValSet.getStringValuesSV();
        List<Object> orderByExprs = extractOrderByValues(orderByBlockValSets);
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            for (int groupKey : groupKeysArray[i]) {
              Object[] values = extractValues(valueExpr, orderByExprs, i);
              setGroupByResult(groupByResultHolder, groupKey, values);
            }
          }
        }
        return;
      }
    }
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExprs = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        Object[] values = extractValues(valueExpr, orderByExprs, i);
        setGroupByResult(groupByResultHolder, groupKey, values);
      }
    }
  }

  @Override
  public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public I extractGroupByResult(GroupByResultHolder groupByResultHolder,
      int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  abstract void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, Object[] value);

  protected Object[] extractValues(String[] valueExpr, List<Object> orderByExprs, int rowIdx) {
    Object[] values = new Object[1 + orderByExprs.size()];
    for (int j = 0; j < orderByExprs.size(); j++) {
      Object arrayObject = orderByExprs.get(j);
      FieldSpec.DataType dataType = _orderByDataTypes.get(j);
      switch (dataType) {
        case INT:
          values[j] = ((int[]) arrayObject)[rowIdx];
          break;
        case LONG:
        case TIMESTAMP:
          values[j] = ((long[]) arrayObject)[rowIdx];
          break;
        case FLOAT:
          values[j] = ((float[]) arrayObject)[rowIdx];
          break;
        case DOUBLE:
          values[j] = ((double[]) arrayObject)[rowIdx];
          break;
        case STRING:
          values[j] = ((String[]) arrayObject)[rowIdx];
          break;
        case BIG_DECIMAL:
          values[j] = ((Object[]) arrayObject)[rowIdx];
          break;
        case BYTES:
          values[j] = ((byte[][]) arrayObject)[rowIdx];
          break;
        default:
          throw new RuntimeException("Unsupported data type for StringJoin OrderBy: " + dataType);
      }
    }
    values[orderByExprs.size()] = valueExpr[rowIdx];
    return values;
  }
}
