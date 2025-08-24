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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * AnyValue aggregation function returns any arbitrary value from the column for each group.
 * This is useful for GROUP BY queries where you want to include a column in SELECT that has
 * a 1:1 mapping with the GROUP BY columns, avoiding the need to add it to GROUP BY clause.
 *
 * Example: SELECT CustomerID, ANY_VALUE(CustomerName), SUM(OrderValue) FROM Orders GROUP BY CustomerID
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AnyValueAggregationFunction extends NullableSingleInputAggregationFunction<Object, Comparable> {

  public AnyValueAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "ANY_VALUE"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ANYVALUE;
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // If we already have a value, we don't need to process more (any value is fine)
    if (aggregationResultHolder.getResult() != null) {
      return;
    }

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        Integer anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          // Return the first non-null value we encounter
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        Long anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        Float anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        Double anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        BigDecimal anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case STRING: {
        String[] values = blockValSet.getStringValuesSV();
        String anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      case BYTES: {
        byte[][] values = blockValSet.getBytesValuesSV();
        byte[] anyValue = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          return acum != null ? acum : values[from];
        });
        if (anyValue != null) {
          aggregationResultHolder.setValue(anyValue);
        }
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case STRING: {
        String[] values = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      case BYTES: {
        byte[][] values = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int groupKey = groupKeyArray[i];
            if (groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, values[i]);
            }
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case STRING: {
        String[] values = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      case BYTES: {
        byte[][] values = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              if (groupByResultHolder.getResult(groupKey) == null) {
                groupByResultHolder.setValueForKey(groupKey, values[i]);
              }
            }
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
    }
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    // For ANY_VALUE, we can return either value. Prefer the first non-null value.
    if (intermediateResult1 != null) {
      return intermediateResult1;
    }
    return intermediateResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public Comparable extractFinalResult(Object intermediateResult) {
    return (Comparable) intermediateResult;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object intermediateResult) {
    // For ANY_VALUE, we serialize the intermediate result using the generic ObjectSerDeUtils
    // Get the object type and serialize with the appropriate type
    int type = ObjectSerDeUtils.ObjectType.getObjectType(intermediateResult).getValue();
    byte[] bytes = ObjectSerDeUtils.serialize(intermediateResult, type);
    return new SerializedIntermediateResult(type, bytes);
  }

  @Override
  public Object deserializeIntermediateResult(CustomObject customObject) {
    // Deserialize the intermediate result using the generic ObjectSerDeUtils
    return ObjectSerDeUtils.deserialize(customObject);
  }
}
