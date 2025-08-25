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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
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
public class AnyValueAggregationFunction extends NullableSingleInputAggregationFunction<Object, String> {

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

    // Extract any non-null value based on data type
    Object anyValue = extractAnyValue(length, blockValSet);
    if (anyValue != null) {
      aggregationResultHolder.setValue(anyValue);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Use helper method to handle different data types
    aggregateGroupByWithHelper(length, blockValSet,
        (i, value) -> {
          int groupKey = groupKeyArray[i];
          // Only set the value if the group doesn't already have a value (ANY_VALUE semantics)
          // Skip null values to avoid setting default values for groups
          if (value != null && groupByResultHolder.getResult(groupKey) == null) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Use helper method to handle different data types
    aggregateGroupByWithHelper(length, blockValSet,
        (i, value) -> {
          for (int groupKey : groupKeysArray[i]) {
            // Only set the value if the group doesn't already have a value (ANY_VALUE semantics)
            // Skip null values to avoid setting default values for groups
            if (value != null && groupByResultHolder.getResult(groupKey) == null) {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        });
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
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(Object intermediateResult) {
    // Convert the result to string since getFinalResultColumnType returns STRING
    if (intermediateResult == null) {
      return null;
    }
    return intermediateResult.toString();
  }

  @Override
  public String mergeFinalResult(String finalResult1, String finalResult2) {
    // For ANY_VALUE, just return the first non-null result
    return finalResult1 != null ? finalResult1 : finalResult2;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object intermediateResult) {
    // Self-contained serialization for ANY_VALUE - avoids ObjectSerDeUtils limitations
    // This approach is more maintainable and supports all data types ANY_VALUE can return

    if (intermediateResult == null) {
      return new SerializedIntermediateResult(0, new byte[0]);
    }

    // Convert all intermediate results to String for consistent serialization
    // This ensures compatibility with all data types and simplifies maintenance
    String stringValue = intermediateResult.toString();
    byte[] bytes = stringValue.getBytes(StandardCharsets.UTF_8);

    // Use a custom type identifier for ANY_VALUE serialization
    // This makes the serialization self-contained and backward compatible
    return new SerializedIntermediateResult(999, bytes); // Custom type ID for ANY_VALUE
  }

  @Override
  public Object deserializeIntermediateResult(CustomObject customObject) {
    // Self-contained deserialization for ANY_VALUE
    if (customObject.getBuffer().remaining() == 0) {
      return null;
    }

    // Deserialize as String - this matches our serialization approach
    byte[] bytes = new byte[customObject.getBuffer().remaining()];
    customObject.getBuffer().get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Helper method to extract any non-null value from a BlockValSet
   */
  private Object extractAnyValue(int length, BlockValSet blockValSet) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getIntValuesSV()[from]);
      case LONG:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getLongValuesSV()[from]);
      case FLOAT:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getFloatValuesSV()[from]);
      case DOUBLE:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getDoubleValuesSV()[from]);
      case STRING:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getStringValuesSV()[from]);
      case BIG_DECIMAL:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getBigDecimalValuesSV()[from]);
      case BYTES:
        return foldNotNull(length, blockValSet, null, (acum, from, to) ->
            acum != null ? acum : blockValSet.getBytesValuesSV()[from]);
      default:
        throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
    }
  }

  /**
   * Helper method for group by aggregation with different data types
   * Handles both null handling enabled and disabled cases properly
   */
  private void aggregateGroupByWithHelper(int length, BlockValSet blockValSet, ValueSetter valueSetter) {
    // Check if null handling is enabled for this block
    boolean nullHandlingEnabled = blockValSet.getNullBitmap() != null;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        if (nullHandlingEnabled) {
          // With null handling enabled, use forEachNotNull to skip nulls
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, intValues[i]);
            }
          });
        } else {
          // With null handling disabled, process all values
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, intValues[i]);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, longValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, floatValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, doubleValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, doubleValues[i]);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, stringValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, stringValues[i]);
          }
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, bigDecimalValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, bigDecimalValues[i]);
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        if (nullHandlingEnabled) {
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              valueSetter.setValue(i, bytesValues[i]);
            }
          });
        } else {
          for (int i = 0; i < length; i++) {
            valueSetter.setValue(i, bytesValues[i]);
          }
        }
        break;
      default:
        throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
    }
  }

  /**
   * Functional interface for setting values in group by operations
   */
  @FunctionalInterface
  private interface ValueSetter {
    void setValue(int index, Object value);
  }
}
