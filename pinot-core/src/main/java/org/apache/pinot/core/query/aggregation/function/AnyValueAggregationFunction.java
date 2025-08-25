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
import java.nio.ByteBuffer;
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
 * AnyValue aggregation function returns any arbitrary NON-NULL value from the column for each group.
 * <p>
 * This is useful for GROUP BY queries where you want to include a column in SELECT that has a 1:1 mapping with the
 * GROUP BY columns, avoiding the need to add it to GROUP BY clause. The implementation is null-aware and will scan
 * only until it finds the first non-null value in the current batch for each group/key. This makes it O(n) over the
 * input once per group until the first value is set, with early-exit fast paths when there are no nulls.
 * </p>
 * <p><strong>Example:</strong></p>
 * <pre>{@code
 * SELECT CustomerID,
 *        ANY_VALUE(CustomerName),
 *        SUM(OrderValue)
 * FROM Orders
 * GROUP BY CustomerID
 * }</pre>
 */
public class AnyValueAggregationFunction extends NullableSingleInputAggregationFunction<Object, Comparable<?>> {
  private ColumnDataType _resultType;

  private void ensureResultType(BlockValSet bvs) {
    if (_resultType != null) {
      return;
    }
    switch (bvs.getValueType().getStoredType()) {
      case INT:
        _resultType = ColumnDataType.INT;
        return;
      case LONG:
        _resultType = ColumnDataType.LONG;
        return;
      case FLOAT:
        _resultType = ColumnDataType.FLOAT;
        return;
      case DOUBLE:
        _resultType = ColumnDataType.DOUBLE;
        return;
      case STRING:
        _resultType = ColumnDataType.STRING;
        return;
      case BIG_DECIMAL:
        _resultType = ColumnDataType.BIG_DECIMAL;
        return;
      case BYTES:
        throw new UnsupportedOperationException("ANY_VALUE does not support BYTES ");
      default:
        throw new IllegalStateException("ANY_VALUE unsupported type: " + bvs.getValueType());
    }
  }

  public AnyValueAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "ANY_VALUE"), nullHandlingEnabled);
    _resultType = null;
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
  public ColumnDataType getIntermediateResultColumnType() {
    return _resultType != null ? _resultType : ColumnDataType.STRING;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return _resultType != null ? _resultType : ColumnDataType.STRING;
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
  public Comparable<?> extractFinalResult(Object intermediateResult) {
    return (Comparable<?>) intermediateResult;
  }

  @Override
  public Object merge(Object left, Object right) {
    return left != null ? left : right;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder holder,
                        Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (holder.getResult() != null) {
      return;
    }

    BlockValSet bvs = blockValSetMap.get(_expression);
    ensureResultType(bvs);

    switch (bvs.getValueType().getStoredType()) {
      case INT:
        aggregateForInt(length, holder, bvs, bvs.getIntValuesSV());
        break;
      case LONG:
        aggregateForLong(length, holder, bvs, bvs.getLongValuesSV());
        break;
      case FLOAT:
        aggregateForFloat(length, holder, bvs, bvs.getFloatValuesSV());
        break;
      case DOUBLE:
        aggregateForDouble(length, holder, bvs, bvs.getDoubleValuesSV());
        break;
      case STRING:
        aggregateForObjectType(length, holder, bvs, bvs.getStringValuesSV());
        break;
      case BIG_DECIMAL:
        aggregateForObjectType(length, holder, bvs, bvs.getBigDecimalValuesSV());
        break;
      default:
        throw new IllegalStateException("ANY_VALUE unsupported type: " + bvs.getValueType());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureResultType(bvs);

    switch (bvs.getValueType().getStoredType()) {
      case INT:
        aggregateGroupBySVForInt(length, groupKeys, holder, bvs, bvs.getIntValuesSV());
        break;
      case LONG:
        aggregateGroupBySVForLong(length, groupKeys, holder, bvs, bvs.getLongValuesSV());
        break;
      case FLOAT:
        aggregateGroupBySVForFloat(length, groupKeys, holder, bvs, bvs.getFloatValuesSV());
        break;
      case DOUBLE:
        aggregateGroupBySVForDouble(length, groupKeys, holder, bvs, bvs.getDoubleValuesSV());
        break;
      case STRING:
        aggregateGroupBySVForObjectType(length, groupKeys, holder, bvs, bvs.getStringValuesSV());
        break;
      case BIG_DECIMAL:
        aggregateGroupBySVForObjectType(length, groupKeys, holder, bvs, bvs.getBigDecimalValuesSV());
        break;
      default:
        throw new IllegalStateException("ANY_VALUE unsupported type: " + bvs.getValueType());
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureResultType(bvs);

    switch (bvs.getValueType().getStoredType()) {
      case INT:
        aggregateGroupByMVForInt(length, groupKeysArray, holder, bvs, bvs.getIntValuesSV());
        break;
      case LONG:
        aggregateGroupByMVForLong(length, groupKeysArray, holder, bvs, bvs.getLongValuesSV());
        break;
      case FLOAT:
        aggregateGroupByMVForFloat(length, groupKeysArray, holder, bvs, bvs.getFloatValuesSV());
        break;
      case DOUBLE:
        aggregateGroupByMVForDouble(length, groupKeysArray, holder, bvs, bvs.getDoubleValuesSV());
        break;
      case STRING:
        aggregateGroupByMVForObjectType(length, groupKeysArray, holder, bvs, bvs.getStringValuesSV());
        break;
      case BIG_DECIMAL:
        aggregateGroupByMVForObjectType(length, groupKeysArray, holder, bvs, bvs.getBigDecimalValuesSV());
        break;
      default:
        throw new IllegalStateException("ANY_VALUE unsupported type: " + bvs.getValueType());
    }
  }

  // Functional interfaces for type-safe primitive operations without boxing
  @FunctionalInterface
  private interface PrimitiveValueSetter {
    void setValue(AggregationResultHolder holder, int index);
  }

  @FunctionalInterface
  private interface PrimitiveGroupSetter {
    void setValueForKey(GroupByResultHolder holder, int groupKey, int index);
  }

  // Unified helper methods using functional interfaces to eliminate duplication
  private void aggregateForInt(int length, AggregationResultHolder holder, BlockValSet bvs, int[] values) {
    aggregateForPrimitive(length, holder, bvs, (h, i) -> h.setValue(values[i]));
  }

  private void aggregateForLong(int length, AggregationResultHolder holder, BlockValSet bvs, long[] values) {
    aggregateForPrimitive(length, holder, bvs, (h, i) -> h.setValue(values[i]));
  }

  private void aggregateForFloat(int length, AggregationResultHolder holder, BlockValSet bvs, float[] values) {
    aggregateForPrimitive(length, holder, bvs, (h, i) -> h.setValue(values[i]));
  }

  private void aggregateForDouble(int length, AggregationResultHolder holder, BlockValSet bvs, double[] values) {
    aggregateForPrimitive(length, holder, bvs, (h, i) -> h.setValue(values[i]));
  }

  private void aggregateForPrimitive(int length, AggregationResultHolder holder, BlockValSet bvs,
                                     PrimitiveValueSetter setter) {
    if (bvs.getNullBitmap() == null) {
      setter.setValue(holder, 0);
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to && holder.getResult() == null; i++) {
          setter.setValue(holder, i);
        }
      });
    }
  }

  // Helper methods for object types (String[], BigDecimal[])
  private <T> void aggregateForObjectType(int length, AggregationResultHolder holder, BlockValSet bvs, T[] values) {
    if (bvs.getNullBitmap() == null) {
      holder.setValue(values[0]);
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to && holder.getResult() == null; i++) {
          T value = values[i];
          if (value != null) {
            holder.setValue(value);
            break;
          }
        }
      });
    }
  }

  private void aggregateGroupBySVForInt(int length, int[] groupKeys, GroupByResultHolder holder, BlockValSet bvs,
                                        int[] values) {
    aggregateGroupBySVForPrimitive(length, groupKeys, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupBySVForLong(int length, int[] groupKeys, GroupByResultHolder holder, BlockValSet bvs,
                                         long[] values) {
    aggregateGroupBySVForPrimitive(length, groupKeys, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupBySVForFloat(int length, int[] groupKeys, GroupByResultHolder holder, BlockValSet bvs,
                                          float[] values) {
    aggregateGroupBySVForPrimitive(length, groupKeys, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupBySVForDouble(int length, int[] groupKeys, GroupByResultHolder holder, BlockValSet bvs,
                                           double[] values) {
    aggregateGroupBySVForPrimitive(length, groupKeys, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupBySVForPrimitive(int length, int[] groupKeys, GroupByResultHolder holder, BlockValSet bvs,
                                              PrimitiveGroupSetter setter) {
    if (!hasAnyMissingGroups(length, groupKeys, holder)) {
      return;
    }

    if (bvs.getNullBitmap() == null) {
      for (int i = 0; i < length; i++) {
        int g = groupKeys[i];
        if (holder.getResult(g) == null) {
          setter.setValueForKey(holder, g, i);
        }
      }
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          int g = groupKeys[i];
          if (holder.getResult(g) == null) {
            setter.setValueForKey(holder, g, i);
          }
        }
      });
    }
  }

  private <T> void aggregateGroupBySVForObjectType(int length, int[] groupKeys, GroupByResultHolder holder,
                                                   BlockValSet bvs, T[] values) {
    if (!hasAnyMissingGroups(length, groupKeys, holder)) {
      return;
    }

    if (bvs.getNullBitmap() == null) {
      for (int i = 0; i < length; i++) {
        int g = groupKeys[i];
        if (holder.getResult(g) == null) {
          holder.setValueForKey(g, values[i]);
        }
      }
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          int g = groupKeys[i];
          if (holder.getResult(g) == null) {
            T value = values[i];
            if (value != null) {
              holder.setValueForKey(g, value);
            }
          }
        }
      });
    }
  }

  private void aggregateGroupByMVForInt(int length, int[][] groupKeysArray, GroupByResultHolder holder, BlockValSet bvs,
                                        int[] values) {
    aggregateGroupByMVForPrimitive(length, groupKeysArray, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupByMVForLong(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                         BlockValSet bvs, long[] values) {
    aggregateGroupByMVForPrimitive(length, groupKeysArray, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupByMVForFloat(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                          BlockValSet bvs, float[] values) {
    aggregateGroupByMVForPrimitive(length, groupKeysArray, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupByMVForDouble(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                           BlockValSet bvs, double[] values) {
    aggregateGroupByMVForPrimitive(length, groupKeysArray, holder, bvs, (h, g, i) -> h.setValueForKey(g, values[i]));
  }

  private void aggregateGroupByMVForPrimitive(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                              BlockValSet bvs, PrimitiveGroupSetter setter) {
    if (!hasAnyMissingGroupsMV(length, groupKeysArray, holder)) {
      return;
    }

    if (bvs.getNullBitmap() == null) {
      for (int i = 0; i < length; i++) {
        int[] keys = groupKeysArray[i];
        for (int g : keys) {
          if (holder.getResult(g) == null) {
            setter.setValueForKey(holder, g, i);
          }
        }
      }
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          int[] keys = groupKeysArray[i];
          for (int g : keys) {
            if (holder.getResult(g) == null) {
              setter.setValueForKey(holder, g, i);
            }
          }
        }
      });
    }
  }

  private <T> void aggregateGroupByMVForObjectType(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                                   BlockValSet bvs, T[] values) {
    if (!hasAnyMissingGroupsMV(length, groupKeysArray, holder)) {
      return;
    }

    if (bvs.getNullBitmap() == null) {
      for (int i = 0; i < length; i++) {
        int[] keys = groupKeysArray[i];
        for (int g : keys) {
          if (holder.getResult(g) == null) {
            holder.setValueForKey(g, values[i]);
          }
        }
      }
    } else {
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          int[] keys = groupKeysArray[i];
          for (int g : keys) {
            if (holder.getResult(g) == null) {
              T value = values[i];
              if (value != null) {
                holder.setValueForKey(g, value);
              }
            }
          }
        }
      });
    }
  }

  // Utility methods to check for missing groups (extracted for reuse)
  private boolean hasAnyMissingGroups(int length, int[] groupKeys, GroupByResultHolder holder) {
    for (int i = 0; i < length; i++) {
      if (holder.getResult(groupKeys[i]) == null) {
        return true;
      }
    }
    return false;
  }

  private boolean hasAnyMissingGroupsMV(int length, int[][] groupKeysArray, GroupByResultHolder holder) {
    for (int i = 0; i < length; i++) {
      for (int g : groupKeysArray[i]) {
        if (holder.getResult(g) == null) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object value) {
    if (value == null) {
      return new SerializedIntermediateResult(0, new byte[0]);
    }

    byte[] bytes = serializeValue(value);
    return new SerializedIntermediateResult(1, bytes);
  }

  @Override
  public Object deserializeIntermediateResult(CustomObject customObject) {
    if (customObject.getBuffer().remaining() == 0) {
      return null;
    }
    return deserializeValue(customObject.getBuffer());
  }

  /**
   * Custom serialization for ANY_VALUE that handles all supported data types efficiently
   */
  private byte[] serializeValue(Object value) {
    if (value == null) {
      return new byte[]{0}; // Type 0 = null
    }

    if (value instanceof Integer) {
      ByteBuffer buffer = ByteBuffer.allocate(5); // 1 byte type + 4 bytes int
      buffer.put((byte) 1); // Type 1 = Integer
      buffer.putInt((Integer) value);
      return buffer.array();
    } else if (value instanceof Long) {
      ByteBuffer buffer = ByteBuffer.allocate(9); // 1 byte type + 8 bytes long
      buffer.put((byte) 2); // Type 2 = Long
      buffer.putLong((Long) value);
      return buffer.array();
    } else if (value instanceof Float) {
      ByteBuffer buffer = ByteBuffer.allocate(5); // 1 byte type + 4 bytes float
      buffer.put((byte) 3); // Type 3 = Float
      buffer.putFloat((Float) value);
      return buffer.array();
    } else if (value instanceof Double) {
      ByteBuffer buffer = ByteBuffer.allocate(9); // 1 byte type + 8 bytes double
      buffer.put((byte) 4); // Type 4 = Double
      buffer.putDouble((Double) value);
      return buffer.array();
    } else if (value instanceof String) {
      byte[] stringBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(5 + stringBytes.length); // 1 byte type + 4 bytes length + string
      buffer.put((byte) 5); // Type 5 = String
      buffer.putInt(stringBytes.length);
      buffer.put(stringBytes);
      return buffer.array();
    } else if (value instanceof BigDecimal) {
      String bigDecimalStr = value.toString();
      byte[] stringBytes = bigDecimalStr.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(5 + stringBytes.length); // 1 byte type + 4 bytes length + string
      buffer.put((byte) 6); // Type 6 = BigDecimal
      buffer.putInt(stringBytes.length);
      buffer.put(stringBytes);
      return buffer.array();
    } else {
      // Fallback to string representation for unknown types
      String stringValue = value.toString();
      byte[] stringBytes = stringValue.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(5 + stringBytes.length); // 1 byte type + 4 bytes length + string
      buffer.put((byte) 7); // Type 7 = Object toString
      buffer.putInt(stringBytes.length);
      buffer.put(stringBytes);
      return buffer.array();
    }
  }

  /**
   * Custom deserialization for ANY_VALUE that handles all supported data types efficiently
   */
  private Object deserializeValue(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return null;
    }

    byte type = buffer.get();
    switch (type) {
      case 0: // null
        return null;
      case 1: // Integer
        return buffer.getInt();
      case 2: // Long
        return buffer.getLong();
      case 3: // Float
        return buffer.getFloat();
      case 4: // Double
        return buffer.getDouble();
      case 5: // String
        int stringLength = buffer.getInt();
        byte[] stringBytes = new byte[stringLength];
        buffer.get(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
      case 6: // BigDecimal
        int bigDecimalLength = buffer.getInt();
        byte[] bigDecimalBytes = new byte[bigDecimalLength];
        buffer.get(bigDecimalBytes);
        return new BigDecimal(new String(bigDecimalBytes, StandardCharsets.UTF_8));
      case 7: // Object toString fallback
        int objectLength = buffer.getInt();
        byte[] objectBytes = new byte[objectLength];
        buffer.get(objectBytes);
        return new String(objectBytes, StandardCharsets.UTF_8);
      default:
        throw new IllegalStateException("Unknown serialization type: " + type);
    }
  }
}
