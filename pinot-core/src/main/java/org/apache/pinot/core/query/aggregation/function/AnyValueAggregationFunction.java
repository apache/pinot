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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


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
  // Result type is determined at runtime based on input expression type
  private ColumnDataType _resultType;

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
    // Default to STRING if result type is not yet determined
    // TODO: See if UNKNOWN can be used instead
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
    // For ANY_VALUE, we just need any non-null value, so merge by returning the first non-null value
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
    aggregateHelper(length, bvs, (i, value) -> {
      holder.setValue(value);
      return true; // Stop after first value found
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureResultType(bvs);
    aggregateHelper(length, bvs, (i, value) -> {
      int g = groupKeys[i];
      if (holder.getResult(g) == null) {
        holder.setValueForKey(g, value);
      }
      return false; // Continue processing for other groups
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureResultType(bvs);
    aggregateHelper(length, bvs, (i, value) -> {
      int[] keys = groupKeysArray[i];
      for (int g : keys) {
        if (holder.getResult(g) == null) {
          holder.setValueForKey(g, value);
        }
      }
      return false; // Continue processing for other groups
    });
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

  @FunctionalInterface
  private interface ValueProcessor<T> {
    boolean process(int index, T value); // Returns true to continue processing, false to stop
  }

  /**
   * Generic helper for processing values with dictionary optimization for all supported data types
   */
  private void aggregateHelper(int length, BlockValSet bvs, ValueProcessor<Object> processor) {
    // Use dictionary-based access for efficiency when available
    if (bvs.getDictionary() != null) {
      final int[] dictIds = bvs.getDictionaryIdsSV();
      final Dictionary dict = bvs.getDictionary();
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          Object value = getDictionaryValue(dict, dictIds[i], bvs.getValueType().getStoredType());
          if (processor.process(i, value)) {
            break;
          }
        }
      });
    } else {
      // Fall back to direct value access based on type
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          Object value = getDirectValue(bvs, i);
          if (value != null && processor.process(i, value)) {
            break;
          }
        }
      });
    }
  }

  /**
   * Get value from dictionary based on data type
   */
  private Object getDictionaryValue(Dictionary dict, int dictId, FieldSpec.DataType storedType) {
    switch (storedType) {
      case INT:
        return dict.getIntValue(dictId);
      case LONG:
        return dict.getLongValue(dictId);
      case FLOAT:
        return dict.getFloatValue(dictId);
      case DOUBLE:
        return dict.getDoubleValue(dictId);
      case STRING:
        return dict.getStringValue(dictId);
      case BIG_DECIMAL:
        return dict.getBigDecimalValue(dictId);
      case BYTES:
        return dict.getBytesValue(dictId);
      default:
        throw new IllegalStateException("Unsupported dictionary type: " + storedType);
    }
  }

  /**
   * Get value directly from BlockValSet based on data type
   */
  private Object getDirectValue(BlockValSet bvs, int index) {
    switch (bvs.getValueType().getStoredType()) {
      case INT:
        return bvs.getIntValuesSV()[index];
      case LONG:
        return bvs.getLongValuesSV()[index];
      case FLOAT:
        return bvs.getFloatValuesSV()[index];
      case DOUBLE:
        return bvs.getDoubleValuesSV()[index];
      case STRING:
        return bvs.getStringValuesSV()[index];
      case BIG_DECIMAL:
        return bvs.getBigDecimalValuesSV()[index];
      case BYTES:
        return bvs.getBytesValuesSV()[index];
      default:
        throw new IllegalStateException("Unsupported direct access type: " + bvs.getValueType().getStoredType());
    }
  }

  /**
   * Custom serialization for ANY_VALUE that handles all supported data types efficiently
   */
  private byte[] serializeValue(Object value) {
    if (value == null) {
      return new byte[]{0}; // Type 0 = null
    }

    if (value instanceof Integer) {
      return serializeFixedValue((byte) 1, 4, buffer -> buffer.putInt((Integer) value));
    } else if (value instanceof Long) {
      return serializeFixedValue((byte) 2, 8, buffer -> buffer.putLong((Long) value));
    } else if (value instanceof Float) {
      return serializeFixedValue((byte) 3, 4, buffer -> buffer.putFloat((Float) value));
    } else if (value instanceof Double) {
      return serializeFixedValue((byte) 4, 8, buffer -> buffer.putDouble((Double) value));
    } else if (value instanceof String) {
      return serializeVariableValue((byte) 5, ((String) value).getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof BigDecimal) {
      return serializeVariableValue((byte) 6, value.toString().getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof byte[]) {
      return serializeVariableValue((byte) 8, (byte[]) value);
    } else {
      // Fallback to string representation for unknown types
      return serializeVariableValue((byte) 7, value.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Helper method for serializing fixed-length values
   */
  private byte[] serializeFixedValue(byte typeId, int valueSize, java.util.function.Consumer<ByteBuffer> valueWriter) {
    ByteBuffer buffer = ByteBuffer.allocate(1 + valueSize); // 1 byte type + value bytes
    buffer.put(typeId);
    valueWriter.accept(buffer);
    return buffer.array();
  }

  /**
   * Helper method for serializing variable-length values
   */
  private byte[] serializeVariableValue(byte typeId, byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(5 + data.length); // 1 byte type + 4 bytes length + data
    buffer.put(typeId);
    buffer.putInt(data.length);
    buffer.put(data);
    return buffer.array();
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
        return new String(deserializeVariableBytes(buffer), StandardCharsets.UTF_8);
      case 6: // BigDecimal
        return new BigDecimal(new String(deserializeVariableBytes(buffer), StandardCharsets.UTF_8));
      case 7: // Object toString fallback
        return new String(deserializeVariableBytes(buffer), StandardCharsets.UTF_8);
      case 8: // BYTES
        return deserializeVariableBytes(buffer);
      default:
        throw new IllegalStateException("Unknown serialization type: " + type);
    }
  }

  /**
   * Helper method for deserializing variable-length byte arrays
   */
  private byte[] deserializeVariableBytes(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }

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
        _resultType = ColumnDataType.BYTES;
        return;
      default:
        throw new IllegalStateException("ANY_VALUE unsupported type: " + bvs.getValueType());
    }
  }
}
