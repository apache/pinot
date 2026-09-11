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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/// AnyValue aggregation function returns any arbitrary NON-NULL value from the column for each group.
///
/// This is useful for GROUP BY queries where you want to include a column in SELECT that has a 1:1 mapping with the
/// GROUP BY columns, avoiding the need to add it to GROUP BY clause. The implementation is null-aware and will scan
/// only until it finds the first non-null value in the current batch for each group/key. This makes it O(n) over the
/// input once per group until the first value is set, with early-exit fast paths when there are no nulls.
/// Bound instances have an immutable logical result type and can be shared across segment threads. The unbound
/// constructor retains historical stored-type inference for callers that do not supply query bindings.
///
/// **Example:**
///
/// ```
/// SELECT CustomerID,
///        ANY_VALUE(CustomerName),
///        SUM(OrderValue)
/// FROM Orders
/// GROUP BY CustomerID
/// ```
public class AnyValueAggregationFunction extends BaseSingleInputAggregationFunction<Object, Comparable<?>> {
  private static final DataType[] DATA_TYPE_VALUES = DataType.values();
  @Nullable
  private final ColumnDataType _resultType;
  @Nullable
  private ColumnDataType _legacyResultType;

  public AnyValueAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "ANY_VALUE"), nullHandlingEnabled);
    _resultType = null;
  }

  public AnyValueAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled,
      AggregateCallBinding binding) {
    super(verifySingleArgument(arguments, "ANY_VALUE"), nullHandlingEnabled);
    _resultType = binding.getResultType();
    Preconditions.checkArgument(binding.getArgumentTypes().size() == 1
            && (binding.getArgumentTypes().get(0) == _resultType
            || binding.getArgumentTypes().get(0) == ColumnDataType.JSON && _resultType == ColumnDataType.STRING),
        "ANY_VALUE result type must match its single input type");
    switch (_resultType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BIG_DECIMAL:
      case BYTES:
        break;
      default:
        throw new IllegalArgumentException("ANY_VALUE unsupported type: " + _resultType);
    }
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
    return getFinalResultColumnType().getStoredType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return _resultType != null ? _resultType : _legacyResultType != null ? _legacyResultType : ColumnDataType.STRING;
  }

  @Nullable
  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Nullable
  @Override
  public Comparable<?> extractFinalResult(@Nullable Object intermediateResult) {
    return (Comparable<?>) intermediateResult;
  }

  @Override
  public Object merge(Object left, Object right) {
    // Any of the two values will do, and both are real values here.
    return left;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder holder,
                        Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (holder.getResult() != null) {
      return;
    }
    BlockValSet bvs = blockValSetMap.get(_expression);
    ensureLegacyResultType(bvs);
    aggregateHelper(length, bvs, (i, value) -> {
      holder.setValue(toIntermediateValue(value));
      return true; // Stop after first value found
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureLegacyResultType(bvs);
    aggregateHelper(length, bvs, (i, value) -> {
      int g = groupKeys[i];
      if (holder.getResult(g) == null) {
        holder.setValueForKey(g, toIntermediateValue(value));
      }
      return false; // Continue processing for other groups
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder holder,
                                 Map<ExpressionContext, BlockValSet> map) {
    BlockValSet bvs = map.get(_expression);
    ensureLegacyResultType(bvs);
    aggregateHelper(length, bvs, (i, value) -> {
      int[] keys = groupKeysArray[i];
      for (int g : keys) {
        if (holder.getResult(g) == null) {
          value = toIntermediateValue(value);
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
    boolean process(int index, T value); // Returns true to stop processing, false to continue
  }

  /// Wrap bytes only when retaining a value, sharing the wrapper across accepting MV groups.
  private static Object toIntermediateValue(Object value) {
    return value instanceof byte[] ? new ByteArray((byte[]) value) : value;
  }

  /// Generic helper for processing values with dictionary optimization for all supported data types
  private void aggregateHelper(int length, BlockValSet bvs, ValueProcessor<Object> processor) {
    // A segment can retain an older physical type after schema evolution. Read through the bound type's conversion
    // getter so values have the representation promised by the intermediate schema before merging or serialization.
    DataType storedType = _resultType != null
        ? _resultType.getStoredType().toDataType()
        : bvs.getValueType().getStoredType();
    // Use dictionary-based access for efficiency when available
    if (bvs.isDictionaryEncoded()) {
      final int[] dictIds = bvs.getDictionaryIdsSV();
      final Dictionary dict = bvs.getDictionary();
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          Object value = getDictionaryValue(dict, dictIds[i], storedType);
          if (processor.process(i, value)) {
            break;
          }
        }
      });
    } else {
      // Fall back to direct value access based on type
      forEachNotNull(length, bvs, (from, to) -> {
        for (int i = from; i < to; i++) {
          Object value = getDirectValue(bvs, i, storedType);
          if (value != null && processor.process(i, value)) {
            break;
          }
        }
      });
    }
  }

  /// Get value from dictionary based on data type
  private Object getDictionaryValue(Dictionary dict, int dictId, DataType storedType) {
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

  /// Get value directly from BlockValSet based on data type
  private Object getDirectValue(BlockValSet bvs, int index, DataType storedType) {
    switch (storedType) {
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
        throw new IllegalStateException("Unsupported direct access type: " + storedType);
    }
  }

  /// Custom serialization for ANY_VALUE that handles all supported data types efficiently
  /// Note: value is never null - null is handled at the serializeIntermediateResult layer
  private byte[] serializeValue(Object value) {
    if (value instanceof Integer) {
      return serializeFixedValue(DataType.INT, 4, buffer -> buffer.putInt((Integer) value));
    } else if (value instanceof Long) {
      return serializeFixedValue(DataType.LONG, 8, buffer -> buffer.putLong((Long) value));
    } else if (value instanceof Float) {
      return serializeFixedValue(DataType.FLOAT, 4, buffer -> buffer.putFloat((Float) value));
    } else if (value instanceof Double) {
      return serializeFixedValue(DataType.DOUBLE, 8, buffer -> buffer.putDouble((Double) value));
    } else if (value instanceof String) {
      return serializeVariableValue(DataType.STRING, ((String) value).getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof BigDecimal) {
      return serializeVariableValue(DataType.BIG_DECIMAL, value.toString().getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof ByteArray) {
      return serializeVariableValue(DataType.BYTES, ((ByteArray) value).getBytes());
    } else if (value instanceof byte[]) {
      return serializeVariableValue(DataType.BYTES, (byte[]) value);
    } else {
      throw new IllegalStateException("Unsupported value type for serialization: " + value.getClass().getName());
    }
  }

  /// Helper method for serializing fixed-length values
  private byte[] serializeFixedValue(DataType dataType, int valueSize, Consumer<ByteBuffer> valueWriter) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + valueSize); // 4 bytes for type ordinal + value bytes
    buffer.putInt(dataType.ordinal());
    valueWriter.accept(buffer);
    return buffer.array();
  }

  /// Helper method for serializing variable-length values
  private byte[] serializeVariableValue(DataType dataType, byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(8 + data.length); // 4 bytes type ordinal + 4 bytes length + data
    buffer.putInt(dataType.ordinal());
    buffer.putInt(data.length);
    buffer.put(data);
    return buffer.array();
  }

  /// Custom deserialization for ANY_VALUE that handles all supported data types efficiently
  /// Note: empty buffer (null) is handled at the deserializeIntermediateResult layer
  private Object deserializeValue(ByteBuffer buffer) {
    int typeOrdinal = buffer.getInt();
    DataType dataType = DATA_TYPE_VALUES[typeOrdinal];
    switch (dataType) {
      case INT:
        return buffer.getInt();
      case LONG:
        return buffer.getLong();
      case FLOAT:
        return buffer.getFloat();
      case DOUBLE:
        return buffer.getDouble();
      case STRING:
        return new String(deserializeVariableBytes(buffer), StandardCharsets.UTF_8);
      case BIG_DECIMAL:
        return new BigDecimal(new String(deserializeVariableBytes(buffer), StandardCharsets.UTF_8));
      case BYTES:
        return new ByteArray(deserializeVariableBytes(buffer));
      default:
        throw new IllegalStateException("Unsupported data type for deserialization: " + dataType);
    }
  }

  /// Helper method for deserializing variable-length byte arrays
  private byte[] deserializeVariableBytes(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }

  private void ensureLegacyResultType(BlockValSet block) {
    if (_resultType == null && _legacyResultType == null) {
      _legacyResultType = ColumnDataType.fromDataType(block.getValueType().getStoredType(), true);
    }
  }

  /// Creates schema-bound ANY_VALUE implementations while retaining the historical constructor for unbound callers.
  public static class Provider implements AggregationFunctionProvider {
    @Override
    public AggregationFunctionType getType() {
      return AggregationFunctionType.ANYVALUE;
    }

    @Override
    public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
      AggregateCallBinding binding = function.getAggregationBinding();
      return binding != null
          ? new AnyValueAggregationFunction(function.getArguments(), nullHandlingEnabled, binding)
          : new AnyValueAggregationFunction(function.getArguments(), nullHandlingEnabled);
    }
  }
}
