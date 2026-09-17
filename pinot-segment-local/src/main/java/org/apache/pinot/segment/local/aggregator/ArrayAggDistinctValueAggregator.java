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
package org.apache.pinot.segment.local.aggregator;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/// Value aggregator for distinct `ARRAYAGG` (i.e. `arrayAgg(column, 'type', true)`) used by the star-tree index.
///
/// The aggregated value is the set of distinct (non-null) raw values seen under a star-tree node. Distinct semantics
/// make the aggregation associative and idempotent (merge = set-union), which is what the star-tree requires; the
/// non-distinct variant would need an unbounded per-node multiset and is intentionally not supported here.
///
/// The set is stored as a variable-length `BYTES` cell in the star-tree forward index. The serialized layout is
/// intentionally byte-for-byte identical to `ObjectSerDeUtils.*_SET_SER_DE` (in pinot-core) so the query-time
/// `arrayAgg` function can deserialize the stored cell directly. The wire layout cannot be shared as code because
/// pinot-segment-local must not depend on pinot-core; any change here must be mirrored there.
///
/// NOTE: Only single-value source columns are supported (enforced at star-tree build config validation time), so the
/// raw value handed in by the builder is always a single boxed scalar, never an array.
public class ArrayAggDistinctValueAggregator implements ValueAggregator<Object, ObjectSet<Object>> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  // Runtime element kind, inferred lazily from the first raw value (mirrors DistinctCountThetaSketchValueAggregator's
  // runtime dispatch). All values in a given aggregator instance come from the same column, so the kind is stable.
  // Each value carries an explicit stable on-disk `tag` that is persisted as a leading byte in the serialized cell,
  // making deserialization self-describing (independent of aggregator instance state, needed for build re-ingestion).
  // The query-time arrayAgg function reads and validates the same tag before decoding the payload.
  //
  // IMPORTANT: tags are an on-disk format. Never change or reuse an existing value's tag; only append new values with
  // new tags. Reordering the enum is safe because the tag, not the ordinal, is persisted.
  public enum ElementType {
    INT(0), LONG(1), FLOAT(2), DOUBLE(3), BIG_DECIMAL(4), STRING(5), BYTES(6);

    private final byte _tag;

    ElementType(int tag) {
      _tag = (byte) tag;
    }

    public byte getTag() {
      return _tag;
    }

    private static final ElementType[] BY_TAG;

    static {
      ElementType[] values = values();
      int maxTag = 0;
      for (ElementType value : values) {
        maxTag = Math.max(maxTag, value._tag);
      }
      BY_TAG = new ElementType[maxTag + 1];
      for (ElementType value : values) {
        BY_TAG[value._tag] = value;
      }
    }

    public static ElementType fromTag(byte tag) {
      if (tag < 0 || tag >= BY_TAG.length || BY_TAG[tag] == null) {
        throw new IllegalStateException("Invalid arrayAgg element type tag: " + tag);
      }
      return BY_TAG[tag];
    }
  }

  @Nullable
  private ElementType _elementType;
  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.ARRAYAGG;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public ObjectSet<Object> getInitialAggregatedValue(@Nullable Object rawValue) {
    ObjectSet<Object> set = new ObjectOpenHashSet<>();
    if (rawValue != null) {
      addRawValue(set, rawValue);
    }
    updateMaxByteSize(set);
    return set;
  }

  @Override
  public ObjectSet<Object> applyRawValue(ObjectSet<Object> value, Object rawValue) {
    if (rawValue != null) {
      addRawValue(value, rawValue);
      updateMaxByteSize(value);
    }
    return value;
  }

  @Override
  public ObjectSet<Object> applyAggregatedValue(ObjectSet<Object> value, ObjectSet<Object> aggregatedValue) {
    value.addAll(aggregatedValue);
    updateMaxByteSize(value);
    return value;
  }

  @Override
  public ObjectSet<Object> cloneAggregatedValue(ObjectSet<Object> value) {
    return new ObjectOpenHashSet<>(value);
  }

  @Override
  public boolean isAggregatedValueFixedSize() {
    return false;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(ObjectSet<Object> value) {
    // Layout: [1-byte element type tag][int size][payload]. The tag makes the cell self-describing so it can be
    // deserialized (during build re-ingestion) without aggregator instance state. An all-empty aggregator that never
    // saw a value defaults to LONG; the payload is empty either way so the tag choice is immaterial for empty cells.
    ElementType elementType = _elementType != null ? _elementType : ElementType.LONG;
    switch (elementType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return serializeFixedWidth(value, elementType);
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
        return serializeVariableWidth(value, elementType);
      default:
        throw new IllegalStateException("Unsupported element type: " + elementType);
    }
  }

  @Override
  public ObjectSet<Object> deserializeAggregatedValue(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    ElementType elementType = ElementType.fromTag(byteBuffer.get());
    int size = byteBuffer.getInt();
    ObjectSet<Object> set = new ObjectOpenHashSet<>(size);
    for (int i = 0; i < size; i++) {
      switch (elementType) {
        case INT:
          set.add(byteBuffer.getInt());
          break;
        case LONG:
          set.add(byteBuffer.getLong());
          break;
        case FLOAT:
          set.add(byteBuffer.getFloat());
          break;
        case DOUBLE:
          set.add(byteBuffer.getDouble());
          break;
        case BIG_DECIMAL: {
          byte[] valueBytes = readLengthPrefixed(byteBuffer);
          set.add(BigDecimalUtils.deserialize(valueBytes));
          break;
        }
        case STRING: {
          byte[] valueBytes = readLengthPrefixed(byteBuffer);
          set.add(new String(valueBytes, StandardCharsets.UTF_8));
          break;
        }
        case BYTES: {
          byte[] valueBytes = readLengthPrefixed(byteBuffer);
          set.add(new ByteArray(valueBytes));
          break;
        }
        default:
          throw new IllegalStateException("Unsupported element type: " + elementType);
      }
    }
    return set;
  }

  private void addRawValue(ObjectSet<Object> set, Object rawValue) {
    if (_elementType == null) {
      _elementType = inferElementType(rawValue);
    }
    // Normalize BYTES to ByteArray so hashCode/equals work as a set key; other types are already value types.
    if (rawValue instanceof byte[]) {
      set.add(new ByteArray((byte[]) rawValue));
    } else {
      set.add(rawValue);
    }
  }

  private static ElementType inferElementType(Object rawValue) {
    if (rawValue instanceof Integer) {
      return ElementType.INT;
    } else if (rawValue instanceof Long) {
      return ElementType.LONG;
    } else if (rawValue instanceof Float) {
      return ElementType.FLOAT;
    } else if (rawValue instanceof Double) {
      return ElementType.DOUBLE;
    } else if (rawValue instanceof BigDecimal) {
      return ElementType.BIG_DECIMAL;
    } else if (rawValue instanceof String) {
      return ElementType.STRING;
    } else if (rawValue instanceof byte[] || rawValue instanceof ByteArray) {
      return ElementType.BYTES;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for arrayAgg star-tree aggregation: " + rawValue.getClass().getSimpleName());
    }
  }

  private void updateMaxByteSize(ObjectSet<Object> value) {
    _maxByteSize = Math.max(_maxByteSize, serializeAggregatedValue(value).length);
  }

  // Size of the leading element-type tag byte prepended to every serialized cell.
  private static final int TAG_BYTES = Byte.BYTES;

  private static byte[] serializeFixedWidth(ObjectSet<Object> value, ElementType elementType) {
    int size = value.size();
    int elementBytes;
    switch (elementType) {
      case INT:
      case FLOAT:
        elementBytes = Integer.BYTES;
        break;
      case LONG:
      case DOUBLE:
        elementBytes = Long.BYTES;
        break;
      default:
        throw new IllegalStateException("Not a fixed-width element type: " + elementType);
    }
    byte[] bytes = new byte[TAG_BYTES + Integer.BYTES + size * elementBytes];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.put(elementType.getTag());
    byteBuffer.putInt(size);
    for (Object element : value) {
      switch (elementType) {
        case INT:
          byteBuffer.putInt((Integer) element);
          break;
        case LONG:
          byteBuffer.putLong((Long) element);
          break;
        case FLOAT:
          byteBuffer.putFloat((Float) element);
          break;
        case DOUBLE:
          byteBuffer.putDouble((Double) element);
          break;
        default:
          throw new IllegalStateException("Not a fixed-width element type: " + elementType);
      }
    }
    return bytes;
  }

  private static byte[] serializeVariableWidth(ObjectSet<Object> value, ElementType elementType) {
    int size = value.size();
    byte[][] valueBytesArray = new byte[size][];
    long bufferSize = TAG_BYTES + (1 + (long) size) * Integer.BYTES;
    int index = 0;
    for (Object element : value) {
      byte[] valueBytes;
      switch (elementType) {
        case BIG_DECIMAL:
          // Must match ObjectSerDeUtils.BIG_DECIMAL_SET_SER_DE element encoding (scale + unscaled bytes), not
          // toString(), so the query-time function can deserialize the star-tree cell with BIG_DECIMAL_SET_SER_DE.
          valueBytes = BigDecimalUtils.serialize((BigDecimal) element);
          break;
        case STRING:
          valueBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
          break;
        case BYTES:
          valueBytes = ((ByteArray) element).getBytes();
          break;
        default:
          throw new IllegalStateException("Unsupported variable width element type: " + elementType);
      }
      bufferSize += valueBytes.length;
      valueBytesArray[index++] = valueBytes;
    }
    if (bufferSize > Integer.MAX_VALUE) {
      throw new IllegalStateException("Serialized arrayAgg set exceeds 2GB");
    }
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.put(elementType.getTag());
    byteBuffer.putInt(size);
    for (byte[] valueBytes : valueBytesArray) {
      byteBuffer.putInt(valueBytes.length);
      byteBuffer.put(valueBytes);
    }
    return bytes;
  }

  private static byte[] readLengthPrefixed(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    byte[] valueBytes = new byte[length];
    byteBuffer.get(valueBytes);
    return valueBytes;
  }
}
