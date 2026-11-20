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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils.SizedObjectSerDe;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/// Value aggregator for distinct `ARRAYAGG` (i.e. `arrayAgg(column, 'type', true)`) used by the star-tree index.
///
/// The aggregated value is the set of distinct (non-null) raw values seen under a star-tree node. Distinct semantics
/// make the aggregation associative and idempotent (merge = set-union), which is what the star-tree requires; the
/// non-distinct variant would need an unbounded per-node multiset and is intentionally not supported here.
///
/// The set is stored as a variable-length `BYTES` cell in the star-tree forward index: a leading element type tag,
/// then the set as written by the `ObjectSerDeUtils` codec of the element type, so the query-time `arrayAgg` function
/// reads the stored cell with the same codec after checking the tag. Primitive element types are kept in fastutil
/// primitive sets, the other types in object sets.
///
/// NOTE: Only single-value source columns are supported (enforced at star-tree build config validation time), so the
/// raw value handed in by the builder is always a single boxed scalar, never an array.
@SuppressWarnings({"rawtypes", "unchecked"})
public class ArrayAggDistinctValueAggregator implements ValueAggregator<Object, Set> {
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

  // Size of the leading element-type tag byte prepended to every serialized cell.
  private static final int TAG_BYTES = Byte.BYTES;

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
  public Set getInitialAggregatedValue(@Nullable Object rawValue) {
    return rawValue != null ? add(null, rawValue) : newSet();
  }

  @Override
  public Set applyRawValue(Set value, @Nullable Object rawValue) {
    return rawValue != null ? add(value, rawValue) : value;
  }

  @Override
  public Set applyAggregatedValue(Set value, Set aggregatedValue) {
    if (aggregatedValue.isEmpty()) {
      return value;
    }
    if (value.isEmpty()) {
      return cloneAggregatedValue(aggregatedValue);
    }
    value.addAll(aggregatedValue);
    return value;
  }

  @Override
  public Set cloneAggregatedValue(Set value) {
    if (value.isEmpty()) {
      return newSet();
    }
    assert _elementType != null;
    return switch (_elementType) {
      case INT -> new IntOpenHashSet((IntSet) value);
      case LONG -> new LongOpenHashSet((LongSet) value);
      case FLOAT -> new FloatOpenHashSet((FloatSet) value);
      case DOUBLE -> new DoubleOpenHashSet((DoubleSet) value);
      default -> new ObjectOpenHashSet<>(value);
    };
  }

  @Override
  public boolean isAggregatedValueFixedSize() {
    return false;
  }

  @Override
  public boolean isMaxAggregatedValueByteSizeDerivedFromSerialization() {
    return true;
  }

  /// Returns the size of the largest cell serialized so far.
  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(Set value) {
    // Layout: [1-byte element type tag][set codec payload]. The tag makes the cell self-describing so it can be
    // deserialized (during build re-ingestion) without aggregator instance state. Every set codec writes an empty set
    // as a zero count, so an empty set is written without its codec; an aggregator that never saw a value tags it LONG.
    byte[] bytes;
    if (value.isEmpty()) {
      bytes = new byte[TAG_BYTES + Integer.BYTES];
      bytes[0] = (_elementType != null ? _elementType : ElementType.LONG).getTag();
    } else {
      assert _elementType != null;
      SizedObjectSerDe<Set> serDe = setSerDe(_elementType);
      bytes = new byte[TAG_BYTES + serDe.getSerializedSize(value)];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.put(_elementType.getTag());
      serDe.serialize(value, buffer);
    }
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return bytes;
  }

  @Override
  public Set deserializeAggregatedValue(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    ElementType elementType = ElementType.fromTag(buffer.get());
    if (_elementType == null) {
      // A merge-only flow (e.g. re-merging stored cells) may deserialize before any raw value has been seen; the tag
      // pins the element type so subsequent values and serialization interpret elements correctly.
      _elementType = elementType;
    }
    return setSerDe(elementType).deserialize(buffer);
  }

  /// Returns an empty set for the element type, or an object set before the first value fixes the type.
  private Set newSet() {
    if (_elementType == null) {
      return new ObjectOpenHashSet<>();
    }
    return switch (_elementType) {
      case INT -> new IntOpenHashSet();
      case LONG -> new LongOpenHashSet();
      case FLOAT -> new FloatOpenHashSet();
      case DOUBLE -> new DoubleOpenHashSet();
      default -> new ObjectOpenHashSet<>();
    };
  }

  /// Adds `rawValue` to `value` and returns the set it was added to: `value` when it is a set of the element type,
  /// otherwise a new one. `value` is `null` for a new set, or may be the empty object set created before the first
  /// value fixed the element type.
  private Set add(@Nullable Set value, Object rawValue) {
    if (_elementType == null) {
      _elementType = inferElementType(rawValue);
    }
    return switch (_elementType) {
      case INT -> {
        IntSet set = value instanceof IntSet intSet ? intSet : new IntOpenHashSet();
        set.add((int) rawValue);
        yield set;
      }
      case LONG -> {
        LongSet set = value instanceof LongSet longSet ? longSet : new LongOpenHashSet();
        set.add((long) rawValue);
        yield set;
      }
      case FLOAT -> {
        FloatSet set = value instanceof FloatSet floatSet ? floatSet : new FloatOpenHashSet();
        set.add((float) rawValue);
        yield set;
      }
      case DOUBLE -> {
        DoubleSet set = value instanceof DoubleSet doubleSet ? doubleSet : new DoubleOpenHashSet();
        set.add((double) rawValue);
        yield set;
      }
      case BYTES -> {
        Set set = value != null ? value : new ObjectOpenHashSet<>();
        // Normalize BYTES to ByteArray so hashCode/equals work as a set key
        set.add(rawValue instanceof byte[] bytes ? new ByteArray(bytes) : rawValue);
        yield set;
      }
      default -> {
        Set set = value != null ? value : new ObjectOpenHashSet<>();
        set.add(rawValue);
        yield set;
      }
    };
  }

  private static ElementType inferElementType(Object rawValue) {
    return switch (rawValue) {
      case Integer _ -> ElementType.INT;
      case Long _ -> ElementType.LONG;
      case Float _ -> ElementType.FLOAT;
      case Double _ -> ElementType.DOUBLE;
      case BigDecimal _ -> ElementType.BIG_DECIMAL;
      case String _ -> ElementType.STRING;
      case byte[] _, ByteArray _ -> ElementType.BYTES;
      default -> throw new IllegalStateException(
          "Unsupported data type for arrayAgg star-tree aggregation: " + rawValue.getClass().getSimpleName());
    };
  }

  /// Returns the codec of the sets of an element type.
  private static SizedObjectSerDe<Set> setSerDe(ElementType elementType) {
    SizedObjectSerDe<? extends Set> setSerDe = switch (elementType) {
      case INT -> ObjectSerDeUtils.INT_SET_SER_DE;
      case LONG -> ObjectSerDeUtils.LONG_SET_SER_DE;
      case FLOAT -> ObjectSerDeUtils.FLOAT_SET_SER_DE;
      case DOUBLE -> ObjectSerDeUtils.DOUBLE_SET_SER_DE;
      case BIG_DECIMAL -> ObjectSerDeUtils.BIG_DECIMAL_SET_SER_DE;
      case STRING -> ObjectSerDeUtils.STRING_SET_SER_DE;
      case BYTES -> ObjectSerDeUtils.BYTES_SET_SER_DE;
    };
    return (SizedObjectSerDe<Set>) setSerDe;
  }
}
