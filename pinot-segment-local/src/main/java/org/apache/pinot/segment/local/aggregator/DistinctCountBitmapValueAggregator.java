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

import org.apache.pinot.common.utils.RoaringBitmapUnion;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Pre-aggregates distinct values into a [RoaringBitmapUnion], which unions serialized-bitmap (`byte[]`) inputs
/// lazily and finalizes the bitmap only when it is read or serialized. A given metric column always provides either
/// serialized bitmaps or plain raw values, never both.
///
/// [#getMaxAggregatedValueByteSize] reports the largest value produced by [#serializeAggregatedValue] so far. This
/// aggregator is only used by the star-tree builders (it has no fixed size, so ingestion-time aggregation rejects
/// it), and both builders serialize every record before that size is consumed: the off-heap builder when it appends
/// a record, the on-heap builder in its pre-serialization pass before the forward indexes are sized.
public class DistinctCountBitmapValueAggregator implements ValueAggregator<Object, RoaringBitmapUnion> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public RoaringBitmapUnion getInitialAggregatedValue(Object rawValue) {
    // NOTE: rawValue cannot be null because this aggregator can only be used for star-tree index, and the builder
    //   never passes a null raw value: a null-aware star-tree leaves the aggregated value null until the group sees
    //   its first non-null input.
    assert rawValue != null;
    if (rawValue instanceof byte[]) {
      return deserializeAggregatedValue((byte[]) rawValue);
    }
    RoaringBitmapUnion initialValue = new RoaringBitmapUnion();
    addToValue(initialValue, rawValue);
    return initialValue;
  }

  @Override
  public RoaringBitmapUnion applyRawValue(RoaringBitmapUnion value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.add(RoaringBitmapUtils.deserialize((byte[]) rawValue));
    } else {
      addToValue(value, rawValue);
    }
    return value;
  }

  /// Adds a raw value (single value or multi-value array) to the union.
  protected void addToValue(RoaringBitmapUnion union, Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        union.add(value.hashCode());
      }
    } else {
      union.add(rawValue.hashCode());
    }
  }

  @Override
  public RoaringBitmapUnion applyAggregatedValue(RoaringBitmapUnion value, RoaringBitmapUnion aggregatedValue) {
    // get() finalizes the other accumulator without consuming it; it stays usable (and is serialized later itself)
    value.add(aggregatedValue.get());
    return value;
  }

  @Override
  public RoaringBitmapUnion cloneAggregatedValue(RoaringBitmapUnion value) {
    RoaringBitmapUnion clone = new RoaringBitmapUnion();
    clone.add(value.get());
    return clone;
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
  public byte[] serializeAggregatedValue(RoaringBitmapUnion value) {
    byte[] bytes = RoaringBitmapUtils.serialize(value.get());
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return bytes;
  }

  @Override
  public RoaringBitmapUnion deserializeAggregatedValue(byte[] bytes) {
    return RoaringBitmapUtils.deserializeToUnion(bytes);
  }
}
