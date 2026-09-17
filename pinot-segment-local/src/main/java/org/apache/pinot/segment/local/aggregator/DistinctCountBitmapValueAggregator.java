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

import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapLazyUnion;


/// For serialized-bitmap (`byte[]`) raw values, the aggregated value is unioned lazily and repaired in
/// [#serializeAggregatedValue]. A given metric column always provides either serialized bitmaps or plain raw values,
/// never both, so the [#addToValue] path never observes a lazy accumulator. The `_maxByteSize` tracking stays valid
/// on a lazy accumulator: serialized container sizes do not depend on the deferred cardinality values, and a lazy
/// bitmap container measures at its full fixed size, which upper-bounds the size after repair.
public class DistinctCountBitmapValueAggregator implements ValueAggregator<Object, RoaringBitmap> {
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
  public RoaringBitmap getInitialAggregatedValue(Object rawValue) {
    // NOTE: rawValue cannot be null because this aggregator can only be used for star-tree index.
    assert rawValue != null;
    RoaringBitmap initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = new RoaringBitmap();
      addToValue(initialValue, rawValue);
      _maxByteSize = Math.max(_maxByteSize, initialValue.serializedSizeInBytes());
    }
    return initialValue;
  }

  @Override
  public RoaringBitmap applyRawValue(RoaringBitmap value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      RoaringBitmapLazyUnion.lazyOr(value, deserializeAggregatedValue((byte[]) rawValue));
    } else {
      addToValue(value, rawValue);
    }
    _maxByteSize = Math.max(_maxByteSize, value.serializedSizeInBytes());
    return value;
  }

  /// Adds a raw value (single value or multi-value array) to the RoaringBitmap.
  protected void addToValue(RoaringBitmap bitmap, Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        bitmap.add(value.hashCode());
      }
    } else {
      bitmap.add(rawValue.hashCode());
    }
  }

  @Override
  public RoaringBitmap applyAggregatedValue(RoaringBitmap value, RoaringBitmap aggregatedValue) {
    // The input may itself be a lazy accumulator (e.g. an on-heap star-tree record built through applyRawValue);
    // repair it before the union because lazy unions require a non-lazy input
    RoaringBitmapLazyUnion.repair(aggregatedValue);
    RoaringBitmapLazyUnion.lazyOr(value, aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, value.serializedSizeInBytes());
    return value;
  }

  @Override
  public RoaringBitmap cloneAggregatedValue(RoaringBitmap value) {
    return value.clone();
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
  public byte[] serializeAggregatedValue(RoaringBitmap value) {
    RoaringBitmapLazyUnion.repair(value);
    return RoaringBitmapUtils.serialize(value);
  }

  @Override
  public RoaringBitmap deserializeAggregatedValue(byte[] bytes) {
    return RoaringBitmapUtils.deserialize(bytes);
  }
}
