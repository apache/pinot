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

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.customobject.tdigest.NonFiniteAwareTDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PercentileTDigestValueAggregator implements ValueAggregator<Object, TDigest> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  // TODO: This is copied from PercentileTDigestAggregationFunction.
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;
  private final int _compressionFactor;
  private int _maxByteSize;

  public PercentileTDigestValueAggregator(List<ExpressionContext> arguments) {
    if (!arguments.isEmpty()) {
      _compressionFactor = arguments.get(0).getLiteral().getIntValue();
    } else {
      _compressionFactor = DEFAULT_TDIGEST_COMPRESSION;
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public TDigest getInitialAggregatedValue(Object rawValue) {
    // NOTE: rawValue cannot be null because this aggregator can only be used for star-tree index.
    assert rawValue != null;
    TDigest initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
    } else {
      initialValue = NonFiniteAwareTDigest.forLegacyAggregation(_compressionFactor);
      addToDigest(initialValue, rawValue);
    }
    updateMaxByteSize(initialValue);
    return initialValue;
  }

  @Override
  public TDigest applyRawValue(TDigest value, Object rawValue) {
    value = asNonFiniteAware(value);
    if (rawValue instanceof byte[]) {
      value.add(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      addToDigest(value, rawValue);
    }
    updateMaxByteSize(value);
    return value;
  }

  /// Adds a raw value (single value or multi-value array) to the TDigest.
  protected void addToDigest(TDigest digest, Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        digest.add(ValueAggregatorUtils.toDouble(value));
      }
    } else {
      digest.add(ValueAggregatorUtils.toDouble(rawValue));
    }
  }

  @Override
  public TDigest applyAggregatedValue(TDigest value, TDigest aggregatedValue) {
    value = asNonFiniteAware(value);
    value.add(aggregatedValue);
    updateMaxByteSize(value);
    return value;
  }

  @Override
  public TDigest cloneAggregatedValue(TDigest value) {
    return asNonFiniteAware(value).copy();
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
  public byte[] serializeAggregatedValue(TDigest value) {
    NonFiniteAwareTDigest wrapped = asNonFiniteAware(value);
    updateMaxByteSize(wrapped);
    byte[] bytes = wrapped.serialize();
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return bytes;
  }

  @Override
  public TDigest deserializeAggregatedValue(byte[] bytes) {
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return NonFiniteAwareTDigest.fromBytes(bytes);
  }

  private static NonFiniteAwareTDigest asNonFiniteAware(TDigest value) {
    if (value instanceof NonFiniteAwareTDigest) {
      return (NonFiniteAwareTDigest) value;
    }
    NonFiniteAwareTDigest wrapper = NonFiniteAwareTDigest.forLegacyAggregation(value.compression());
    wrapper.add(value);
    return wrapper;
  }

  private void updateMaxByteSize(TDigest value) {
    _maxByteSize = Math.max(_maxByteSize, value.maxSerializedByteSize());
  }
}
