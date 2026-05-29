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

import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
 *
 * @param <R> Type of the raw value (non-aggregated)
 * @param <A> Type of the aggregated value
 */
public interface ValueAggregator<R, A> {

  /**
   * Returns the type of the aggregation.
   */
  AggregationFunctionType getAggregationType();

  /**
   * Returns the data type of the aggregated value.
   */
  DataType getAggregatedValueType();

  /**
   * Returns the initial aggregated value.
   * <p>NOTE: rawValue can be null when the aggregator is used for ingestion aggregation, and the column is not
   * specified in the schema.
   */
  A getInitialAggregatedValue(@Nullable R rawValue);

  /**
   * Applies a raw value to the current aggregated value.
   * <p>NOTE: if value is mutable, will directly modify the value.
   */
  A applyRawValue(A value, R rawValue);

  /**
   * Applies a raw value to the current aggregated value, reading from a {@link ByteBuffer} view of
   * the serialized payload.
   *
   * <p>The default implementation drains the buffer into a {@code byte[]} and delegates to
   * {@link #applyRawValue}, preserving source compatibility for implementors that only handle
   * byte-array raw values. Sketch implementations override to consume the buffer directly via
   * {@code Memory.wrap(ByteBuffer)}, avoiding the per-call {@code byte[]} allocation.
   *
   * <p>The implementation MUST drain the buffer's remaining bytes before returning. The caller
   * may invalidate the buffer immediately after the call (see the lifetime contract on
   * {@link org.apache.pinot.segment.spi.index.reader.ForwardIndexReader#getBytesView}).
   *
   * <p>This method is only meaningful for aggregators whose raw values are byte payloads
   * ({@code R = byte[]} or {@code R = Object} with {@code byte[]} dispatch). Aggregators with
   * non-byte raw types should not be invoked through this method.
   */
  @SuppressWarnings("unchecked")
  default A applyRawValueFromBuffer(A value, ByteBuffer buf) {
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return applyRawValue(value, (R) bytes);
  }

  /**
   * Applies an aggregated value to the current aggregated value.
   * <p>NOTE: if value is mutable, will directly modify the value.
   */
  A applyAggregatedValue(A value, A aggregatedValue);

  /**
   * Applies an aggregated value to the current aggregated value, reading from a {@link ByteBuffer}
   * view of a serialized aggregated value.
   *
   * <p>The default implementation drains the buffer, deserializes via
   * {@link #deserializeAggregatedValue(byte[])}, and delegates to {@link #applyAggregatedValue}.
   * Sketch implementations override to consume the buffer directly. The same lifetime contract
   * applies: the buffer must be consumed before this method returns.
   */
  default A applyAggregatedValueFromBuffer(A value, ByteBuffer buf) {
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return applyAggregatedValue(value, deserializeAggregatedValue(bytes));
  }

  /**
   * Clones an aggregated value.
   */
  A cloneAggregatedValue(A value);

  /**
   * Returns whether the aggregated value is of fixed size. Value aggregator can be used for ingestion aggregation only
   * when the aggregated value is of fixed size.
   */
  boolean isAggregatedValueFixedSize();

  /**
   * Returns the maximum size in bytes of the aggregated values seen so far.
   */
  int getMaxAggregatedValueByteSize();

  /**
   * Serializes an aggregated value into a byte array.
   */
  byte[] serializeAggregatedValue(A value);

  /**
   * De-serializes an aggregated value from a byte array.
   */
  A deserializeAggregatedValue(byte[] bytes);
}
