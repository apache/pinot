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

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
///
/// @param <R> Type of the raw value (non-aggregated)
/// @param <A> Type of the aggregated value
public interface ValueAggregator<R, A> {

  /// Returns the type of the aggregation.
  AggregationFunctionType getAggregationType();

  /// Returns the data type of the aggregated value.
  DataType getAggregatedValueType();

  /// Returns the initial aggregated value.
  ///
  /// NOTE: rawValue can be null when the aggregator is used for ingestion aggregation, and the column is not
  /// specified in the schema.
  A getInitialAggregatedValue(@Nullable R rawValue);

  /// Returns the aggregated value of a group whose input values are all null, or `null` to have the star-tree record
  /// the group in its null vector instead.
  ///
  /// Only consulted by null-aware star-trees, which exclude null input values from the pre-aggregation and can
  /// therefore produce a group with no values at all.
  ///
  /// Returning `null` is safe whenever the aggregation function skips null rows while reading the pre-aggregated
  /// column, which every aggregation function does apart from `COUNT`. A group recorded in the null vector is never
  /// read back, so the placeholder left in the forward index is never deserialized.
  ///
  /// `COUNT` is the exception and overrides this: it is read back by summing the pre-aggregated column rather than
  /// through the null vector, so it answers `0` itself. Every other aggregator takes the default, which keeps an
  /// all-null group down to a placeholder plus one null-vector bit instead of a serialized empty sketch.
  ///
  /// An aggregator whose [#getAggregatedValueType] is `BYTES` must also make [#getMaxAggregatedValueByteSize] account
  /// for the value returned here, because the star-tree sizes the variable-length forward index from that and would
  /// otherwise under-allocate for a metric whose every group is null.
  @Nullable
  default A getAllNullAggregatedValue() {
    return null;
  }

  /// Applies a raw value to the current aggregated value.
  ///
  /// NOTE: if value is mutable, will directly modify the value.
  A applyRawValue(A value, R rawValue);

  /// Applies an aggregated value to the current aggregated value.
  ///
  /// NOTE: if value is mutable, will directly modify the value.
  A applyAggregatedValue(A value, A aggregatedValue);

  /// Clones an aggregated value.
  A cloneAggregatedValue(A value);

  /// Returns whether the aggregated value is of fixed size. Value aggregator can be used for ingestion aggregation only
  /// when the aggregated value is of fixed size.
  boolean isAggregatedValueFixedSize();

  /// Returns the maximum size in bytes of the aggregated values seen so far.
  int getMaxAggregatedValueByteSize();

  /// Serializes an aggregated value into a byte array.
  byte[] serializeAggregatedValue(A value);

  /// De-serializes an aggregated value from a byte array.
  A deserializeAggregatedValue(byte[] bytes);
}
