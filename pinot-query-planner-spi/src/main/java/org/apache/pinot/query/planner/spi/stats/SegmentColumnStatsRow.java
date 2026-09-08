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
package org.apache.pinot.query.planner.spi.stats;

import javax.annotation.Nullable;

/// Per-column statistics for a single segment, as persisted in the broker-local [StatsStore].
///
/// Min/max values are serialized as strings so heterogeneous column types share one schema; the
/// ordering to compare them under is carried separately by `valueType`, because it cannot be
/// recovered from the text. Unknown numeric fields are represented by `-1`.
///
/// Prefer [#builder()] over the canonical constructor: the components include two adjacent
/// strings (min/max) and two adjacent doubles (avg bytes / null fraction), so a transposition
/// compiles silently and corrupts statistics.
///
/// @param segmentName      the segment name
/// @param columnName       the column name
/// @param ndv              number of distinct values, or `-1` if unknown
/// @param minValue         minimum value as text, or `null` if unknown
/// @param maxValue         maximum value as text, or `null` if unknown
/// @param minTrusted       `false` when the minimum may be polluted by a null sentinel
/// @param avgBytesPerValue average encoded size per value, or `-1` if unknown
/// @param nullFraction     fraction of null values in `[0, 1]`, or `-1` if unknown
/// @param valueType        how min/max must be ordered, or `null` if unknown
public record SegmentColumnStatsRow(String segmentName, String columnName, long ndv, @Nullable String minValue,
                                    @Nullable String maxValue, boolean minTrusted, double avgBytesPerValue,
                                    double nullFraction, @Nullable ColumnValueType valueType) {

  public static Builder builder() {
    return new Builder();
  }

  /// Names each component at the call site, so no pair of same-typed fields can be swapped
  /// silently. Unset numeric fields default to the `-1` unknown sentinel.
  ///
  /// Thread-safety: not thread-safe; use from a single thread.
  public static final class Builder {
    private String _segmentName;
    private String _columnName;
    private long _ndv = -1;
    @Nullable
    private String _minValue;
    @Nullable
    private String _maxValue;
    private boolean _minTrusted = true;
    private double _avgBytesPerValue = -1;
    private double _nullFraction = -1;
    @Nullable
    private ColumnValueType _valueType;

    public Builder segmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder columnName(String columnName) {
      _columnName = columnName;
      return this;
    }

    public Builder ndv(long ndv) {
      _ndv = ndv;
      return this;
    }

    /// Bounds and the ordering they must be compared under, set together because a bound whose
    /// ordering is unknown cannot be used.
    public Builder bounds(@Nullable String minValue, @Nullable String maxValue,
        @Nullable ColumnValueType valueType) {
      _minValue = minValue;
      _maxValue = maxValue;
      _valueType = valueType;
      return this;
    }

    public Builder minTrusted(boolean minTrusted) {
      _minTrusted = minTrusted;
      return this;
    }

    public Builder avgBytesPerValue(double avgBytesPerValue) {
      _avgBytesPerValue = avgBytesPerValue;
      return this;
    }

    public Builder nullFraction(double nullFraction) {
      _nullFraction = nullFraction;
      return this;
    }

    public SegmentColumnStatsRow build() {
      return new SegmentColumnStatsRow(_segmentName, _columnName, _ndv, _minValue, _maxValue, _minTrusted,
          _avgBytesPerValue, _nullFraction, _valueType);
    }
  }
}
