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
package org.apache.pinot.broker.stats;

import javax.annotation.Nullable;


/// Immutable value object holding per-column statistics for a single segment, as persisted in the
/// broker-local [StatsStore].
///
/// Min/max values are serialized as strings to allow storage of heterogeneous column types
/// without type-specific schema. Unknown numeric fields are represented by `-1`.
///
/// Thread-safety: immutable; safe for concurrent access.
public class SegmentColumnStatsRow {
  private final String _segmentName;
  private final String _columnName;
  private final long _ndv;
  @Nullable
  private final String _minValue;
  @Nullable
  private final String _maxValue;
  private final boolean _minTrusted;
  private final double _avgBytesPerValue;
  private final double _nullFraction;

  public SegmentColumnStatsRow(String segmentName, String columnName, long ndv,
      @Nullable String minValue, @Nullable String maxValue, boolean minTrusted,
      double avgBytesPerValue, double nullFraction) {
    _segmentName = segmentName;
    _columnName = columnName;
    _ndv = ndv;
    _minValue = minValue;
    _maxValue = maxValue;
    _minTrusted = minTrusted;
    _avgBytesPerValue = avgBytesPerValue;
    _nullFraction = nullFraction;
  }

  /// Returns the segment name.
  public String getSegmentName() {
    return _segmentName;
  }

  /// Returns the column name.
  public String getColumnName() {
    return _columnName;
  }

  /// Returns the estimated number of distinct values (NDV), or `-1` if unknown.
  public long getNdv() {
    return _ndv;
  }

  /// Returns the string-serialized minimum value for the column, or `null` if unknown.
  @Nullable
  public String getMinValue() {
    return _minValue;
  }

  /// Returns the string-serialized maximum value for the column, or `null` if unknown.
  @Nullable
  public String getMaxValue() {
    return _maxValue;
  }

  /// Returns `false` when the minimum value is polluted by the numeric null-sentinel default
  /// (i.e. the column is nullable and its stored min equals the type's minimum representable
  /// value). When `false`, segment pruning must never use this bound.
  public boolean isMinTrusted() {
    return _minTrusted;
  }

  /// Returns the average number of bytes per stored value, or `-1` if unknown.
  public double getAvgBytesPerValue() {
    return _avgBytesPerValue;
  }

  /// Returns the fraction of null values in `[0.0, 1.0]`, or `-1` if unknown.
  public double getNullFraction() {
    return _nullFraction;
  }
}
