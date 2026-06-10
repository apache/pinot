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


/// Immutable per-column statistics for a table, used by the cost-based query planner.
///
/// Instances are created via [#builder()]. Unknown numeric fields are represented
/// by `-1`. Min/max values may be `null` when unknown.
///
/// Thread-safety: immutable; safe for concurrent access.
public class ColumnStatistics {
  private final String _columnName;
  private final long _ndv;
  private final StatConfidence _ndvConfidence;
  @Nullable
  private final Comparable<?> _minValue;
  @Nullable
  private final Comparable<?> _maxValue;
  private final boolean _minTrusted;
  private final double _avgBytesPerValue;
  private final double _nullFraction;

  private ColumnStatistics(Builder builder) {
    _columnName = builder._columnName;
    _ndv = builder._ndv;
    _ndvConfidence = builder._ndvConfidence;
    _minValue = builder._minValue;
    _maxValue = builder._maxValue;
    _minTrusted = builder._minTrusted;
    _avgBytesPerValue = builder._avgBytesPerValue;
    _nullFraction = builder._nullFraction;
  }

  /// Returns a new [Builder] for constructing [ColumnStatistics] instances.
  public static Builder builder() {
    return new Builder();
  }

  /// Returns the name of the column these statistics describe.
  public String getColumnName() {
    return _columnName;
  }

  /// Returns the estimated number of distinct values (NDV) for the column, or `-1` if unknown.
  public long getNdv() {
    return _ndv;
  }

  /// Returns the confidence level of the [#getNdv()] value.
  public StatConfidence getNdvConfidence() {
    return _ndvConfidence;
  }

  /// Returns the minimum observed value for the column, or `null` if unknown.
  @Nullable
  public Comparable<?> getMinValue() {
    return _minValue;
  }

  /// Returns the maximum observed value for the column, or `null` if unknown.
  @Nullable
  public Comparable<?> getMaxValue() {
    return _maxValue;
  }

  /// Returns `false` when the minimum value is polluted by the numeric null-sentinel default
  /// (i.e. the column is nullable and its stored min equals the type's minimum representable value).
  /// When `false`, estimation may assume the range `[-max..max]`, and segment pruning
  /// must never use this bound.
  public boolean isMinTrusted() {
    return _minTrusted;
  }

  /// Returns the average number of bytes per stored value for the column, or `-1` if unknown.
  public double getAvgBytesPerValue() {
    return _avgBytesPerValue;
  }

  /// Returns the fraction of rows where the column value is null (in the range `[0.0, 1.0]`),
  /// or `-1` if unknown.
  public double getNullFraction() {
    return _nullFraction;
  }

  /// Builder for [ColumnStatistics].
  ///
  /// Default values: string fields default to `null`, numeric fields to `-1`
  /// (unknown), confidence fields to [StatConfidence#UNKNOWN], and `minTrusted`
  /// defaults to `true`.
  ///
  /// Thread-safety: not thread-safe; use from a single thread.
  public static class Builder {
    @Nullable
    private String _columnName;
    private long _ndv = -1;
    private StatConfidence _ndvConfidence = StatConfidence.UNKNOWN;
    @Nullable
    private Comparable<?> _minValue;
    @Nullable
    private Comparable<?> _maxValue;
    private boolean _minTrusted = true;
    private double _avgBytesPerValue = -1;
    private double _nullFraction = -1;

    private Builder() {
    }

    /// Sets the column name.
    public Builder columnName(String columnName) {
      _columnName = columnName;
      return this;
    }

    /// Sets the number of distinct values (NDV) and its confidence level.
    public Builder ndv(long ndv, StatConfidence confidence) {
      _ndv = ndv;
      _ndvConfidence = confidence;
      return this;
    }

    /// Sets the minimum observed value.
    public Builder minValue(@Nullable Comparable<?> minValue) {
      _minValue = minValue;
      return this;
    }

    /// Sets the maximum observed value.
    public Builder maxValue(@Nullable Comparable<?> maxValue) {
      _maxValue = maxValue;
      return this;
    }

    /// Sets whether the minimum value is trustworthy (i.e. not polluted by a null-sentinel
    /// default).
    public Builder minTrusted(boolean minTrusted) {
      _minTrusted = minTrusted;
      return this;
    }

    /// Sets the average number of bytes per stored value.
    public Builder avgBytesPerValue(double avgBytesPerValue) {
      _avgBytesPerValue = avgBytesPerValue;
      return this;
    }

    /// Sets the null fraction (proportion of null values in `[0.0, 1.0]`).
    public Builder nullFraction(double nullFraction) {
      _nullFraction = nullFraction;
      return this;
    }

    /// Builds the immutable [ColumnStatistics] instance.
    public ColumnStatistics build() {
      return new ColumnStatistics(this);
    }
  }
}
