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


/// Immutable aggregate statistics for a table, used by the cost-based query planner.
///
/// Instances are created via [#builder()]. Unknown numeric fields are represented
/// by `-1`; unknown timestamp fields by `0`.
///
/// Thread-safety: immutable; safe for concurrent access.
public class TableStatistics {
  private final long _rowCount;
  private final StatConfidence _rowCountConfidence;
  private final long _tableSizeBytes;
  private final StatConfidence _sizeConfidence;
  private final long _updatedAtMs;

  private TableStatistics(Builder builder) {
    _rowCount = builder._rowCount;
    _rowCountConfidence = builder._rowCountConfidence;
    _tableSizeBytes = builder._tableSizeBytes;
    _sizeConfidence = builder._sizeConfidence;
    _updatedAtMs = builder._updatedAtMs;
  }

  /// Returns a new [Builder] for constructing [TableStatistics] instances.
  public static Builder builder() {
    return new Builder();
  }

  /// Returns the estimated number of rows in the table, or `-1` if unknown.
  public long getRowCount() {
    return _rowCount;
  }

  /// Returns the confidence level of the [#getRowCount()] value.
  public StatConfidence getRowCountConfidence() {
    return _rowCountConfidence;
  }

  /// Returns the estimated total size of the table in bytes, or `-1` if unknown.
  public long getTableSizeBytes() {
    return _tableSizeBytes;
  }

  /// Returns the confidence level of the [#getTableSizeBytes()] value.
  public StatConfidence getSizeConfidence() {
    return _sizeConfidence;
  }

  /// Returns the epoch-millisecond timestamp at which these statistics were last updated,
  /// or `0` if unknown.
  public long getUpdatedAtMs() {
    return _updatedAtMs;
  }

  /// Builder for [TableStatistics].
  ///
  /// Default values: numeric fields default to `-1` (unknown),
  /// confidence fields default to [StatConfidence#UNKNOWN],
  /// timestamp fields default to `0` (unknown).
  ///
  /// Thread-safety: not thread-safe; use from a single thread.
  public static class Builder {
    private long _rowCount = -1;
    private StatConfidence _rowCountConfidence = StatConfidence.UNKNOWN;
    private long _tableSizeBytes = -1;
    private StatConfidence _sizeConfidence = StatConfidence.UNKNOWN;
    private long _updatedAtMs = 0;

    private Builder() {
    }

    /// Sets the row count and its confidence level.
    public Builder rowCount(long rowCount, StatConfidence confidence) {
      _rowCount = rowCount;
      _rowCountConfidence = confidence;
      return this;
    }

    /// Sets the table size in bytes and its confidence level.
    public Builder tableSizeBytes(long tableSizeBytes, StatConfidence confidence) {
      _tableSizeBytes = tableSizeBytes;
      _sizeConfidence = confidence;
      return this;
    }

    /// Sets the epoch-millisecond timestamp of the last statistics update.
    public Builder updatedAtMs(long updatedAtMs) {
      _updatedAtMs = updatedAtMs;
      return this;
    }

    /// Builds the immutable [TableStatistics] instance.
    public TableStatistics build() {
      return new TableStatistics(this);
    }
  }
}
