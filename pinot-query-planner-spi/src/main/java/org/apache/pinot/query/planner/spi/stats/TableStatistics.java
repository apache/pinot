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


/**
 * Immutable aggregate statistics for a table, used by the cost-based query planner.
 *
 * <p>Instances are created via {@link #builder()}. Unknown numeric fields are represented
 * by {@code -1}; unknown timestamp fields by {@code 0}.
 *
 * <p>Thread-safety: immutable; safe for concurrent access.
 */
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

  /**
   * Returns a new {@link Builder} for constructing {@link TableStatistics} instances.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the estimated number of rows in the table, or {@code -1} if unknown.
   */
  public long getRowCount() {
    return _rowCount;
  }

  /**
   * Returns the confidence level of the {@link #getRowCount()} value.
   */
  public StatConfidence getRowCountConfidence() {
    return _rowCountConfidence;
  }

  /**
   * Returns the estimated total size of the table in bytes, or {@code -1} if unknown.
   */
  public long getTableSizeBytes() {
    return _tableSizeBytes;
  }

  /**
   * Returns the confidence level of the {@link #getTableSizeBytes()} value.
   */
  public StatConfidence getSizeConfidence() {
    return _sizeConfidence;
  }

  /**
   * Returns the epoch-millisecond timestamp at which these statistics were last updated,
   * or {@code 0} if unknown.
   */
  public long getUpdatedAtMs() {
    return _updatedAtMs;
  }

  /**
   * Builder for {@link TableStatistics}.
   *
   * <p>Default values: numeric fields default to {@code -1} (unknown),
   * confidence fields default to {@link StatConfidence#UNKNOWN},
   * timestamp fields default to {@code 0} (unknown).
   *
   * <p>Thread-safety: not thread-safe; use from a single thread.
   */
  public static class Builder {
    private long _rowCount = -1;
    private StatConfidence _rowCountConfidence = StatConfidence.UNKNOWN;
    private long _tableSizeBytes = -1;
    private StatConfidence _sizeConfidence = StatConfidence.UNKNOWN;
    private long _updatedAtMs = 0;

    private Builder() {
    }

    /** Sets the row count and its confidence level. */
    public Builder rowCount(long rowCount, StatConfidence confidence) {
      _rowCount = rowCount;
      _rowCountConfidence = confidence;
      return this;
    }

    /** Sets the table size in bytes and its confidence level. */
    public Builder tableSizeBytes(long tableSizeBytes, StatConfidence confidence) {
      _tableSizeBytes = tableSizeBytes;
      _sizeConfidence = confidence;
      return this;
    }

    /** Sets the epoch-millisecond timestamp of the last statistics update. */
    public Builder updatedAtMs(long updatedAtMs) {
      _updatedAtMs = updatedAtMs;
      return this;
    }

    /** Builds the immutable {@link TableStatistics} instance. */
    public TableStatistics build() {
      return new TableStatistics(this);
    }
  }
}
