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


/// Aggregation semantics shared by every [StatsStore] implementation.
///
/// These rules define what the stored per-segment rows *mean*, so they must not be re-derived per
/// implementation: two stores that disagree here would make the optimizer behave differently
/// depending on which store an operator configured. They live beside the contract, and are public,
/// so an implementation outside this package can reuse them rather than reinventing them.
///
/// Thread-safety: stateless; all methods are pure.
public final class StatsAggregations {

  private StatsAggregations() {
  }

  /// Returns how many of a segment's `docs` fall in the half-open query range
  /// `[startMs, endMs)`, given the segment's own inclusive time range `[segStart, segEnd]`.
  ///
  /// - Unknown segment times (the `-1` sentinel on either bound) count in full: the segment cannot
  ///   be excluded, and over-counting is the conservative direction for an estimate.
  /// - No overlap contributes 0.
  /// - Full containment contributes every doc.
  /// - Partial overlap is interpolated linearly over the segment's duration.
  /// - A zero-length segment counts in full when its single point lies in range.
  public static long overlapRows(long docs, long segStart, long segEnd, long startMs, long endMs) {
    if (segStart == -1 || segEnd == -1) {
      return docs;
    }
    if (segEnd <= startMs || segStart >= endMs) {
      return 0;
    }
    if (segStart >= startMs && segEnd <= endMs) {
      return docs;
    }
    long segDuration = segEnd - segStart;
    if (segDuration <= 0) {
      return segStart >= startMs && segStart < endMs ? docs : 0;
    }
    long overlapStart = Math.max(startMs, segStart);
    long overlapEnd = Math.min(endMs, segEnd);
    double fraction = (double) (overlapEnd - overlapStart) / segDuration;
    return Math.round(docs * fraction);
  }

  /// Accumulates per-segment column rows into a single [ColumnStatistics], applying the rules that
  /// define what those stored rows mean.
  ///
  /// Every store must fold rows through this, so that the estimate a query gets cannot depend on
  /// which store an operator configured.
  ///
  /// Thread-safety: not thread-safe; use one accumulator per aggregation.
  public static final class ColumnStatsAccumulator {
    private long _maxNdv = -1;
    private boolean _anyUntrustedMin;
    private long _totalDocs;
    private double _weightedAvgBytes;
    /// Documents behind [#_weightedAvgBytes]. Tracked separately from [#_totalDocs] because rows
    /// carrying the "unknown" sentinel contribute no weight, so dividing by the full document
    /// count would understate the average.
    private long _avgBytesDocs;
    private double _weightedNullFraction;
    /// Documents behind [#_weightedNullFraction]; see [#_avgBytesDocs].
    private long _nullFractionDocs;
    @Nullable
    private String _min;
    @Nullable
    private String _max;
    @Nullable
    private ColumnValueType _valueType;
    private boolean _typeConflict;
    private boolean _empty = true;

    /// Adds one segment's row for this column, weighted by that segment's document count.
    public void add(long segmentDocs, SegmentColumnStatsRow row) {
      _empty = false;
      _maxNdv = Math.max(_maxNdv, row.ndv());
      if (!row.minTrusted()) {
        _anyUntrustedMin = true;
      }
      _totalDocs += segmentDocs;
      // Both fields reserve a negative value for "unknown" (see SegmentColumnStatsRow). Weighting
      // that sentinel in as if it were a measurement produces a nonsense average -- a mix of an
      // unknown row and a known 0.2 null fraction would yield a negative fraction, which is
      // neither a legal fraction nor the sentinel a consumer tests for.
      double avgBytes = row.avgBytesPerValue();
      if (avgBytes >= 0) {
        _weightedAvgBytes += avgBytes * segmentDocs;
        _avgBytesDocs += segmentDocs;
      }
      double nullFraction = row.nullFraction();
      if (nullFraction >= 0) {
        _weightedNullFraction += nullFraction * segmentDocs;
        _nullFractionDocs += segmentDocs;
      }

      ColumnValueType rowType = row.valueType();
      if (rowType == null) {
        // No recorded ordering: the text cannot tell us how to compare, so stop trusting bounds
        // rather than guessing one.
        _typeConflict = true;
      } else if (_valueType == null) {
        _valueType = rowType;
      } else if (_valueType != rowType) {
        // Segments disagreeing about a column's type means one of them is stale; ordering across
        // them is undefined.
        _typeConflict = true;
      }

      // Once the ordering is in doubt the bounds are discarded wholesale by build(), so there is
      // nothing to gain by folding further rows into them under a guessed ordering.
      if (!_typeConflict && _valueType != null) {
        _min = minOf(_min, row.minValue(), _valueType);
        _max = maxOf(_max, row.maxValue(), _valueType);
      }
    }

    /// Returns `true` when no row was added, in which case the caller reports "no statistics"
    /// rather than an empty aggregate.
    public boolean isEmpty() {
      return _empty;
    }

    public ColumnStatistics build(String columnName) {
      ColumnValueType effectiveType = _typeConflict || _valueType == null ? null : _valueType;
      return ColumnStatistics.builder()
          .columnName(columnName)
          // Bounds folded without a known ordering are neither the true minimum nor the true
          // maximum under any ordering, so they are reported as absent rather than as untrusted:
          // isMinTrusted says nothing about the maximum, and a consumer following its documented
          // remedy would build a range out of an equally unreliable bound.
          .minValue(effectiveType == null ? null : effectiveType.toComparable(_min))
          .maxValue(effectiveType == null ? null : effectiveType.toComparable(_max))
          .ndv(_maxNdv, StatConfidence.ESTIMATED)
          .minTrusted(!_anyUntrustedMin && effectiveType != null)
          .avgBytesPerValue(_avgBytesDocs > 0 ? _weightedAvgBytes / _avgBytesDocs : -1)
          .nullFraction(_nullFractionDocs > 0 ? _weightedNullFraction / _nullFractionDocs : -1)
          .build();
    }
  }

  /// Returns the smaller of two stored values under `type`; `null` means unknown, so the other
  /// value wins.
  @Nullable
  public static String minOf(@Nullable String a, @Nullable String b, ColumnValueType type) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return type.compare(a, b) <= 0 ? a : b;
  }

  /// Returns the larger of two stored values under `type`; `null` means unknown, so the other
  /// value wins.
  @Nullable
  public static String maxOf(@Nullable String a, @Nullable String b, ColumnValueType type) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return type.compare(a, b) >= 0 ? a : b;
  }
}
