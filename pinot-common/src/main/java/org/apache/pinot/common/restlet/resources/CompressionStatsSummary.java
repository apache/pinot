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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/// Table-level compression statistics summary, aggregated across all servers for an offline or realtime table.
/// Reported under the `compressionStats` key in table-size and aggregate-metadata responses.
///
/// Sizes are per replica; the controller selects one coherent contribution for each logical segment, so totals reflect
/// one copy of the data rather than the physical replication factor.
///
/// `partialCoverage` is true when one or more segments lack stats (e.g. built before
/// `compressionStatsEnabled` was set), meaning the ratio is computed from a subset of segments only.
/// Instances are immutable and thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class CompressionStatsSummary {
  /// Sum of selected per-segment uncompressed column-value sizes.
  private final long _uncompressedValueSizePerReplicaInBytes;

  /// Sum of selected per-segment forward-index and dictionary sizes on disk.
  private final long _forwardIndexAndDictionaryStorageSizePerReplicaInBytes;

  /// Overall ratio of uncompressed value bytes to forward-index and dictionary storage bytes.
  private final double _compressionRatio;

  /// Number of segments that have compression stats (built with `compressionStatsEnabled=true`).
  private final int _segmentsWithCompleteStats;

  /// Total number of segments in the sub-table.
  private final int _totalSegments;

  /// True when `segmentsWithCompleteStats < totalSegments`, meaning the ratio covers only a subset of segments.
  private final boolean _partialCoverage;

  @JsonCreator
  public CompressionStatsSummary(
      @JsonProperty("uncompressedValueSizePerReplicaInBytes") long uncompressedValueSizePerReplicaInBytes,
      @JsonProperty("forwardIndexAndDictionaryStorageSizePerReplicaInBytes")
      long forwardIndexAndDictionaryStorageSizePerReplicaInBytes,
      @JsonProperty("compressionRatio") double compressionRatio,
      @JsonProperty("segmentsWithCompleteStats") int segmentsWithCompleteStats,
      @JsonProperty("totalSegments") int totalSegments,
      @JsonProperty("partialCoverage") boolean partialCoverage) {
    _uncompressedValueSizePerReplicaInBytes = uncompressedValueSizePerReplicaInBytes;
    _forwardIndexAndDictionaryStorageSizePerReplicaInBytes = forwardIndexAndDictionaryStorageSizePerReplicaInBytes;
    _compressionRatio = compressionRatio;
    _segmentsWithCompleteStats = segmentsWithCompleteStats;
    _totalSegments = totalSegments;
    _partialCoverage = partialCoverage;
  }

  /// Returns uncompressed serialized column-value bytes for one logical copy of the covered segments.
  public long getUncompressedValueSizePerReplicaInBytes() {
    return _uncompressedValueSizePerReplicaInBytes;
  }

  /// Returns forward-index and dictionary bytes for one logical copy of the covered segments.
  public long getForwardIndexAndDictionaryStorageSizePerReplicaInBytes() {
    return _forwardIndexAndDictionaryStorageSizePerReplicaInBytes;
  }

  /// Returns `uncompressedValueSizePerReplicaInBytes / forwardIndexAndDictionaryStorageSizePerReplicaInBytes`, or `0`
  /// when no bytes are covered.
  public double getCompressionRatio() {
    return _compressionRatio;
  }

  /// Returns the number of logical segments with complete statistics.
  public int getSegmentsWithCompleteStats() {
    return _segmentsWithCompleteStats;
  }

  /// Returns the total logical segment count used as the coverage denominator.
  public int getTotalSegments() {
    return _totalSegments;
  }

  /// Returns whether at least one logical segment is not covered.
  @JsonProperty("partialCoverage")
  public boolean isPartialCoverage() {
    return _partialCoverage;
  }
}
