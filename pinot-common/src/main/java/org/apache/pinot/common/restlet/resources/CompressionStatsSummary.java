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


/// Table-level compression statistics summary, aggregated across all servers for a sub-table type
/// (offline or realtime). Reported under the `compressionStats` key in the
/// `GET /tables/{tableName}/size` response.
///
/// Sizes are "per replica" — each segment's contribution is the maximum reported across its replicas,
/// so the total reflects a single logical copy of the data rather than the physical replication factor.
///
/// `isPartialCoverage` is true when one or more segments lack stats (e.g. built before
/// `compressionStatsEnabled` was set), meaning the ratio is computed from a subset of segments only.
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompressionStatsSummary {
  /// Sum of per-replica max uncompressed forward index sizes across all segments that have stats.
  private final long _rawForwardIndexSizePerReplicaInBytes;

  /// Sum of per-replica max compressed forward index sizes across all segments that have stats.
  private final long _compressedForwardIndexSizePerReplicaInBytes;

  /// Overall ratio of raw to compressed size (`rawSize / compressedSize`). `0` when no segments have stats.
  private final double _compressionRatio;

  /// Number of segments that have compression stats (built with `compressionStatsEnabled=true`).
  private final int _segmentsWithStats;

  /// Total number of segments in the sub-table.
  private final int _totalSegments;

  /// True when `segmentsWithStats < totalSegments`, meaning the ratio covers only a subset of segments.
  private final boolean _isPartialCoverage;

  @JsonCreator
  public CompressionStatsSummary(
      @JsonProperty("rawForwardIndexSizePerReplicaInBytes") long rawForwardIndexSizePerReplicaInBytes,
      @JsonProperty("compressedForwardIndexSizePerReplicaInBytes") long compressedForwardIndexSizePerReplicaInBytes,
      @JsonProperty("compressionRatio") double compressionRatio,
      @JsonProperty("segmentsWithStats") int segmentsWithStats,
      @JsonProperty("totalSegments") int totalSegments,
      @JsonProperty("isPartialCoverage") boolean isPartialCoverage) {
    _rawForwardIndexSizePerReplicaInBytes = rawForwardIndexSizePerReplicaInBytes;
    _compressedForwardIndexSizePerReplicaInBytes = compressedForwardIndexSizePerReplicaInBytes;
    _compressionRatio = compressionRatio;
    _segmentsWithStats = segmentsWithStats;
    _totalSegments = totalSegments;
    _isPartialCoverage = isPartialCoverage;
  }

  public long getRawForwardIndexSizePerReplicaInBytes() {
    return _rawForwardIndexSizePerReplicaInBytes;
  }

  public long getCompressedForwardIndexSizePerReplicaInBytes() {
    return _compressedForwardIndexSizePerReplicaInBytes;
  }

  public double getCompressionRatio() {
    return _compressionRatio;
  }

  public int getSegmentsWithStats() {
    return _segmentsWithStats;
  }

  public int getTotalSegments() {
    return _totalSegments;
  }

  @JsonProperty("isPartialCoverage")
  public boolean isPartialCoverage() {
    return _isPartialCoverage;
  }
}
