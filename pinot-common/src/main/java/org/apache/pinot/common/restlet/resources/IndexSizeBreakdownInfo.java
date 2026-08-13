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


/// On-disk size contributed by one index type, aggregated across every column of a table. Reported as the value of
/// each entry under the `indexSizeBreakdown` key on the table-size and aggregate-metadata responses, keyed by
/// `IndexType#getId()` — for example `forward_index`, `inverted_index`, `bloom_filter`.
///
/// Sizes are per replica: the controller selects one coherent contribution per logical segment, so a total reflects
/// one copy of the data rather than the physical replication factor.
///
/// `indexSizeBreakdown` is a **table-level** aggregate and deliberately does not honour the `columns=` filter, unlike
/// the per-column `columnIndexSizeMap` that appears alongside it on the metadata endpoint. The two answer different
/// questions: which index types cost the most across the table, versus what a specific column contains.
///
/// **An index type absent from the breakdown means no segment reported a size for it, not that it occupies zero
/// bytes.** Sizes are only persisted for segments built while `tableIndexConfig.indexSizeStatsEnabled` was set, and
/// star-tree and multi-column text indexes are never measured. Callers must not treat a missing entry as zero.
///
/// Instances are immutable and thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class IndexSizeBreakdownInfo {
  /// Summed on-disk bytes for this index type across all columns, for one replica.
  private final long _sizePerReplicaInBytes;

  /// Number of segments that contributed a size for this index type. Lets a caller tell a small genuine total apart
  /// from a total assembled from only a handful of segments.
  private final int _segmentsWithStats;

  @JsonCreator
  public IndexSizeBreakdownInfo(@JsonProperty("sizePerReplicaInBytes") long sizePerReplicaInBytes,
      @JsonProperty("segmentsWithStats") int segmentsWithStats) {
    _sizePerReplicaInBytes = sizePerReplicaInBytes;
    _segmentsWithStats = segmentsWithStats;
  }

  /// Returns the summed on-disk bytes for this index type across all columns, for one replica.
  public long getSizePerReplicaInBytes() {
    return _sizePerReplicaInBytes;
  }

  /// Returns how many segments contributed a size for this index type.
  public int getSegmentsWithStats() {
    return _segmentsWithStats;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IndexSizeBreakdownInfo)) {
      return false;
    }
    IndexSizeBreakdownInfo that = (IndexSizeBreakdownInfo) o;
    return _sizePerReplicaInBytes == that._sizePerReplicaInBytes && _segmentsWithStats == that._segmentsWithStats;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(_sizePerReplicaInBytes);
    return 31 * result + _segmentsWithStats;
  }

  @Override
  public String toString() {
    return "IndexSizeBreakdownInfo{_sizePerReplicaInBytes=" + _sizePerReplicaInBytes + ", _segmentsWithStats="
        + _segmentsWithStats + '}';
  }
}
