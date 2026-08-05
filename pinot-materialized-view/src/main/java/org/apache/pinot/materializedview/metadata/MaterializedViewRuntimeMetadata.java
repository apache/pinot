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
package org.apache.pinot.materializedview.metadata;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/// Stores the mutable runtime state of a materialized view: how far it has been materialized
/// and per-partition consistency info.
///
/// Persisted in ZooKeeper under `/CONFIGS/MATERIALIZED_VIEW/RUNTIME/<viewTableNameWithType>`.
/// This ZNode is updated by the Minion Executor (APPEND/OVERWRITE/DELETE) and the Controller
/// ConsistencyManager (VALID → STALE marking).
///
/// ### Coverage model (Design C)
///
/// The partition map IS the authoritative coverage: a bucket entry's presence + state
/// determines whether the broker can serve queries against that time range from the MV.
///
///   - `watermarkMs` — scheduling hint: highest contiguous VALID block from epoch up.  Used
///     by the scheduler to drive APPEND task selection and by the broker as the split point
///     for the 2-way SPLIT_REWRITE.  Not used as a coverage boundary directly — the
///     partition map controls per-bucket routing.
///   - `partitions` — `bucketStart → PartitionInfo(VALID|STALE, fingerprint, lastRefreshTime)`.
///     A bucket's absence from this map means "MV does not cover this time range";
///     deletion is modeled by removing the entry (no separate EXPIRED state).
///
/// Freshness is derived on read (e.g. `now - watermarkMs > stalenessThresholdMs` ⇒ STALE);
/// there is no persistent freshness field.
///
/// ### Partition model (TIME-WINDOWED ONLY in PR 1)
///
/// `_partitions` is keyed by `Long bucketStartMs`. The wire format already stores keys as
/// strings (`Long.toString(bucketStart)` in [#toZNRecord], parsed in [#fromZNRecord]), so the
/// on-disk schema is partition-shape neutral and can carry future categorical keys without
/// a breaking change. The in-memory key type, however, is `Long` today and will need to
/// generalize (to `String`, or to a `PartitionKey` sum type) when fixed-partition MVs land.
/// See `pinot-materialized-view/DESIGN.md` for the migration plan.
///
/// Thread-safety: instances are effectively immutable after construction.
public class MaterializedViewRuntimeMetadata {
  private static final String WATERMARK_MS_KEY = "watermarkMs";
  private static final String PARTITION_INFOS_MAP_KEY = "partitionInfos";

  private final String _materializedViewTableNameWithType;
  private final long _watermarkMs;
  private final Map<Long, PartitionInfo> _partitions;

  public MaterializedViewRuntimeMetadata(String viewTableNameWithType, long watermarkMs,
      Map<Long, PartitionInfo> partitions) {
    Preconditions.checkArgument(watermarkMs >= 0,
        "watermarkMs must be non-negative, got: %s", watermarkMs);
    _materializedViewTableNameWithType = viewTableNameWithType;
    _watermarkMs = watermarkMs;
    // Defensive copy: the class advertises immutability, but the underlying PartitionInfo entries
    // are themselves immutable. Copying just the map structure prevents callers from mutating
    // their handle after construction and silently corrupting the cached / persisted view.
    _partitions = partitions == null ? Map.of() : Map.copyOf(partitions);
  }

  /// Validates the writer-side invariants before persistence.  Writers MUST invoke this.
  public void validateForPersist() {
    // No cross-field invariants under Design C: partitions map IS the coverage, watermarkMs is
    // a derived scheduling hint.  Kept for forward-compatibility — future invariants can be
    // added here.
  }

  public String getMaterializedViewTableNameWithType() {
    return _materializedViewTableNameWithType;
  }

  public long getWatermarkMs() {
    return _watermarkMs;
  }

  public Map<Long, PartitionInfo> getPartitions() {
    return _partitions;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_materializedViewTableNameWithType);
    znRecord.setLongField(WATERMARK_MS_KEY, _watermarkMs);

    // Each partition becomes its own map-field entry keyed by `bucketStartMs`.  This uses
    // ZNRecord's native two-level structure (top-level mapFields → Map<String,String> per
    // partition) so the on-the-wire shape is structured rather than packed strings.
    for (Map.Entry<Long, PartitionInfo> entry : _partitions.entrySet()) {
      znRecord.setMapField(Long.toString(entry.getKey()), entry.getValue().toFieldMap());
    }

    return znRecord;
  }

  public static MaterializedViewRuntimeMetadata fromZNRecord(ZNRecord znRecord) {
    String viewTableNameWithType = znRecord.getId();
    long watermarkMs = znRecord.getLongField(WATERMARK_MS_KEY, 0L);

    Map<Long, PartitionInfo> partitions = new HashMap<>();
    Map<String, Map<String, String>> mapFields = znRecord.getMapFields();
    if (mapFields != null) {
      for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
        // Skip the legacy combined-partitions key from V1 if encountered (forward-compat read).
        if (PARTITION_INFOS_MAP_KEY.equals(entry.getKey())) {
          continue;
        }
        try {
          long partitionStartMs = Long.parseLong(entry.getKey());
          partitions.put(partitionStartMs, PartitionInfo.fromFieldMap(entry.getValue()));
        } catch (NumberFormatException e) {
          // Non-numeric map-field keys are not partitions; ignore for forward compat.
        }
      }
    }

    return new MaterializedViewRuntimeMetadata(viewTableNameWithType, watermarkMs, partitions);
  }
}
