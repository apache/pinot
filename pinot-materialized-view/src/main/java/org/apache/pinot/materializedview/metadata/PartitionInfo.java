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
import java.util.Objects;


/// Tracks the state and provenance of a single materialized partition.
///
/// Fields:
///
///   - `state` – whether the partition is up-to-date ([PartitionState#VALID])
///       or needs re-materialization ([PartitionState#STALE]).
///   - `fingerprint` – the base segment snapshot (count + CRC) recorded when
///       the partition was last materialized.
///   - `lastRefreshTime` – wall clock time (millis) of the last successful materialization.
///
/// Serialized as a typed `Map<String, String>` ZNRecord map-field entry with keys
/// `state`, `segmentCount`, `crc`, `lastRefreshTime`.  This shape is forward-compatible
/// (new fields can be added without breaking older readers, which ignore unknown keys)
/// unlike the prior packed `"V,10,5000,1700006400000"` string format.
///
/// Thread-safety: instances are immutable after construction.
public class PartitionInfo {
  private static final String STATE_KEY = "state";
  private static final String SEGMENT_COUNT_KEY = "segmentCount";
  private static final String CRC_KEY = "crc";
  private static final String LAST_REFRESH_TIME_KEY = "lastRefreshTime";

  private final PartitionState _state;
  private final PartitionFingerprint _fingerprint;
  private final long _lastRefreshTime;

  public PartitionInfo(PartitionState state, PartitionFingerprint fingerprint, long lastRefreshTime) {
    _state = state;
    _fingerprint = fingerprint;
    _lastRefreshTime = lastRefreshTime;
  }

  public PartitionState getState() {
    return _state;
  }

  public PartitionFingerprint getFingerprint() {
    return _fingerprint;
  }

  public long getLastRefreshTime() {
    return _lastRefreshTime;
  }

  /// Creates a new `PartitionInfo` with the given state, keeping fingerprint and
  /// lastRefreshTime unchanged.
  public PartitionInfo withState(PartitionState newState) {
    return new PartitionInfo(newState, _fingerprint, _lastRefreshTime);
  }

  /// Serializes to a typed map suitable for `ZNRecord.setMapField(bucketStart, ...)`.
  public Map<String, String> toFieldMap() {
    Map<String, String> map = new HashMap<>(4);
    map.put(STATE_KEY, _state.encode());
    map.put(SEGMENT_COUNT_KEY, Integer.toString(_fingerprint.getSegmentCount()));
    map.put(CRC_KEY, Long.toString(_fingerprint.getCrcChecksum()));
    map.put(LAST_REFRESH_TIME_KEY, Long.toString(_lastRefreshTime));
    return map;
  }

  /// Deserializes from the typed field map produced by [#toFieldMap].  Unknown extra keys
  /// are ignored (forward compatibility for future field additions).
  ///
  /// @throws IllegalArgumentException if any required key is missing or malformed
  public static PartitionInfo fromFieldMap(Map<String, String> map) {
    Preconditions.checkArgument(map != null, "PartitionInfo field map must not be null");
    String stateStr = map.get(STATE_KEY);
    String segmentCountStr = map.get(SEGMENT_COUNT_KEY);
    String crcStr = map.get(CRC_KEY);
    String lastRefreshTimeStr = map.get(LAST_REFRESH_TIME_KEY);
    Preconditions.checkArgument(stateStr != null && segmentCountStr != null && crcStr != null
            && lastRefreshTimeStr != null,
        "PartitionInfo field map missing required keys; got: %s", map);
    PartitionState state = PartitionState.decode(stateStr);
    int segmentCount = Integer.parseInt(segmentCountStr);
    long crcChecksum = Long.parseLong(crcStr);
    long lastRefreshTime = Long.parseLong(lastRefreshTimeStr);
    return new PartitionInfo(state, new PartitionFingerprint(segmentCount, crcChecksum), lastRefreshTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) o;
    return _lastRefreshTime == that._lastRefreshTime
        && _state == that._state
        && Objects.equals(_fingerprint, that._fingerprint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_state, _fingerprint, _lastRefreshTime);
  }

  @Override
  public String toString() {
    return "PartitionInfo{state=" + _state + ", fingerprint=" + _fingerprint
        + ", lastRefreshTime=" + _lastRefreshTime + "}";
  }
}
