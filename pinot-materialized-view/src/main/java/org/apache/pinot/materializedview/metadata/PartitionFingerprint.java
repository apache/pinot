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

import com.google.common.hash.Hashing;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;


/// Fingerprint of a materialized partition, capturing how many base segments contributed
/// and their aggregate CRC. Used to detect when base table data has changed since the
/// partition was last materialized.
///
/// Single-entry serialized form: `"segmentCount,crcChecksum"` for ZNRecord map fields.
///
/// Map serialized form (for task config transport):
/// `"partStartMs1=segCnt1,crc1;partStartMs2=segCnt2,crc2"`.
///
/// Thread-safety: instances are immutable after construction.
public class PartitionFingerprint {
  private static final char SEPARATOR = ',';

  /// Canonical fingerprint for an "empty" partition window — no base segments overlap.
  ///
  /// The `crcChecksum` field is initialised by running the same `farmHashFingerprint64`
  /// hasher with no input bytes, which is byte-identical to what
  /// `MaterializedViewTaskUtils#computeWindowFingerprint` (the single source of truth used
  /// by both the scheduler and the minion executor) produces when the overlapping segment
  /// list is empty. Two consequences:
  ///
  ///   - Existing ZK records written by the APPEND-empty path (carrying `(0, farmHash64(""))`)
  ///     are byte-equal to this constant, so `equals` comparisons against [#EMPTY] continue
  ///     to behave correctly across rolling upgrades.
  ///
  ///   - The DELETE task executor uses [#EMPTY] when it persists a `VALID + empty`
  ///     PartitionInfo after retention-deleting the source data. Reusing the same value the
  ///     APPEND-empty path naturally produces avoids introducing a second representation of
  ///     "empty fingerprint" that would silently fail equality checks.
  ///
  /// `farmHashFingerprint64("")` is a deterministic non-zero constant; never use
  /// `new PartitionFingerprint(0, 0L)` as a stand-in for "empty" — the two values do NOT
  /// compare equal.
  public static final PartitionFingerprint EMPTY =
      new PartitionFingerprint(0, Hashing.farmHashFingerprint64().newHasher().hash().asLong());

  /// Number of base table segments whose time range overlaps this partition window.
  private final int _segmentCount;

  /// Sum of CRC values from all overlapping base segments, recorded at materialization time.
  ///
  /// This field is NOT used during the event-driven dirty marking phase
  /// (`MaterializedViewConsistencyManager`), which marks partitions as STALE based
  /// solely on time-range overlap with changed segments.
  ///
  /// It IS used by the Generator's precise verification step
  /// (`tryGenerateOverwriteTask`): when a partition is already marked STALE, the
  /// Generator re-computes the current fingerprint and compares it against this stored
  /// baseline via [#equals]. Without the CRC, a scenario where one segment is
  /// deleted and a different segment is uploaded would leave `segmentCount` unchanged,
  /// causing the Generator to incorrectly revert the partition to VALID. The CRC sum
  /// will differ in that case, correctly confirming the data change.
  ///
  /// The Executor writes this value after successful materialization, establishing the
  /// baseline snapshot for future Generator comparisons.
  private final long _crcChecksum;

  public PartitionFingerprint(int segmentCount, long crcChecksum) {
    _segmentCount = segmentCount;
    _crcChecksum = crcChecksum;
  }

  public int getSegmentCount() {
    return _segmentCount;
  }

  public long getCrcChecksum() {
    return _crcChecksum;
  }

  /// Encodes this fingerprint as `"segmentCount,crcChecksum"`.
  public String encode() {
    return _segmentCount + String.valueOf(SEPARATOR) + _crcChecksum;
  }

  /// Decodes a fingerprint from the format `"segmentCount,crcChecksum"`.
  ///
  /// @throws IllegalArgumentException if the string is malformed
  public static PartitionFingerprint decode(String encoded) {
    int separatorIdx = encoded.indexOf(SEPARATOR);
    if (separatorIdx < 0) {
      throw new IllegalArgumentException("Invalid PartitionFingerprint encoding: " + encoded);
    }
    int segmentCount = Integer.parseInt(encoded.substring(0, separatorIdx));
    long crcChecksum = Long.parseLong(encoded.substring(separatorIdx + 1));
    return new PartitionFingerprint(segmentCount, crcChecksum);
  }

  /// Encodes a map of partition fingerprints as
  /// `"partStartMs1=segCnt1,crc1;partStartMs2=segCnt2,crc2"`.
  /// Entries are sorted by `partStartMs` key so the output is deterministic across JVMs and
  /// stable across decode/encode round-trips. Callers that key caches or logs on this string
  /// rely on byte-identical output for the same logical map.
  public static String encodeMap(Map<Long, PartitionFingerprint> map) {
    if (map == null || map.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<Long, PartitionFingerprint> entry : new TreeMap<>(map).entrySet()) {
      if (!first) {
        sb.append(';');
      }
      sb.append(entry.getKey()).append('=').append(entry.getValue().encode());
      first = false;
    }
    return sb.toString();
  }

  /// Decodes a map of partition fingerprints from the format produced by [#encodeMap].
  ///
  /// @return empty map if the input is null or blank
  /// @throws IllegalArgumentException if any entry is malformed
  public static Map<Long, PartitionFingerprint> decodeMap(String encoded) {
    Map<Long, PartitionFingerprint> map = new HashMap<>();
    if (encoded == null || encoded.isEmpty()) {
      return map;
    }
    for (String entry : encoded.split(";")) {
      int eqIdx = entry.indexOf('=');
      if (eqIdx < 0) {
        throw new IllegalArgumentException("Invalid partition fingerprint map entry: " + entry);
      }
      long partitionStartMs = Long.parseLong(entry.substring(0, eqIdx));
      PartitionFingerprint fp = decode(entry.substring(eqIdx + 1));
      map.put(partitionStartMs, fp);
    }
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionFingerprint that = (PartitionFingerprint) o;
    return _segmentCount == that._segmentCount && _crcChecksum == that._crcChecksum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentCount, _crcChecksum);
  }

  @Override
  public String toString() {
    return "PartitionFingerprint{segmentCount=" + _segmentCount + ", crcChecksum=" + _crcChecksum + "}";
  }
}
