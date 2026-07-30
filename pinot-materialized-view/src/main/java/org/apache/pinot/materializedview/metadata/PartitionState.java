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


/// State of a materialized partition in the MV lifecycle.
///
///   - `VALID` – partition is up-to-date with base table data.  When the base table window
///     is empty (no overlapping segments), the entry carries
///     [PartitionFingerprint#EMPTY] and is still `VALID`; the broker treats those buckets
///     as "covered by the MV with no rows".
///   - `STALE` – base table data has changed since the partition was last materialized;
///     the scheduler will dispatch a recompute (OVERWRITE), or DELETE if the source
///     window is now empty.
///
/// `absent` (no entry in the partition map) is reserved for cold-start, before the first
/// APPEND has run for that bucket.  After the first run of any task type — APPEND with
/// data, APPEND with empty source, OVERWRITE, or DELETE — the bucket carries an entry
/// (`VALID` with a real fingerprint, or `VALID` with [PartitionFingerprint#EMPTY]).
/// The DELETE task explicitly does NOT remove the entry; it rewrites it to
/// `VALID + PartitionFingerprint.EMPTY` so subsequent backfills into the now-empty
/// window flow through the standard `VALID → STALE → OVERWRITE` cycle.
///
/// Encoded as a single character (`"V"` / `"S"`) for compact ZK storage.
public enum PartitionState {
  VALID("V"),
  STALE("S");

  private final String _code;

  PartitionState(String code) {
    _code = code;
  }

  public String encode() {
    return _code;
  }

  public static PartitionState decode(String code) {
    switch (code) {
      case "V":
        return VALID;
      case "S":
        return STALE;
      default:
        throw new IllegalArgumentException("Unknown PartitionState code: " + code);
    }
  }
}
