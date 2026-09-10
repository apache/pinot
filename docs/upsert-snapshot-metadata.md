<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Upsert snapshot diagnostics

This opt-in feature records why a snapshot attempt missed work and fingerprints the saved bitmap
population for later comparison. Each partition retains one compact report, with no per-segment list.

```json
{"upsertConfig": {"mode": "FULL", "snapshot": "ENABLE",
  "metadataManagerConfigs": {"enableSnapshotMetadata": "true"}}}
```

The default is false. On-heap and RocksDB managers share snapshot accounting and file fingerprints.
RocksDB additionally observes its actual asynchronous cleanup pass.

## Code flow

1. `BasePartitionUpsertMetadataManager` counts outcomes in its existing snapshot loops.
2. `ImmutableSegmentImpl` caches hashes while existing operations write or load saved bitmap bytes.
3. `UpsertSnapshotDiagnostics` collects those cached contributions, builds the report, and publishes it.
4. `UpsertSnapshotMetadataStore` persists/reads the sidecar; the server API prefers the live report.

The report is plain immutable data. Cleanup publishes immutable before/after values under the existing
RocksDB lifecycle lock. Snapshot readers do not acquire that lock. Concurrent captures use a local
sequence and active count; there is no general lifecycle observer or configuration-comparison engine.

## Snapshot outcomes

`attempt` contains selected, written, directly lock-skipped, otherwise skipped, and failed segment
counts, plus an aborted flag. A partial valid/queryable write counts as failed. New-file work deferred
because an earlier existing-file write or lock failed counts as otherwise skipped. Unchanged segments
contribute to content coverage but are not selected for another write.

`runtimeEpoch` identifies one manager lifetime. Cumulative attempt, lock-affected attempt, and failed
attempt counters reveal failures between polls; compare them only within that epoch. `captureId` and
timestamps identify local attempts. Startup captures also carry the consumer name and opaque start
offset; manual/shutdown captures do not. None of these fields aligns replicas. Exits before an attempt
starts, such as disabled snapshots or a stopped manager, produce no new report.

## Saved content

`content.populationFingerprint` covers sorted segment names, data CRCs, and bitmap types for all
tracked immutable segments. `savedFilesFingerprint` additionally covers their raw saved-file hashes.
Types are `VALID` and, with a delete column, `QUERYABLE`. The versioned encoding uses length-prefixed
UTF-8 fields and domain separators. `expectedFiles` and `knownFiles` expose coverage; unknown CRCs,
duplicate identities, or missing contributions prevent a complete aggregate hash.

Each immutable segment optionally caches two hashes. Writes hash already serialized bytes after
successful persistence; normal loads can seed an unchanged cache from bytes already read. File
changes, deletion, untracking, and destruction invalidate contributions. Overlapping changes remain
unknown. Retained files participate even when an attempt writes no dirty segments. There is no extra
producer file read to fill gaps, and the declared population does not certify all source segments loaded.

Raw Roaring bytes are not canonical membership: equal sets can serialize differently after
`runOptimize()`. A fingerprint mismatch needs logical bitmap comparison. The existing saved-bitmap
endpoint can return the hash of the exact raw bytes it read:

```http
GET /segments/orders_REALTIME/<segment>/validDocIdsBitmap?validDocIdsType=SNAPSHOT&includeSnapshotFingerprint=true
```

Use `SNAPSHOT_WITH_DELETE` for queryable bitmaps. A future checker must fetch every declared file with
bounded concurrency, reproduce the target report's population and saved-file fingerprints from the
response identities and `snapshotFileFingerprint`, then compare logical bitmap memberships. The
response bitmap is reserialized: hashing those bytes cannot replace the raw-file hash. If files were
overwritten or identities/coverage changed, the target is unavailable. Do not mix old reports with new
files. Equal cardinalities alone do not establish equal membership; no primary-key scan is added.

## Cleanup and boundary

`cleanupBefore` and `cleanupAfter` are frozen observations; `cleanupNow` is current process state.
Cleanup exposes its phase, observation version, target and last-completed watermarks, whether the pass
may affect valid doc IDs, and cumulative failures. Only a successful actual pass advances the completed
watermark. `FAILED_POSSIBLY_PARTIAL` retains the previous completed watermark. Version changes or a
running phase identify possible overlap without making snapshot readers wait. Engines without actual
cleanup instrumentation report unknown cleanup when TTL is enabled.

`concurrentSnapshots` identifies overlapping attempts. `boundaryStatus` remains `UNVERIFIED`:
matching offsets or saved files cannot establish a shared logical source boundary. Confirming different
saved memberships is possible after file binding; calling that replica divergence at the same source
boundary requires additional attestation. Controller scheduling, logical comparison, and alerts remain
separate work. Version 3 removes provisional lifecycle/configuration fields from the earlier draft;
older sidecars are treated as unavailable.

## Publication and cost

The producer publishes the immutable report in memory and atomically replaces a server-local sidecar:

```text
<table data directory>/upsert.snapshot.metadata.partition.<partitionId>.json
```

```http
GET /tables/orders_REALTIME/upsertSnapshotMetadata/3
```

The API reports `availability`, `partitionId`, `snapshot`, and `source` (`LIVE` or `HISTORICAL_FILE`).
Live responses include `persistenceStatus`, cumulative `publicationFailures`, and `cleanupNow`. A failed
sidecar write leaves the live report readable. Historical files can lag live state; concurrent attempts
can persist in completion order different from the live report. Missing, malformed, unsupported-version,
or wrong-partition files are unavailable. The API checks access/database translation and does no scan.

There are no new per-record hooks, database scans, bitmap serializations, blocking locks, retries, or
predecessor waits. Enabled captures add hashing proportional to saved bytes, temporary sorting and
aggregation proportional to the immutable population, two cached hashes per immutable segment, and
synchronous compact JSON/file publication. Existing bitmap monitors are released before hashing, but
segment locks and startup calls still include that work. Measure representative rollover latency before
broad enablement; unknown coverage is preferable to extra work to complete a report.
