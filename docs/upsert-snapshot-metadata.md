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

The opt-in producer records two kinds of evidence: work skipped during an existing snapshot attempt,
and fingerprints of the saved bitmap population. The partition payload has no per-segment list and its
size does not grow with segment count. This is diagnostic evidence, not a recovery manifest.

Enable it through the existing table configuration:

```json
{
  "upsertConfig": {
    "mode": "FULL",
    "snapshot": "ENABLE",
    "metadataManagerConfigs": {"enableSnapshotMetadata": "true"}
  }
}
```

The default is false. The base partition manager supplies snapshot accounting and fingerprints to the
on-heap manager and its subclasses. Engines with asynchronous cleanup must override the cleanup
observation hook at the actual cleanup pass; a no-op startup TTL hook cannot establish completed cleanup.

## Snapshot outcomes

Version 2 records selected, successfully written, directly lock-skipped, otherwise skipped, and failed
segment counts, plus an aborted flag. A segment whose valid bitmap was written but whose queryable
bitmap failed counts as failed. Segments deferred because an earlier existing-file write/lock failed
count as otherwise skipped, not as direct lock failures. Retained unchanged segments are included in
content coverage but are not selected for a new write.

`runtimeEpoch` identifies one manager lifetime. Cumulative attempt, lock-affected attempt, and failed
attempt counters let a poller observe failures between polls. Compare counters only within that epoch.
`captureId` and timestamps identify local attempts; neither aligns replicas. Startup calls carry their
consumer name and opaque start offset. Manual/shutdown captures have no startup context. Early exits
before an attempt starts (snapshots disabled, initial full-upsert startup, or stopped manager) produce
no new attempt.

## Saved content and coverage

`content.populationFingerprint` covers the sorted segment names, data CRCs, and bitmap types of all
tracked immutable segments. `content.savedFilesFingerprint` additionally covers their raw saved file
hashes. Types are `VALID` and, for tables with a delete column, `QUERYABLE`. Length-prefixed UTF-8
fields and explicit domain separators avoid ambiguous concatenation. `algorithm` versions this encoding.
`expectedFiles` and `knownFiles` expose coverage. Missing CRCs, duplicate identities, or unknown file
contributions prevent a complete aggregate hash. The declared population does not certify that all
expected source segments are loaded.

Each immutable segment optionally caches two SHA-256 fingerprints. Existing snapshot writes hash the
already serialized bytes after successful persistence. Existing loads can seed an unchanged cache from
the bytes they already read. In-place snapshot deletion/write, untracking, and destruction invalidate
contributions; overlapping file changes remain unknown. No producer file reread is added to fill gaps.
Retained files contribute their cached hashes even when a later attempt writes no dirty segments.

Raw Roaring bytes are not canonical membership. Equivalent sets can serialize differently after
`runOptimize()`. A differing aggregate is a candidate requiring exact logical bitmap comparison.
For confirmation, use the existing saved-bitmap endpoint with its optional hash:

```http
GET /segments/orders_REALTIME/<segment>/validDocIdsBitmap?validDocIdsType=SNAPSHOT&includeSnapshotFingerprint=true
```

Use `SNAPSHOT_WITH_DELETE` for the queryable bitmap. `snapshotFileFingerprint` hashes the exact raw file
bytes read for that response. The response bitmap is reserialized and must not be hashed as a substitute.
A future checker must fetch every declared file with bounded concurrency, reproduce both aggregate
fingerprints using response identities/raw-file hashes, and require an exact match to its target report
before comparing logical bitmap memberships. Overwritten files, unknown hashes, changed CRCs or
population mismatch make the target unavailable; do not pair old metadata with today's bitmaps.
Equal cardinalities do not imply equal membership. No primary-key duplicate scan is performed.

## Boundary and cleanup limitations

`boundaryStatus` is always `UNVERIFIED` in version 2. `comparisonIssues` exposes incomplete outcomes,
unknown content/configuration, concurrent captures, consuming segments, observed lifecycle overlap,
and cleanup overlap/failure. Manager-local epochs cannot observe every outer reload/directory change,
attest stream identity across configuration changes, or establish settled predecessor processing in
pauseless ingestion. `SOURCE_BOUNDARY_UNVERIFIED` therefore remains present even in otherwise quiet
captures. Matching offsets, config hashes and files cannot promote this to a verified source boundary.

Cleanup observations distinguish disabled, not yet run, running, completed, and possibly partial failed
passes. A target watermark becomes completed only after an actual pass and its bookkeeping succeed.
Failure retains the previous completed watermark. Historical before/after observations are immutable;
the API exposes current cleanup separately. Manager lifecycle failure totals remain visible for the
manager lifetime. These observations do not synchronize or delay cleanup.

A checker can confirm that saved contents differ when it binds the fetched files successfully. Calling
that difference replica divergence at the same source boundary additionally requires producer boundary
attestation, which is not implemented here. Missing/unverified evidence is not agreement. Controller
scheduling, logical comparison/alerts, and SRT integration are separate work.

## Publication and cost

At the end of each actual attempt, the producer publishes its immutable report in memory and
synchronously atomically replaces this server-local file:

```text
<table data directory>/upsert.snapshot.metadata.partition.<partitionId>.json
```

The sidecar is not uploaded to deep store. A metadata write failure leaves recovery bitmaps intact and
keeps the current in-memory report readable with its failure status. Only one report is retained; a
historical file can lag live state and concurrent completions can persist in a different order.

```http
GET /tables/orders_REALTIME/upsertSnapshotMetadata/3
```

The response reports `availability`, `partitionId`, `snapshot`, and `source: LIVE` or `HISTORICAL_FILE`.
Live responses also include `persistenceStatus`, cumulative `publicationFailures`, and `cleanupNow`.
Missing, malformed, unsupported-version or wrong-partition sidecars are unavailable. The metadata API
uses access control and database translation and never takes snapshots, reads bitmaps or scans keys.

There are no new per-record hooks, database scans, bitmap serializations, blocking locks, retries, or
waits for predecessor readiness. Enabled captures do add hashing proportional to serialized bytes,
transient aggregation/sorting proportional to the tracked immutable population, two optional hash
strings per live immutable segment, and synchronous compact JSON/file publication. Existing bitmap
monitors are released before hashing, but the segment lock and startup call still include that work.
This is not a zero-latency guarantee; measure rollover latency with representative snapshot sizes before
enabling broadly. Unknown coverage is retained instead of adding work to make a capture comparable.

A local Java 25.0.3/aarch64 smoke measurement (100 warmed samples, no production traffic) observed
SHA-256 medians of 0.42 ms for 1 MiB and 6.79 ms for 16 MiB, and aggregate medians of 0.28 ms for
1,000 files and 3.67 ms for 10,000 files. These isolate hashing/aggregation and exclude serialization,
file I/O, lock contention and end-to-end startup latency. They are not a benchmark guarantee.
