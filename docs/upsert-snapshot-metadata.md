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

# Upsert snapshot count metadata

An optional server-local sidecar records the counts written during an existing startup snapshot attempt.
It helps explain count differences between replicas by exposing the observed startup offset and which
segments were captured. Version 1 always reports `boundaryStatus: UNVERIFIED`. Equal startup offsets do
not prove comparable state: predecessor replacement, replay and background cleanup may still differ.

Enable it through the table's existing upsert metadata-manager configuration:

```json
{
  "upsertConfig": {
    "mode": "FULL",
    "snapshot": "ENABLE",
    "metadataManagerConfigs": {
      "enableSnapshotMetadata": "true"
    }
  }
}
```

`enableSnapshotMetadata` defaults to false. Both the on-heap manager and managers extending the base
partition implementation can use it. There is no new work in per-record ingestion, no additional bitmap
serialization and no wait for predecessor reconciliation. Capture adds bounded per-segment count objects;
a single background writer serializes JSON and atomically replaces one file per partition:

```text
<table data directory>/upsert.snapshot.metadata.partition.<partitionId>.json
```

The writer queue holds at most eight summaries; a summary contains at most 10,000 segment entries.
Larger captures set `truncated: true`. A full queue drops the new diagnostic summary without waiting.
Publication failure preserves any previous summary. CPU, allocation and disk overhead still need to be
measured before enabling the feature for a production workload. Recovery bitmap files and their write
ordering are unchanged. The sidecar is not uploaded to deep store.

Read the last successfully published summary through the server API:

```http
GET /tables/orders_REALTIME/upsertSnapshotMetadata/3
```

The response has `availability: AVAILABLE`, `partitionId`, and `snapshot`, or `availability: UNAVAILABLE`
when the sidecar is missing, invalid or unsupported. Table access control and database-name translation
apply. The API reads only the sidecar; it never triggers a snapshot or scans bitmaps or primary keys.

The snapshot contains:

| Field | Meaning |
|---|---|
| `formatVersion` | Serialization version; version 1 cannot certify a logical boundary. |
| `partitionId`, `consumingSegmentName`, `startOffset` | Partition and triggering consumer's observed starting offset. The offset is opaque and is not proof that preceding effects are reconciled. |
| `capturedAtMillis` | Capture-attempt start time for diagnosing age, not cross-replica alignment. |
| `numTrackedSegments`, `numConsumingSegments`, `numUnchangedSegments` | Existing snapshot-loop counts explaining omitted coverage. |
| `truncated` | Whether the diagnostic entry limit omitted successfully captured segments. |
| `segments` | Segment name to content CRC, captured valid-doc count and optional queryable-doc count. |
| `boundaryStatus` | Always `UNVERIFIED` in version 1. |

Only successful writes from this attempt appear in `segments`. Skipped and unchanged snapshots are not
relabeled with a newer offset. A missing queryable count is not zero. Counts in this summary are
self-contained historical observations: never pair its context with today's bitmap or a live count.
They can remain available after restart or after the original segment was removed. No bitmap-integrity
hash or historical bitmap retention is required for this count-only diagnostic artifact.

This first version retains only the last published attempt per partition. It deliberately defers common
boundary verification, lifecycle instrumentation, cleanup/configuration fingerprints, history, membership
digests, and controller scheduling. Consumers must treat missing or unverified evidence as unavailable for
a definitive divergence verdict, not as agreement. A raw count-disagreement metric remains an operational
observation and must not be presented as a verified divergence alarm.
