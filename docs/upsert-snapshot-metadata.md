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

# Upsert snapshot partition context

An optional server-local sidecar records the partition context of an existing startup snapshot attempt.
It contains the triggering consumer, its observed start offset, and the capture-attempt start time.
There is no per-segment inventory, CRC, or document count. Payload size does not grow with the number
of tracked or captured segments.

Version 1 always reports `boundaryStatus: UNVERIFIED`. Equal startup offsets do not prove comparable
state: predecessor replacement, replay and background cleanup may still differ. This partition context
is not bound to individual bitmap files or separately fetched counts.

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
partition implementation can use it. At the end of the existing startup snapshot attempt, the consuming
thread synchronously serializes the compact context and atomically replaces one file per partition:

```text
<table data directory>/upsert.snapshot.metadata.partition.<partitionId>.json
```

There is no background writer, queue, per-record work, additional bitmap serialization, or wait for
predecessor reconciliation. The extra serialization and file I/O complete before the next consumer starts
reading records. A failed metadata write is logged and leaves the bitmap snapshots intact. The API can
continue to return a previous context if replacement fails. Recovery bitmap write ordering is unchanged.
The sidecar is not uploaded to deep store. Manual or shutdown snapshots do not reuse a startup offset.

Read the last successfully persisted context through the server API:

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
| `boundaryStatus` | Always `UNVERIFIED` in version 1. |

Individual bitmap files may have been skipped or unchanged during the recorded attempt, or overwritten
later. Consumers must not label those files or their counts with this partition offset. SRT may display
whether replicas report different partition startup contexts, but that does not establish the cause of
a separately observed segment-count difference or make matching contexts comparable.

This first version retains only the last persisted attempt per partition. It defers binding context to
per-segment snapshots, common boundary verification, lifecycle instrumentation, cleanup/configuration
fingerprints, history, membership digests, and controller scheduling. Consumers must treat missing or
unverified evidence as unavailable for
a definitive divergence verdict, not as agreement. A raw count-disagreement metric remains an operational
observation and must not be presented as a verified divergence alarm.
