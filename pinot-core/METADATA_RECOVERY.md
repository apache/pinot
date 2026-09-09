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

# Recovery after realtime metadata removal failure

An offload failure can leave shared upsert/dedup metadata referring to a discarded consuming segment.
The consumer permit is still released. This change reports the failure; it does not automatically
block ingestion or queries, repair metadata, or make a subsequent Helix reset safe.

## Detection

Alert on any increase of the table meter `REALTIME_METADATA_REMOVAL_FAILURES`
(`realtimeMetadataRemovalFailures` in Pinot's metric naming). Look for
`REALTIME_METADATA_REMOVAL_FAILED` in server logs and retain the first occurrence, full exception,
table, partition, source segment, instance and time. Realtime offload also records the message in
the existing segment error cache. The ConcurrentMap upsert backend emits the same meter/log for
its existing fallback that removes a key when the predecessor reader or valid-document bitmap is
unavailable. Those fallback errors need recovery even if the Helix transition succeeds.

For pauseless tables, also monitor `PAUSELESS_SEGMENTS_IN_UNRECOVERABLE_ERROR_COUNT`, segment ERROR
states and consumption lag. That controller gauge is supplementary: it need not rise for a single
failed replica, a caught row error, or a swallowed reader error. Do not wait for every replica to fail.

Example local log search (use the equivalent filters in the deployed log service):

```sh
rg -n 'REALTIME_METADATA_REMOVAL_FAILED|UPSERT_METADATA_FAILURE' /path/to/pinotServer.log
```

## Manual procedure

1. Capture the earliest affected segment per stream partition from the logs. Save the table config,
   segment ZK metadata, start/end stream offsets, server errors and storage diagnostics. Verify that
   the source still retains every offset needed to replay from the **start of the affected segment**.
   If that boundary or a valid predecessor cannot be established, restore a verified earlier backup
   or rebuild a replacement table from the source. Do not guess an offset or resume at `largest`.
2. Pause consumption. Record the job/status and wait for the pause flag; also check consumers have
   stopped. Pause can itself cause commits/offloads, so capture any additional failures and expand
   the affected segment range. If workers cannot quiesce, stop the affected server processes using
   the deployment's service manager. Keep affected table traffic away from replicas with suspect
   metadata during recovery; this PR does not fence their query results.
3. For a **pauseless** table, preview the suffix deletion below. Check it contains the failed segment
   and **all segments with sequence >= its sequence in the same partition**, across replicas. Pass
   the earliest affected segment for each affected partition. Save the preview, then execute it with
   `dryRun=false`. Leave `force=false`; do not bypass the paused-table check.
4. Rebuild local metadata on every replica that consumed the affected suffix. Stop its server before
   touching local state. For ConcurrentMap upsert, retire the old table manager with a server restart
   and quarantine `validdocids.bitmap.snapshot` and `queryabledocids.bitmap.snapshot` for retained
   segments of the affected partition so loading reconstructs visibility from retained segment data.
   Check downloaded/prebuilt snapshots too. For an external metadata backend, use its recovery guide
   (RocksDB: the companion `startree-rocksdb-upsert/METADATA_RECOVERY.md`). Deleting segment records
   in the controller alone does not rebuild the metadata backend.
5. Confirm the retained predecessor's end offset is the intended replay boundary. Resume with
   `consumeFrom=lastConsumed`, then verify the new consumer's actual start offset against that saved
   boundary. Pinot can clamp expired offsets to the first available offset; that is data loss, not a
   successful replay. If no valid retained predecessor exists, use the verified backup/replacement
   table recovery from step 1 instead of this suffix procedure.
6. Before returning traffic, wait for replicas to catch up and compare representative primary keys,
   versions, deleted rows and partial aggregates against source events or a verified replica. Check
   replica results agree, new errors have stopped, and segment states and lag recover. Restore any
   temporary settings only after a clean reconstruction is verified.

Controller command templates (supply the deployment's normal authentication; URL-encode names):

```sh
CONTROLLER='https://controller.example'
TABLE='example_REALTIME'
FAILED_SEGMENT='example__0__42__20260909T0000Z'

curl --fail-with-body -X POST "$CONTROLLER/tables/$TABLE/pauseConsumption"
curl --fail-with-body "$CONTROLLER/tables/$TABLE/pauseStatus"
curl --fail-with-body "$CONTROLLER/tables/$TABLE/consumingSegmentsInfo"
curl --fail-with-body "$CONTROLLER/segments/$TABLE/zkmetadata"

# Preview only. Repeat the segments parameter for other affected partitions.
curl --fail-with-body -X DELETE   "$CONTROLLER/deleteSegmentsFromSequenceNum/$TABLE?segments=$FAILED_SEGMENT&dryRun=true&force=false"
# After checking the preview and preserving the replay offsets:
curl --fail-with-body -X DELETE   "$CONTROLLER/deleteSegmentsFromSequenceNum/$TABLE?segments=$FAILED_SEGMENT&dryRun=false&force=false"

# Only after rebuilding metadata and verifying the retained predecessor/replay boundary:
curl --fail-with-body -X POST   "$CONTROLLER/tables/$TABLE/resumeConsumption?consumeFrom=lastConsumed"
```

For a non-pauseless table, the suffix endpoint rejects the request with `force=false`. Use the
established table recovery workflow to delete the saved affected segment set while paused and
rebuild every affected replica, or rebuild a replacement table. Do not force the pauseless endpoint
as a substitute for validating offsets and metadata.

## Deferred work and its detection

| TODO | Detection | Manual response |
| --- | --- | --- |
| Automatically complete failed offload / handle one-shot reset and lost segment references | New meter/log and offload's segment error | Pause, reconstruct every affected replica, replay the suffix; reset/restart alone is insufficient |
| Recover ConcurrentMap predecessor-reader failures instead of removing the key | New meter/log includes source and previous segment | Same reconstruction/replay procedure; transition success is not evidence of correctness |
| Roll back every effect of a skipped ingestion row | Existing row indexing errors/`ROWS_WITH_ERRORS`; RocksDB companion adds `UPSERT_METADATA_FAILURE` | Follow the backend guide; include all later rows/segments that may have merged from the skipped update |
| Automatically protect queries, already-admitted parallel consumers and all replacement paths | Metadata failure logs, replica comparisons, segment errors; no universal detector for silent divergence | Pause and route away from suspect replicas; expand the replay range to the earliest suspect source segment |

This procedure is an operator guide, not an automated cluster recovery test. Exceptions already
caught inside other metadata backends need backend-specific detection. Silent divergence without
an exception remains outside this change; replica/source comparisons are required to detect it.
