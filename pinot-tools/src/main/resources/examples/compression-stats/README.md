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

# Compression statistics

Pinot can persist compression inputs while building a segment and expose table- and column-level statistics through
the existing table size and metadata APIs. Collection is disabled by default.

## Enable collection

Set `tableIndexConfig.compressionStatsEnabled` to `true`. See
[`compressionStats_offline_table_config.json`](compressionStats_offline_table_config.json) for a complete example.
The setting affects segments built or forward indexes rewritten while it is enabled. Existing segments are not backfilled.

Raw forward indexes record the uncompressed serialized column-value bytes presented to their chunk compressor and the
resolved chunk-compression type. Dictionary-encoded columns record uncompressed serialized column-value bytes; their
`forwardIndexAndDictionaryStorageSizeInBytes` includes both the dictionary and forward-index files. Columns without a
forward index and old segments without value-size metadata are excluded from compression ratios.

## Inspect realtime segment size metadata

For a completed realtime segment, `segment.size.in.bytes` is the logical size of the finalized, uncompressed immutable
segment. Before publishing this value, the committing server applies the latest table index configuration. For example,
if an inverted index was removed while the segment was consuming, the removed index is not included in the published
size. Deep-store recovery also refreshes the value from the immutable segment loaded by the selected server.

This field is not the compressed archive size and does not represent filesystem-allocated blocks reported by tools
such as `du`. A later segment reload can also make replica sizes differ from the shared metadata snapshot.

To compare the metadata snapshot with the currently loaded copies of one realtime segment:

```bash
CONTROLLER='http://localhost:9000'
TABLE='events'
SEGMENT='events__0__0__20260714T0000Z'

curl -sS "$CONTROLLER/segments/${TABLE}_REALTIME/$SEGMENT/metadata" \
  | jq '{segmentSizeInBytes: (."segment.size.in.bytes" | tonumber)}'

curl -sS "$CONTROLLER/tables/$TABLE/size?verbose=true" \
  | jq --arg segment "$SEGMENT" \
    '.realtimeSegments.segments[$segment].serverInfo
     | to_entries[]
     | {server: .key, diskSizeInBytes: .value.diskSizeInBytes}'
```

The first value is one shared segment metadata snapshot and does not include replication. The second request reports
each loaded replica separately.

## Query statistics

Request the table-size summary and per-segment details:

```bash
curl -sS 'http://localhost:9000/tables/compressionStats/size?verbose=true'
```

Per-column details are opt-in because they can be large:

```bash
curl -sS 'http://localhost:9000/tables/compressionStats/size?verbose=true&includeColumnCompressionStats=true'
curl -sS 'http://localhost:9000/tables/compressionStats/metadata?type=OFFLINE&includeColumnCompressionStats=true'
curl -sS 'http://localhost:9000/tables/compressionStats/metadata?type=OFFLINE&columns=message&includeColumnCompressionStats=true'
```

The table summary reports `uncompressedValueSizePerReplicaInBytes`,
`forwardIndexAndDictionaryStorageSizePerReplicaInBytes`, `compressionRatio`, `segmentsWithCompleteStats`,
`totalSegments`, and `partialCoverage`. A segment contributes to the table summary only when all of its eligible
forward-index columns have usable uncompressed-value and forward-index/dictionary storage sizes. A column entry
separates forward-index encoding from the resolved chunk-compression type:

In verbose size responses, Pinot selects one complete replica for each logical segment. Compression fields are attached
only to that replica's `segments.<segment>.serverInfo.<server>` entry; other replicas continue to report disk size only.

```json
{
  "column": "message",
  "uncompressedValueSizeInBytes": 120000,
  "forwardIndexAndDictionaryStorageSizeInBytes": 18000,
  "compressionRatio": 6.666666666666667,
  "observedIndexes": ["forward_index"],
  "encodingBreakdown": [
    {
      "encoding": "RAW",
      "chunkCompressionType": "ZSTANDARD",
      "numSegments": 12,
      "uncompressedValueSizeInBytes": 120000,
      "forwardIndexAndDictionaryStorageSizeInBytes": 18000
    }
  ],
  "numSegments": 12
}
```

When a column uses multiple encodings or chunk-compression types across segments, `encodingBreakdown` contains one
entry for each combination. Dictionary entries use `"encoding": "DICTIONARY"` and omit `chunkCompressionType`.

## Rolling upgrades and coverage

The metadata API requests replica contributions in bounded batches and de-duplicates them with the same policy as the
size API. Detailed server responses are limited to 10,000 segment-column contributions; the controller sizes requests
conservatively and automatically splits a batch if a server rejects it as too large. Unsupported, unreachable, or
unloaded replicas make `partialCoverage` true only when no current replica can provide statistics for the logical
segment. Old segments continue to make `partialCoverage` true until they are rebuilt.

## Metrics

The lead controller emits these per-table gauges when compression statistics are available:

| Gauge | Unit |
| --- | --- |
| `tableCompressionStatsRatioPercent` | Ratio multiplied by 100 |
| `tableCompressionStatsUncompressedValueSizePerReplica` | Bytes |
| `tableCompressionStatsForwardIndexAndDictionaryStorageSizePerReplica` | Bytes |

The gauges are removed when collection is disabled or no covered segment remains.
