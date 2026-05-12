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
# Realtime consuming segment tier override

Realtime tables can use `tierOverwrites.consuming` to apply a richer index shape only while a segment is mutable and
consuming. The committed immutable segment still uses the persisted table config unless it is later assigned to a real
storage tier.

Use this when recent data is queried heavily but long-term storage should stay compact. For example, keep `userId` RAW
on disk, but add a dictionary and inverted index in the consuming segment so equality filters on fresh rows are fast.

Example query:

```sql
SELECT COUNT(*)
FROM userEvents
WHERE userId = 'u123';
```

In `userEvents_realtime_table_config.json`, the persisted shape is RAW and has no inverted index:

```json
"tableIndexConfig": {
  "noDictionaryColumns": ["userId"]
},
"fieldConfigList": [
  {
    "name": "userId",
    "encodingType": "RAW"
  }
]
```

The synthetic `consuming` tier override applies only to mutable consuming segments:

```json
"tableIndexConfig": {
  "noDictionaryColumns": ["userId"],
  "tierOverwrites": {
    "consuming": {
      "noDictionaryColumns": []
    }
  }
},
"fieldConfigList": [
  {
    "name": "userId",
    "encodingType": "RAW",
    "tierOverwrites": {
      "consuming": {
        "encodingType": "DICTIONARY",
        "indexes": {
          "inverted": {"enabled": true}
        }
      }
    }
  }
]
```

You normally do not need a `tierConfigs` entry named `consuming`. If a table already uses `consuming` as a real storage
tier name, Pinot keeps the existing storage-tier behavior and does not treat `tierOverwrites.consuming` as the synthetic
mutable consuming override for that table. Rename the real storage tier if the table should use this mutable-only
override.

When the persisted column is listed in `tableIndexConfig.noDictionaryColumns` or `noDictionaryConfig`, clear that
setting in `tableIndexConfig.tierOverwrites.consuming` so the consuming view can enable the dictionary.

`tableIndexConfig.tierOverwrites.consuming` is limited to index-loading settings such as dictionary, inverted, range,
JSON, Bloom filter, and related dictionary optimization options. Settings that change ingestion or row shape, such as
`aggregateMetrics` or `segmentPartitionConfig`, are not supported for the synthetic `consuming` tier.
