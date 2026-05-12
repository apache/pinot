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
# Realtime consuming segment index profile

`realtimeConfig.consumingSegmentIndexConfig` lets a realtime table use a richer index shape only while a segment is
mutable and consuming. The committed immutable segment still uses the persisted table config.

Use this when recent data is queried heavily but long-term storage should stay compact. For example, keep `userId` RAW
on disk, but add a dictionary and inverted index in the consuming segment so equality filters on fresh rows are fast.

Example query:

```sql
SELECT COUNT(*)
FROM userEvents
WHERE userId = 'u123';
```

In `userEvents_realtime_table_config.json`, the persisted shape is RAW:

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

The consuming profile applies only to mutable consuming segments:

```json
"realtimeConfig": {
  "consumingSegmentIndexConfig": {
    "fieldConfigList": [
      {
        "name": "userId",
        "encodingType": "DICTIONARY",
        "indexes": {
          "inverted": {
            "enabled": true
          }
        }
      }
    ]
  }
}
```

Supported fields in the first version are intentionally narrow:

- `encodingType`
- `indexes.inverted`

The profile is not a storage tier. `tierOverwrites` continue to apply only to immutable segments loaded on real segment
tiers.
