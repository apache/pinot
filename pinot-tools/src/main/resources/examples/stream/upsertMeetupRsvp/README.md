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

# Server ingestion OOM protection

Server ingestion OOM protection applies local backpressure to realtime ingestion when the server JVM heap is above the
configured threshold. It is disabled by default. Set the server mode to `UPSERT_DEDUP_ONLY` to protect upsert and dedup
realtime tables, or `ENABLE` to protect all realtime tables. When active, the consuming loop waits and checks heap usage
again at the configured interval, then resumes ingestion after heap usage reaches the recovery threshold. While
throttled, Pinot also requests JVM garbage collection at a rate-limited interval so a mostly ingestion-only server does
not stay paused forever with reclaimable garbage still counted as used heap.

Server-level properties are set on the server instance config:

```properties
pinot.server.instance.ingestion.oom.protection.mode=UPSERT_DEDUP_ONLY
pinot.server.instance.ingestion.oom.protection.heapUsageThrottleThreshold=0.95
pinot.server.instance.ingestion.oom.protection.heapUsageRecoveryThreshold=0.90
pinot.server.instance.ingestion.oom.protection.checkIntervalMs=1000
pinot.server.instance.ingestion.oom.protection.gcIntervalMs=30000
```

The same server properties can be set in the Pinot cluster config and updated dynamically at runtime. Runtime cluster
config changes use the same full `pinot.server.instance.*` property names:

```json
{
  "pinot.server.instance.ingestion.oom.protection.mode": "UPSERT_DEDUP_ONLY",
  "pinot.server.instance.ingestion.oom.protection.heapUsageThrottleThreshold": "0.95",
  "pinot.server.instance.ingestion.oom.protection.heapUsageRecoveryThreshold": "0.90",
  "pinot.server.instance.ingestion.oom.protection.checkIntervalMs": "1000",
  "pinot.server.instance.ingestion.oom.protection.gcIntervalMs": "30000"
}
```

Use `pinot.server.instance.ingestion.oom.protection.mode=ENABLE` to apply the server-level policy to all realtime tables.
Use `pinot.server.instance.ingestion.oom.protection.mode=DISABLE` to disable the server-level policy. Set
`pinot.server.instance.ingestion.oom.protection.gcIntervalMs=0` to disable the explicit GC request while throttled.

Each realtime table can override the server policy under `ingestionConfig.streamIngestionConfig`:

```json
"oomProtection": "ENABLE"
```

If table `oomProtection` is unset or `DEFAULT`, the table follows the server mode. Set it to `ENABLE` to
protect this table even when the server mode is `DISABLE` or would otherwise skip it, or `DISABLE` to turn protection
off for this table. Thresholds are configured at the server level only.
