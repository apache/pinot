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
# Stored table definition migration

The controller inspects physical table configs and their shared schemas every hour.
By default it only logs warnings and records recommendations; it does not change stored definitions.

The initial rules move legacy stream/batch ingestion properties into `ingestionConfig`, convert
schema `timeFieldSpec` to `dateTimeFieldSpecs`, and move schema-field `transformFunction` properties
into each affected table's `ingestionConfig.transformConfigs`. Existing table transforms take precedence.
Unsupported conversions are left unchanged. Modern definitions are no-ops, including ones without
migration markers. A later edit that reintroduces legacy fields is detected again.

After all cluster components have been upgraded and rollback requirements have been reviewed, enable
updates through the cluster configuration API:

```sh
curl -X POST 'http://localhost:9000/cluster/configs' \
  -H 'Content-Type: application/json' \
  -d '{"controller.config.migration.auto.update.enabled":"true"}'
```

This cluster flag controls both schema and table-config updates. Controllers pick up changes without
a restart; updates run on the next scan. Leave it unset, delete it, or set it to `false` for warning-only mode.
The scan schedule remains a controller setting: `controller.config.migration.frequencyPeriod=1h` by default,
or `-1s` to disable the scan entirely.

For manual updates, move `tableIndexConfig.streamConfigs` into
`ingestionConfig.streamIngestionConfig.streamConfigMaps` as a one-element list. Move
`segmentsConfig.segmentPushType` and `segmentPushFrequency` into
`ingestionConfig.batchIngestionConfig.segmentIngestionType` and `segmentIngestionFrequency`.
TIME fields require an equivalent DATE_TIME format, granularity, and transform; unsupported time
conversions require manual review rather than automatic rewriting.

For each nonempty, nonvirtual schema-field transform, add a table transform with the same `columnName`
and `transformFunction` unless that table already overrides the column. Both OFFLINE and REALTIME table
configs are checked before removing the shared schema transforms. TIME conversion transforms generated
by the preceding rule are moved the same way. Empty, virtual, and nested-child transforms are left unchanged.

The controller validates all candidates, checks every affected table's captured version, writes changed
table configs, and removes the schema transforms in one atomic ZooKeeper transaction. A failed validation
or version conflict leaves all stored definitions unchanged. Table creation atomically advances the
schema's ZooKeeper version without changing its contents, so a new table also invalidates an in-flight
migration plan. Ingestion metadata readers fetch schema before table config to avoid combining an old
table config with a schema whose transforms have already moved.
Transforms whose behavior would change through template resolution or decorators are left on the schema.

Recommendation meters are `CONFIG_MIGRATION_RECOMMENDED` per physical table and
`SCHEMA_MIGRATION_RECOMMENDED` per shared schema. They count recommendations on each scan, rather than
unique definitions. `CONFIG_MIGRATION_SUCCESS` counts successful writes and `CONFIG_MIGRATION_FAILURE`
counts failed validation or update attempts. Schemas are counted once across hybrid table halves per run.

Automatic updates validate candidates and use version-checked writes. Concurrent edits/deletions and
stored fields that cannot survive a typed round trip prevent the update. Resolved environment values
are used for table validation and runtime actions; raw templates remain in storage. Schema-transform
migrations coordinate table and schema writes; other rules retain their independent update paths.
