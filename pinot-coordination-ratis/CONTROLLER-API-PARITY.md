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
# Controller API parity implementation tracker

Baseline inventory: PR #19959 (`701da8dda10101efe3fe6d3a790fae6db894fc96`).

The standard resource package has **264 annotated HTTP methods across 43 resource classes**. Duplicate verb/path pairs include multipart vs JSON overloads. Class path prefixes and segment-completion constants are resolved below. This is an endpoint inventory, not a claim that all resource classes are enabled under every legacy configuration. NativeControllerResource currently exposes 11 metadata GET methods plus seven conditional mutations; the native starter separately registers landing/auth/version/appconfig/health resources.

## Implementation status

- Baseline: validated configuration reads/writes, controller authentication, HTTP/HTTPS, and metadata UI.
- Table sizes: shared table-size metadata adapter, optional read-only native server admin endpoint,
  identity-checked native OFFLINE table-size REST/UI support. Query listeners remain disabled.
- Native minions: explicit `MINION_TASK` authorization, durable bounded schedules, normal minion startup and atomic
  output publication for `NativeSegmentGenerationTask`; see [the task contract](MINION-TASK-PLAN.md).
- Native immutable OFFLINE uploads use the normal multipart `POST /v2/segments` endpoint with captured table/schema
  validation, durable content-addressed archives and fenced publication. Published content, including minion outputs,
  has bounded automatic replica placement from explicit administrator-owned tenant/domain policy, durable installation
  intent and exact server receipts. The task API reports publication, desired and installed replicas separately.
  This does not provide replacement/deletion, general rebalance, physical cleanup or query-serving qualification.
- Native broker routing: authenticated, tenant-scoped table discovery and complete OFFLINE routing observations,
  immutable broker images and query-identity construction. Query planning, dispatch and serving activation remain
  outstanding; installed replicas without query authority are not selectable.
- Outstanding: remaining offline lifecycle, full existing task API/plugin parity, realtime,
  remaining feature families and query-serving qualification. A full replacement is not yet implemented.
- ZooKeeper tree APIs are not applicable to the replacement; a mutable compatibility tree will not be introduced.

Every added family needs real backend behavior, compatible authorization and failure semantics, recovery coverage,
and a declared UI capability. Empty successful responses and desired-as-observed state do not satisfy parity.

## Dependencies and implementation order

1. **Shared observability boundary**: extract immutable table-size read inputs from Helix without changing the existing constructor, DTOs, fan-out, estimation, compression selection, or metrics. Keep live ownership checks at metric publication. Do not fabricate a native implementation.
2. **Native server administration and membership endpoint binding**: real authenticated admin listener; bounded read-only size/segment metadata requests; bind endpoint/report to server registration and installed table/content/materialization identities. Preserve held-segment lifetime. Configure server admin auth/TLS using current service config. Only then expose real controller size/stats/index/instance observations.
3. **Offline control loop**: autonomous controller membership + table ownership, publication scheduling, assignment selection, server-restart reassignment, durable retryable operations, replica status observation, table/schema refresh, reload/reset, segment upload/download/replacement/delete and table deletion. Physical bytes are reclaimed only after drain/reference and replacement rules; never treat desired assignment as observed ONLINE.
4. **Query integration**: routing image, owner/registration/content fences on broker-to-server and result delivery, authenticated native transports, fault/partition and stale-session testing, then explicitly enable query listeners. Controller query/running-query proxy requires this.
5. **Minion framework**: backend-neutral task gateway plus durable schedules/jobs/subtasks/attempts, exact MINION registration leases, queue limits, retry/timeout/cancel/drain state machine, worker execution adapters, progress/config APIs, task metadata CAS, task locks, idempotent/fenced segment output publication, recovery and retention. An in-memory scheduler or a replacement TaskDriver facade is insufficient.
6. **Realtime**: consuming assignment and partition ownership, offset/sequence monotonicity, fenced LLC commit election/upload/metadata finalization, pause/resume/force commit, stream metadata, watermarks, realtime-to-offline bridge, retention/upsert/dedup.
7. **Remaining feature families**: tenants/quotas/logical/materialized tables, database cascade semantics, query workload, users and auth rotation, tiers/instance partitions, periodic task framework, index/page-cache/debug APIs. Raw `/zk/*` has no storage-independent semantics and must explicitly be unsupported/not-applicable; do not emulate a mutable ZooKeeper tree. IdealState/ExternalView paths need documented read-only compatibility projections of desired state and independently verified observed state, never synthetic Helix objects used as authority.

## Table-size correctness

`TableSize` exposes GET `/tables/{tableName}/size`; keep database scoping and `GET_SIZE` authorization, typed/raw-name compatibility, verbose filtering, includeReplacedSegments, compression-mode flags, missing-table result and response DTOs. `TableSizeReader` currently directly reads ZK table configs, Helix desired server-to-segments map, server admin URLs/auth, replica count and LeadControllerManager. `ServerTableSizeReader` already offers backend-independent HTTP fan-out. Server `TableSizeResource` is independent of Helix after injection of ServerInstance: it acquires/releases actual segment references and calls ImmutableSegment.getSegmentSizeBytes(). The baseline did not start native admin resources; this change adds the explicit read-only size listener.

Reported size is actual untarred segment disk bytes returned by each expected server replica. Estimated size substitutes the largest observed replica of the same segment for each missing expected replica. Entirely unreported segments use -1 and increment missingSegments. Per-replica size sums each segment's largest report. Do not use compressed archive bytes, manifest byte count, number of rows, or zero placeholders. Extra unassigned/unselected files and old installation directories must not count. Missing/stale/unreachable replicas remain visible in coverage; desired assignment is not proof of installation. Replaced-segment filtering needs durable lineage. Preserve existing versioned compression responses, selective lookup, deadlines, auth-provider credential rotation and table flag gating. Replica/table/schema/content identities must prevent a reused segment/server name from contributing an old report. Live controller owner must gate metrics after polling.

## Complete source endpoint list

### metadata

- **PinotAccessControlUserRestletResource**: `GET /users`; `GET /users/{username}`; `POST /users`; `DELETE /users/{username}`; `PUT /users/{username}`.

- **PinotApplicationQuotaRestletResource**: `GET /applicationQuotas`; `GET /applicationQuotas/{appName}`; `POST /applicationQuotas/{appName}`.

- **PinotClusterConfigs**: `GET /cluster/info`; `GET /cluster/configs`; `POST /cluster/configs`; `DELETE /cluster/configs/{configName}`; `GET /cluster/configs/groovy/staticAnalyzerConfig`; `POST /cluster/configs/groovy/staticAnalyzerConfig`; `GET /cluster/configs/groovy/staticAnalyzerConfig/default`.

- **PinotDatabaseRestletResource**: `GET /databases`; `DELETE /databases/{databaseName}`; `POST /databases/{databaseName}/quotas`; `GET /databases/{databaseName}/quotas`.

- **PinotDdlRestletResource**: `POST /sql/ddl`.

- **PinotLogicalTableResource**: `GET /logicalTables`; `GET /logicalTables/{tableName}`; `POST /logicalTables`; `PUT /logicalTables/{tableName}`; `DELETE /logicalTables/{tableName}`.

- **PinotMaterializedViewRestletResource**: `GET /materializedViews`; `GET /materializedViews/{materializedViewTableName}`; `DELETE /materializedViews/{materializedViewTableName}`.

- **PinotQueryWorkloadRestletResource**: `GET /queryWorkloadConfigs`; `GET /queryWorkloadConfigs/{queryWorkloadName}`; `GET /queryWorkloadConfigs/instance/{instanceName}`; `POST /queryWorkloadConfigs`; `DELETE /queryWorkloadConfigs/{queryWorkloadName}`; `POST /queryWorkloadConfigs/refresh`.

- **PinotSchemaRestletResource**: `GET /schemas`; `GET /schemas/info`; `GET /schemas/{schemaName}`; `DELETE /schemas/{schemaName}`; `PUT /schemas/{schemaName}`; `PUT /schemas/{schemaName}`; `POST /schemas`; `POST /schemas`; `POST /schemas/validate`; `POST /schemas/validate`; `GET /schemas/fieldSpec`.

- **PinotTableSchema**: `GET /tables/{tableName}/schema`.

- **TableConfigsRestletResource**: `GET /tableConfigs`; `GET /tableConfigs/{tableName}`; `POST /tableConfigs`; `DELETE /tableConfigs/{tableName}`; `PUT /tableConfigs/{tableName}`; `POST /tableConfigs/validate`; `POST /tableConfigs/tune`.

### membership and placement

- **PinotBrokerRestletResource**: `GET /brokers`; `GET /brokers/tenants`; `GET /brokers/tenants/{tenantName}`; `GET /brokers/tables`; `GET /brokers/tables/{tableName}`; `GET /v2/brokers`; `GET /v2/brokers/tenants`; `GET /v2/brokers/tenants/{tenantName}`; `GET /v2/brokers/tables`; `GET /v2/brokers/tables/{tableName}`; `POST /brokers/instances/{instanceName}/qps`.

- **PinotInstanceAssignmentRestletResource**: `GET /tables/{tableName}/instancePartitions`; `POST /tables/{tableName}/assignInstances`; `PUT /tables/{tableName}/instancePartitions`; `DELETE /tables/{tableName}/instancePartitions`; `POST /tables/{tableName}/replaceInstance`.

- **PinotInstanceRestletResource**: `GET /instances`; `GET /liveinstances`; `GET /instances/{instanceName}`; `POST /instances`; `PUT /instances/{instanceName}/state`; `POST /instances/{instanceName}/state`; `DELETE /instances/{instanceName}`; `PUT /instances/{instanceName}`; `PUT /instances/{instanceName}/updateTags`; `POST /instances/{instanceName}/updateBrokerResource`; `GET /instances/dropInstance/validate`; `POST /instances/updateTags/validate`.

- **PinotLeadControllerRestletResource**: `GET /leader/tables`; `GET /leader/tables/{tableName}`.

- **PinotTableInstances**: `GET /tables/{tableName}/instances`; `GET /tables/livebrokers`; `DELETE /tables/{tableName}/{instanceId}/ingestionMetrics`.

- **PinotTableTenantConfigs**: `POST /tables/{tableName}/rebuildBrokerResourceFromHelixTags`.

- **PinotTenantRestletResource**: `POST /tenants`; `PUT /tenants`; `GET /tenants`; `GET /tenants/{tenantName}`; `POST /tenants/{tenantName}`; `GET /tenants/{tenantName}/tables`; `GET /tenants/{tenantName}/instancePartitions`; `PUT /tenants/{tenantName}/instancePartitions`; `GET /tenants/{tenantName}/metadata`; `POST /tenants/{tenantName}/metadata`; `DELETE /tenants/{tenantName}`; `DELETE /tenants/rebalance/{jobId}`; `POST /tenants/{tenantName}/rebalance`; `GET /tenants/rebalanceStatus/{jobId}`; `GET /tenants/{tenantName}/rebalanceJobs`.

### offline lifecycle and server observability

- **DebugResource**: `GET /debug/tables/{tableName}`; `GET /debug/segments/{tableName}/{segmentName}`.

- **PageCacheWarmupRestletResource**: `GET /pagecache/queries/{tableName}`; `POST /pagecache/queries/{tableName}`; `DELETE /pagecache/queries/{tableName}`.

- **PinotIngestionRestletResource**: `POST /ingestFromFile`; `POST /ingestFromURI`.

- **PinotSegmentRestletResource**: `GET /segments/{tableName}`; `GET /segments/{tableName}/servers`; `GET /segments/{tableName}/lineage`; `GET /segments/{tableName}/crc`; `GET /segments/{tableName}/{segmentName}/metadata`; `POST /segments/{tableNameWithType}/{segmentName}/reset`; `POST /segments/{tableNameWithType}/reset`; `POST /segments/{tableNameWithType}/uploadFromServerToDeepstore`; `DELETE /segments/{tableName}/{segmentName}`; `DELETE /segments/{tableName}`; `POST /segments/{tableName}/delete`; `DELETE /segments/{tableName}/choose`; `GET /segments/{tableName}/metadata`; `GET /segments/{tableNameWithType}/isStale`; `GET /segments/{tableName}/zkmetadata`; `GET /segments/{tableName}/invalidPartitionMetadata`; `GET /segments/{tableName}/tiers`; `GET /segments/{tableName}/{segmentName}/tiers`; `GET /segments/{tableName}/select`; `POST /segments/{tableNameWithType}/updateZKTimeInterval`; `DELETE /deleteSegmentsFromSequenceNum/{tableNameWithType}`.

- **PinotSegmentUploadDownloadRestletResource**: `GET /segments/{tableName}/{segmentName}`; `POST /segments`; `POST /segments`; `POST /segments/batchUpload`; `POST /v2/segments`; `POST /v2/segments`; `POST /segments/{tableName}/startReplaceSegments`; `POST /segments/{tableName}/endReplaceSegments`; `POST /segments/{tableName}/revertReplaceSegments`; `POST /segments/reingested`.

- **PinotTableReloadResource**: `POST /segments/{tableName}/{segmentName}/reload`; `POST /segments/{tableName}/reload`; `GET /segments/segmentReloadStatus/{jobId}`; `GET /segments/{tableNameWithType}/needReload`.

- **PinotTableRestletResource**: `POST /tables`; `POST /tables/{tableName}/copy`; `PUT /tables/recommender`; `GET /tables`; `GET /tables/{tableName}`; `DELETE /tables/{tableName}`; `PUT /tables/{tableName}`; `POST /tables/validate`; `POST /tables/{tableName}/rebalance`; `DELETE /tables/{tableName}/rebalance`; `GET /rebalanceStatus/{jobId}`; `GET /tables/{tableName}/state`; `PUT /tables/{tableName}/state`; `GET /tables/{tableName}/stats`; `GET /tables/{tableName}/status`; `GET /tables/{tableName}/metadata`; `GET /tables/{tableNameWithType}/aggregateMetadata`; `GET /tables/{tableName}/validDocIdsMetadata`; `GET /tables/{tableName}/indexes`; `GET /table/{tableName}/jobs`; `POST /tables/{tableName}/timeBoundary`; `DELETE /tables/{tableName}/timeBoundary`.

- **TableSize**: `GET /tables/{tableName}/size`.

- **TableViews**: `GET /tables/{tableName}/idealstate`; `GET /tables/{tableName}/externalview`; `GET /tables/{tableName}/segmentsStatus`; `GET /tables/{tableName}/badLLCSegmentsPerPartition`.

### query and realtime

- **LLCSegmentCompletionHandlers**: `GET /extendBuildTime`; `GET /segmentConsumed`; `GET /segmentStoppedConsuming`; `GET /segmentCommitStart`; `POST /segmentUpload`; `POST /segmentCommitEndWithMetadata`; `GET /segmentBuildDeterministicFailure`.

- **PinotQueryResource**: `POST /sql`; `GET /sql`; `GET /timeseries/api/v1/query_range`; `POST /query/timeseries`; `POST /validateMultiStageQuery`; `POST /query/tableNames`.

- **PinotRealtimeTableResource**: `POST /tables/{tableName}/pauseConsumption`; `POST /tables/{tableName}/pauseTopicConsumption`; `POST /tables/{tableName}/resumeConsumption`; `POST /tables/{tableName}/resumeTopicConsumption`; `POST /tables/{tableName}/forceCommit`; `GET /tables/forceCommitStatus/{jobId}`; `GET /tables/{tableName}/pauseStatus`; `GET /tables/{tableName}/consumingSegmentsInfo`; `GET /tables/{tableName}/pauselessDebugInfo`; `GET /tables/{tableName}/consumerWatermarks`.

- **PinotRunningQueryResource**: `DELETE /query/{brokerId}/{queryId}`; `DELETE /clientQuery/{brokerId}/{clientQueryId}`; `DELETE /clientQuery/{clientQueryId}`; `GET /queries`.

- **PinotUpsertRestletResource**: `POST /upsert/estimateHeapUsage`.

### minion and scheduled work

- **PinotControllerPeriodicTaskRestletResource**: `GET /periodictask/run`; `POST /periodictask/run`; `GET /periodictask/names`.

- **PinotMinionRestletResource**: `GET /minions/status`.

- **PinotTaskRestletResource**: `GET /tasks/tasktypes`; `GET /tasks/summary`; `GET /tasks/{taskType}/state`; `GET /tasks/{taskType}/tasks`; `GET /tasks/{taskType}/tasks/count`; `GET /tasks/{taskType}/{tableNameWithType}/state`; `GET /tasks/{taskType}/{tableNameWithType}/metadata`; `DELETE /tasks/{taskType}/{tableNameWithType}/metadata`; `GET /tasks/{taskType}/taskcounts`; `GET /tasks/{taskType}/debug`; `GET /tasks/{taskType}/{tableNameWithType}/debug`; `GET /tasks/generator/{tableNameWithType}/{taskType}/debug`; `GET /tasks/task/{taskName}/debug`; `GET /tasks/{taskType}/taskstates`; `GET /tasks/task/{taskName}/state`; `GET /tasks/subtask/{taskName}/state`; `GET /tasks/task/{taskName}/config`; `GET /tasks/task/{taskName}/runtime/config`; `GET /tasks/subtask/{taskName}/config`; `GET /tasks/subtask/{taskName}/progress`; `GET /tasks/subtask/workers/progress`; `GET /tasks/scheduler/information`; `GET /tasks/scheduler/jobKeys`; `GET /tasks/scheduler/jobDetails`; `POST /tasks/schedule`; `POST /tasks/execute`; `PUT /tasks/{taskType}/cleanup`; `PUT /tasks/{taskType}/stop`; `PUT /tasks/{taskType}/resume`; `DELETE /tasks/{taskType}`; `DELETE /tasks/task/{taskName}`; `DELETE /tasks/lock/forceRelease`.

### local process and shell

- **LandingPageHandler**: `GET /`.

- **PinotControllerAppConfigs**: `GET /appconfigs`.

- **PinotControllerAuthResource**: `GET /auth/verify`; `GET /auth/verify/v2`; `GET /auth/info`.

- **PinotControllerHealthCheck**: `GET /pinot-controller/admin`; `GET /health`; `GET /uptime`; `GET /invalidconfigs`; `GET /start-time`.

- **PinotControllerLogger**: `GET /loggers`; `GET /loggers/{loggerName}`; `PUT /loggers/{loggerName}`; `GET /loggers/files`; `GET /loggers/download`; `GET /loggers/instances`; `GET /loggers/instances/{instanceName}`; `GET /loggers/instances/{instanceName}/download`.

- **PinotControllerTimeseriesResource**: `GET /timeseries/languages`.

- **PinotVersionRestletResource**: `GET /version`.

### ZooKeeper-specific

- **ZookeeperResource**: `GET /zk/get`; `DELETE /zk/delete`; `PUT /zk/putChildren`; `PUT /zk/put`; `POST /zk/create`; `GET /zk/ls`; `GET /zk/getChildren`; `GET /zk/lsl`; `GET /zk/stat`.
