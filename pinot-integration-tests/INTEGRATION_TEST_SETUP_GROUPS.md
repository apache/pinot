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
# Pinot Integration Test Setup Groups

This inventory groups the `pinot-integration-tests` TestNG tests by the infrastructure
they start today. The goal is to make it clear which classes can be moved behind a
single suite-level infrastructure setup and which classes need a dedicated setup
because they override process configuration, start alternate components, use Docker,
or intentionally restart services.

Current CI wiring is alphabetical for most tests (`integration-tests-set-1` and
`integration-tests-set-2`) plus one shared TestNG suite for
`org.apache.pinot.integration.tests.custom`. Alphabetical execution does not align
with infrastructure compatibility, so most classes still start and tear down their
own clusters.

## Already Suite-Shared

`CustomDataQueryClusterIntegrationTest` is the existing model for one infrastructure
setup per suite:

- `@BeforeSuite`: starts ZK, Kafka, controller, broker, server, and minion once.
- `@BeforeClass`: creates the class-specific table/data.
- `@AfterClass`: drops the class-specific table/data.
- `@AfterSuite`: tears down the shared infrastructure once.

Classes currently covered by `custom-cluster-integration-test-suite.xml`:

- `AggregateMetricsTest`
- `ArithmeticFunctionsIntegrationTest`
- `ArrayTest`
- `BitwiseFunctionsIntegrationTest`
- `BytesTypeTest`
- `CLPEncodingRealtimeTest`
- `CpcSketchTest`
- `DistinctQueriesTest`
- `FloatingPointDataTypeTest`
- `FunnelCountTest`
- `GeoSpatialTest`
- `GroupByOptionsTest`
- `GroupByTrimmingTest`
- `IvfFlatVectorTest`
- `IvfPqVectorRealtimeTest`
- `IvfPqVectorTest`
- `JsonPathTest`
- `MapFieldTypeMixedValueIngestingIntegrationTest`
- `MapFieldTypeRealtimeTest`
- `MapFieldTypeTest`
- `MapTypeTest`
- `MultiColumnRealtimeColMajorTextIndicesTest`
- `MultiColumnRealtimeRowMajorTextIndicesTest`
- `MultiColumnTextIndicesTest`
- `MultiTopicRealtimeClusterIntegrationTest`
- `OfflineUpsertTableTest`
- `ProtoBufCodeGenMessageDecoderTest`
- `RefreshSegmentMinionTest`
- `RowExpressionTest`
- `SSBQueryTest`
- `StarTreeTest`
- `SumPrecisionTest`
- `TableSamplerIntegrationTest`
- `TextIndicesRealtimeTest`
- `TextIndicesTest`
- `ThetaSketchTest`
- `TimestampTest`
- `TupleSketchTest`
- `ULLTest`
- `UnnestIntegrationTest`
- `VectorTest`
- `WindowFunnelTest`

`BigNumberOfSegmentsTest` is in the same package but disabled.

## Setup Signature Matrix

Legend:

- `C/B/S/M` means Pinot controllers, Pinot brokers, Pinot servers, and minions
  started as the baseline test setup. Most rows also start one ZK; multi-cluster
  rows start one ZK per cluster.
- `Kafka` means an embedded Kafka cluster is part of setup. Exact Kafka broker
  count comes from `getNumKafkaBrokers()` and can still matter for a final suite.
- `Overrides` lists process-level setup differences: `override*Conf()`, custom
  `create*Starter()`, Swagger, fake servers, schema registry, Docker/Kinesis, or
  tests that add/restart participants.
- Some tests also mutate Helix cluster/table config after the cluster is running.
  Those are not counted as process config overrides, but shared suites still need
  to either make those mutations part of suite setup or reset them per class.
- Rows with the same `C/B/S/M`, same external infra, and no overrides are the
  highest-confidence candidates for one shared suite run.
- Rows with overrides are still useful buckets, but they should share only when
  the actual override values are intentionally compatible.

## Component Config Override Summary

This table groups runnable integration test classes only by inherited component
config override methods:

- `overrideControllerConf()`
- `overrideBrokerConf()`
- `overrideServerConf()`
- `overrideMinionConf()`

Subclasses inherit the group of their base class. For example, subclasses of
`BaseRealtimeClusterIntegrationTest` count as `server` because that base overrides
server config.

| Component config override group | Runnable test classes |
| --- | ---: |
| none | 88 |
| broker | 11 |
| server | 8 |
| controller | 3 |
| broker + server | 12 |
| controller + broker | 1 |
| controller + server | 14 |
| controller + broker + server | 9 |
| controller + broker + server + minion | 3 |
| total | 149 |

## Refactor Plan For No-Override Tests

The 88 tests without inherited component config overrides are the best place to
start. A single rich shared environment should cover most of them:

- 1 ZK
- 1 controller
- 1 broker
- 2 servers
- 1 minion
- embedded Kafka started once, preferably without creating a default topic

This should be treated as a superset environment, not as proof that every test can
immediately run unchanged. Extra servers/minions are usually harmless at the
process level, but they can change routing, assignment, rebalance summaries,
metrics, and `numServersQueried` assertions.

### Target Coverage

Likely target for the first shared-rich-cluster suite:

- 42 `custom/*` tests already use the same broad topology and are suite-shared.
- About 32-33 additional no-override tests should be reasonable first migration
  targets after table/topic/tenant cleanup.
- That puts the practical first target around 74-75 of the 88 no-override tests.

Keep these no-override tests out of the first shared-rich-cluster pass:

- `ControllerLeaderLocatorIntegrationTest`, `ServerStarterIntegrationTest`: controller-only tests with method-local
  component starts.
- `CancelQueryIntegrationTests`: requires 4 servers.
- `PartialUpsertTableRebalanceIntegrationTest`, `KafkaPartitionSubsetChaosIntegrationTest`,
  `UpsertTableSegmentUploadIntegrationTest`: add, remove, or restart servers during methods.
- `SegmentCompletionIntegrationTest`: uses a fake Helix server participant.
- `KinesisShardChangeTest`, `RealtimeKinesisIntegrationTest`: Docker LocalStack/Kinesis.
- `MultiClusterIntegrationTest`, `SameTableNameMultiClusterIntegrationTest`: two isolated clusters plus extra broker.
- `UdfTest`: manual UDF cluster with known non-daemon thread caveat.
- `AdminConsoleIntegrationTest`: can join only if the shared controller enables Swagger for the whole suite.

### Current Draft Suite Timing

The current draft suite moves twenty-three low-risk no-override source test classes
behind the shared rich cluster. `ErrorCodesIntegrationTest` is represented by
its four concrete inner TestNG classes:

- `SegmentUploadIntegrationTest`
- `IngestionConfigHybridIntegrationTest`
- `TPCHQueryIntegrationTest`
- `BaseDedupIntegrationTest`
- `StaleSegmentCheckIntegrationTest`
- `SegmentWriterUploaderIntegrationTest`
- `SegmentGenerationMinionClusterIntegrationTest`
- `DimensionTableIntegrationTest`
- `SparkSegmentMetadataPushIntegrationTest`
- `StarTreeFunctionParametersIntegrationTest`
- `SegmentPartitionLLCRealtimeClusterIntegrationTest`
- `ErrorCodesIntegrationTest$MultiStageBrokerTestCase`
- `ErrorCodesIntegrationTest$SingleStageBrokerTestCase`
- `ErrorCodesIntegrationTest$MultiStageControllerTestCase`
- `ErrorCodesIntegrationTest$SingleStageControllerTestCase`
- `LogicalTableWithOneOfflineTableIntegrationTest`
- `LogicalTableWithTwoOfflineTablesIntegrationTest`
- `LogicalTableWithTwelveOfflineTablesIntegrationTest`
- `LogicalTableWithOneRealtimeTableIntegrationTest`
- `LogicalTableWithOneOfflineOneRealtimeTableIntegrationTest`
- `LogicalTableWithTwoRealtimeTableIntegrationTest`
- `LogicalTableWithTwoOfflineOneRealtimeTableIntegrationTest`
- `LogicalTableWithTwelveOfflineOneRealtimeTableIntegrationTest`
- `PurgeMetadataPushMinionClusterIntegrationTest`
- `RealtimeToOfflineSegmentsMinionClusterIntegrationTest`
- `QueryQuotaClusterIntegrationTest`

On this workstation, the same 202 TestNG tests passed in both modes:

| Mode | Command | Wall time |
| --- | --- | ---: |
| Per-class lifecycle | `./mvnw -pl pinot-integration-tests -Dtest=SegmentUploadIntegrationTest,IngestionConfigHybridIntegrationTest,TPCHQueryIntegrationTest,BaseDedupIntegrationTest,StaleSegmentCheckIntegrationTest,SegmentWriterUploaderIntegrationTest,SegmentGenerationMinionClusterIntegrationTest,DimensionTableIntegrationTest,SparkSegmentMetadataPushIntegrationTest,StarTreeFunctionParametersIntegrationTest,SegmentPartitionLLCRealtimeClusterIntegrationTest,ErrorCodesIntegrationTest,LogicalTableWithOneOfflineTableIntegrationTest,PurgeMetadataPushMinionClusterIntegrationTest,RealtimeToOfflineSegmentsMinionClusterIntegrationTest,QueryQuotaClusterIntegrationTest,LogicalTableWithTwoOfflineTablesIntegrationTest,LogicalTableWithTwelveOfflineTablesIntegrationTest,LogicalTableWithOneRealtimeTableIntegrationTest,LogicalTableWithOneOfflineOneRealtimeTableIntegrationTest,LogicalTableWithTwoRealtimeTableIntegrationTest,LogicalTableWithTwoOfflineOneRealtimeTableIntegrationTest,LogicalTableWithTwelveOfflineOneRealtimeTableIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test` | 780.87s |
| Shared rich suite | `./mvnw -pl pinot-integration-tests -Pshared-rich-cluster-integration-test-suite test` | 501.50s |

That is a 279.37s wall-clock reduction, about 36% for this draft batch.

Previous validated checkpoint: 155 TestNG tests passed with a 597.19s per-class
lifecycle and a 385.20s shared-rich-suite run, a 211.99s wall-clock reduction
or about 35%.

Attempted but not included yet:

- `PauselessRealtimeIngestionWithDedupIntegrationTest`: intermittently hit unavailable realtime segments under
  strict replica-group routing in the shared run.
- `KafkaPartitionSubsetChaosIntegrationTest`: uses its own chaos topology with a control realtime table, three subset
  realtime tables, six Kafka partitions, pause/resume, force-commit, and server restart coverage; keep it in a
  separate setup bucket.
- `SegmentGenerationMinionRealtimeIngestionTest`: uses `@BeforeTest` and creates `mytable_REALTIME` before earlier
  suite classes; it needs class-scoped setup or unique table/topic names before sharing.
- `CommitTimeCompactionIntegrationTest`: needs a complete conversion to unique per-method table/topic names and
  cleanup; the first worker pass was intentionally parked before wiring it into the suite.
- `PurgeMinionClusterIntegrationTest`: a shared-mode patch compiled, but `testRealtimeLastSegmentPreservation`
  timed out waiting for purged realtime records; it needs deeper realtime purge/task-state isolation.
- `UpsertCompactMergeTaskIntegrationTest`: a shared-mode patch compiled, but the task generator skipped segments
  with empty download URLs and no task names were scheduled; segment download URL generation needs a targeted fix.

### Suite Infrastructure

Create a shared base similar to `CustomDataQueryClusterIntegrationTest`, but make
it reusable by the non-custom no-override tests:

1. Add a shared suite holder that starts the rich environment in `@BeforeSuite`.
2. Start Kafka with no default topic; classes create only the topics they need.
3. Start 2 Pinot servers and 1 minion even for tests that do not need minion.
4. Expose shared controller, broker, server, minion, Kafka, Helix, and admin-client state through delegation methods.
5. Tear everything down once in `@AfterSuite`.

### Tenant Isolation

Use tenants to make a 2-server physical cluster behave like either a 1-server or
2-server logical test cluster:

1. Tag server 0 with a one-server tenant, e.g. `SharedOneServerTenant`.
2. Tag both servers with a two-server tenant, e.g. `SharedTwoServerTenant`.
3. Default migrated tests to the one-server tenant unless they currently require 2 servers.
4. Map current 2-server tests to the two-server tenant.
5. Keep the broker tenant shared; one broker is enough for these candidates.

This avoids many failures where a formerly 1-server test observes 2 servers in
routing or assignment.

### Per-Class Isolation

Keep data setup class-scoped even though infrastructure is suite-scoped:

1. `@BeforeClass`: create schema, table config, Kafka topic, segments, H2 data, and query generator.
2. `@AfterClass`: drop offline/realtime/logical tables, delete schemas, clear task metadata where needed, and delete or
   uniquify Kafka topics.
3. Wait for ExternalView, IdealState, routing, and table-data-manager cleanup before the next class starts.
4. Run these shared-suite tests sequentially; do not enable TestNG class parallelism.

Reuse `mytable` only if teardown fully waits for cleanup. Otherwise add a table
name indirection layer and update hard-coded query text/query-file handling.

### Migration Order

1. **Extract shared infrastructure** from `CustomDataQueryClusterIntegrationTest`
   into a reusable helper/base.
2. **Move the existing custom suite** onto that helper without changing behavior.
3. **Add non-custom 2-server Kafka tests**: `BaseDedupIntegrationTest`,
   `CommitTimeCompactionIntegrationTest`, logical-table tests, and
   `PauselessRealtimeIngestionWithDedupIntegrationTest`.
4. **Add minion Kafka tests** that do not mutate process config:
   merge/rollup, purge, realtime-to-offline, segment-generation realtime, stale
   segment check, upsert compact merge, and upsert table.
5. **Add one-server offline/realtime tests** using the one-server tenant mapping.
6. **Evaluate Swagger once** and either include `AdminConsoleIntegrationTest` by
   enabling Swagger suite-wide or keep it dedicated.
7. **Handle mutable-participant tests last** with explicit baseline restore
   checks after each class.

### Acceptance Criteria

Before moving a test into the shared-rich-cluster suite, verify:

- It has no inherited component config override.
- It does not require a fake server, LocalStack/Kinesis, schema registry, UDF cluster, or multi-cluster broker.
- It does not depend on exact physical server count unless tenant isolation preserves that expectation.
- It cleans up all tables, schemas, logical tables, Kafka topics, and task metadata it creates.
- Running it before and after another migrated class gives the same result.

### Subagent Migration Assessment

Read-only subagents inspected the no-component-config-override tests against the
shared-rich-cluster target. They did not edit files or run the full shared suite.

#### Try First

These are the lowest-risk classes to place behind suite-aware lifecycle first:

- `SegmentUploadIntegrationTest`
- `TPCHQueryIntegrationTest`
- `IngestionConfigHybridIntegrationTest`
- `LogicalTableWithOneOfflineTableIntegrationTest`
- `LogicalTableWithTwoOfflineTablesIntegrationTest`
- `LogicalTableWithTwelveOfflineTablesIntegrationTest`
- `LogicalTableWithOneRealtimeTableIntegrationTest`
- `LogicalTableWithOneOfflineOneRealtimeTableIntegrationTest`
- `LogicalTableWithTwelveOfflineOneRealtimeTableIntegrationTest`

The custom query tests are already suite-shared and match the target topology.
They should stay as the control group while the non-custom suite is introduced.

#### Patch Cleanup Or Isolation First

These are plausible shared-rich-cluster candidates, but need table/topic/task
cleanup, unique names, sequential execution, or reset hooks before moving:

- `BaseDedupIntegrationTest`: also drop `DedupTableWithReplicas`.
- `PauselessRealtimeIngestionWithDedupIntegrationTest`: same cleanup as dedup.
- `CommitTimeCompactionIntegrationTest`: exact segment/count assertions and temporary cluster config mutation.
- `SegmentPartitionLLCRealtimeClusterIntegrationTest`: hard-coded `mytable` queries and exact segment counts.
- `DimensionTableIntegrationTest`: fixed `mytable`; deletes table during a test method.
- `MultiStageEngineIntegrationTest`: multiple fixed table names and cluster-config toggles.
- `QueryQuotaClusterIntegrationTest`: quota mutations need strict reset and isolation.
- `SegmentWriterUploaderIntegrationTest`: fixed `mytable`; exact segment-count assertions.
- `SparkSegmentMetadataPushIntegrationTest`: fixed `_testTable = "mytable"`.
- `StarTreeFunctionParametersIntegrationTest`: fixed `mytable`, table reloads, cluster config mutation.
- `LogicalTableWithTwoOfflineOneRealtimeTableIntegrationTest`: stateful logical-table time-boundary mutation.
- `LogicalTableWithTwoRealtimeTableIntegrationTest`: fixed Kafka topic and per-instance counters.
- `SegmentGenerationMinionClusterIntegrationTest`: drop all tables it creates.
- `SimpleMinionClusterIntegrationTest`: global minion task state and cluster task config.
- `MergeRollupMinionClusterIntegrationTest`: global task queues and generic table/topic names.
- `PurgeMetadataPushMinionClusterIntegrationTest`: inherited setup creates extra tables/task state.
- `PurgeMinionClusterIntegrationTest`: global `MinionContext` purger and generic table names.
- `RealtimeToOfflineSegmentsMinionClusterIntegrationTest`: currently leaves metadata table cleanup work.
- `SegmentGenerationMinionRealtimeIngestionTest`: `@BeforeTest/@AfterTest`, fixed realtime table.
- `StaleSegmentCheckIntegrationTest`: simple candidate, but currently does not drop table/schema.
- `UpsertCompactMergeTaskIntegrationTest`: fixed table names and Kafka topic isolation.
- `MultiTopicRealtimeClusterIntegrationTest`: fixed topics and table; no topic deletion.
- `MultiColumnTextIndicesTest`, `MultiColumnRealtimeRowMajorTextIndicesTest`,
  `MultiColumnRealtimeColMajorTextIndicesTest`, `TextIndicesRealtimeTest`: table config mutations and reloads;
  keep sequential.
- `SSBQueryTest`: generic table names `customer`, `dates`, `lineorder`, `part`, `supplier`.
- `TimestampTest`: changes JVM default timezone; keep sequential.

#### Keep Dedicated Initially

These should not be moved into the first shared-rich-cluster suite:

- `ControllerLeaderLocatorIntegrationTest`: controller-only flow, starts a second controller inside the method.
- `ServerStarterIntegrationTest`: controller-only flow, starts/stops short-lived servers inside methods.
- `AdminConsoleIntegrationTest`: requires Swagger controller unless Swagger is enabled suite-wide.
- `HelixZNodeSizeLimitTest`: changes ZK/client buffer assumptions and writes very large IdealState data.
- `OfflineClusterIntegrationTest`: destructive instance decommission and exact private-cluster assertions.
- `CancelQueryIntegrationTests`: requires 4 servers.
- `PartialUpsertTableRebalanceIntegrationTest`: starts from 1 server and adds/stops servers with exact assertions.
- `KafkaPartitionSubsetChaosIntegrationTest`: restarts shared servers and has fixed topic/table state.
- `SegmentCompletionIntegrationTest`: fake Helix server participant, no real Pinot server.
- `KinesisShardChangeTest`, `RealtimeKinesisIntegrationTest`: Docker LocalStack/Kinesis.
- `MultiClusterIntegrationTest`, `SameTableNameMultiClusterIntegrationTest`: two isolated clusters plus extra broker.
- `UdfTest`: manual UDF cluster with non-daemon thread caveat.
- `RefreshSegmentMinionTest`: global `RefreshSegmentTask` queues/state.
- `UpsertTableIntegrationTest`: custom extra server/metadata-manager test and exact routing counts.

### No Process Config Overrides

These are the best first candidates for shared suite-level infrastructure. The
main cleanup work is table/schema/topic isolation plus resetting any Helix config
the class changes at runtime.

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=0 S=0 M=0` | none | `ControllerLeaderLocatorIntegrationTest` *(starts a second controller inside the method)*, `ServerStarterIntegrationTest` *(starts/stops short-lived servers inside methods)* |
| `C=1 B=1 S=1 M=0` | none | `DimensionTableIntegrationTest`, `HelixZNodeSizeLimitTest`, `MultiStageEngineIntegrationTest`, `OfflineClusterIntegrationTest`, `QueryQuotaClusterIntegrationTest`, `SegmentUploadIntegrationTest`, `SegmentWriterUploaderIntegrationTest`, `SparkSegmentMetadataPushIntegrationTest`, `StarTreeFunctionParametersIntegrationTest`, `TPCHQueryIntegrationTest` |
| `C=1 B=1 S=4 M=0` | none | `CancelQueryIntegrationTests` |
| `C=1 B=1 S=1 M=0` | Kafka | `IngestionConfigHybridIntegrationTest`, `PartialUpsertTableRebalanceIntegrationTest` *(adds temporary servers during methods)*, `SegmentPartitionLLCRealtimeClusterIntegrationTest` |
| `C=1 B=1 S=2 M=0` | Kafka | `BaseDedupIntegrationTest`, `CommitTimeCompactionIntegrationTest`, `LogicalTableWithOneOfflineOneRealtimeTableIntegrationTest`, `LogicalTableWithOneOfflineTableIntegrationTest`, `LogicalTableWithOneRealtimeTableIntegrationTest`, `LogicalTableWithTwelveOfflineOneRealtimeTableIntegrationTest`, `LogicalTableWithTwelveOfflineTablesIntegrationTest`, `LogicalTableWithTwoOfflineOneRealtimeTableIntegrationTest`, `LogicalTableWithTwoOfflineTablesIntegrationTest`, `LogicalTableWithTwoRealtimeTableIntegrationTest`, `PauselessRealtimeIngestionWithDedupIntegrationTest` |
| `C=1 B=1 S=1 M=1` | none | `SegmentGenerationMinionClusterIntegrationTest`, `SimpleMinionClusterIntegrationTest` |
| `C=1 B=1 S=1 M=1` | Kafka | `MergeRollupMinionClusterIntegrationTest`, `PurgeMetadataPushMinionClusterIntegrationTest`, `PurgeMinionClusterIntegrationTest`, `RealtimeToOfflineSegmentsMinionClusterIntegrationTest`, `SegmentGenerationMinionRealtimeIngestionTest`, `StaleSegmentCheckIntegrationTest`, `UpsertCompactMergeTaskIntegrationTest` |
| `C=1 B=1 S=2 M=1` | Kafka | `UpsertTableIntegrationTest` |
| `C=1 B=1 S=2 M=1` | Kafka | `custom/*` tests listed above; already suite-shared |

### Broker Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=0 M=0` | none | `BrokerServiceDiscoveryIntegrationTest` |
| `C=1 B=1 S=1 M=0` | none | `CursorCronCleanupIntegrationTest`, `CursorFsIntegrationTest`, `CursorIntegrationTest`, `EmptyResponseIntegrationTest` |
| `C=1 B=1 S=2 M=0` | none | `MultiStageEngineExplainIntegrationTest`, `OfflineTimestampIndexIntegrationTest`, `QueryThreadContextIntegrationTest`, `SpoolIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `BrokerQueryLimitTest`, `NullHandlingIntegrationTest` |

### Server Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | none | `MultiStageWithoutStatsIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `ExactlyOnceKafkaRealtimeClusterIntegrationTest` *(transactional Kafka)*, `KafkaConsumingSegmentToBeMovedSummaryIntegrationTest` *(adds a server during the method)*, `KafkaIncreaseDecreasePartitionsIntegrationTest`, `RealtimeConsumptionRateLimiterClusterIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka + schema registry | `KafkaConfluentSchemaRegistryAvroMessageDecoderRealtimeClusterIntegrationTest` |

### Broker And Server Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | none | `CpuBasedBrokerQueryKillingIntegrationTest`, `CpuBasedServerQueryKillingIntegrationTest`, `JmxMetricsIntegrationTest`, `MemoryBasedServerQueryKillingIntegrationTest`, `OfflineGRPCServerIntegrationTest`, `OfflineGRPCServerMultiStageIntegrationTest`, `OfflineSecureGRPCServerIntegrationTest`, `WindowResourceAccountingTest` |
| `C=1 B=1 S=2 M=0` | none | `GroupByEnableTrimOptionIntegrationTest` |
| `C=1 B=1 S=4 M=0` | none | `MultiStageEngineSmallBufferTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `QueryWorkloadIntegrationTest` |
| `C=1 B=2 S=3 M=0` | none | `MultiNodesOfflineClusterIntegrationTest` *(also adds/stops a broker and restarts a server inside methods)* |

### Controller Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | none | `MultiStageEngineCustomTenantIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `PinotLLCRealtimeSegmentManagerIntegrationTest` |
| `C=1 B=1 S=4 M=0` | Kafka | `ControllerPeriodicTasksIntegrationTest` |

### Controller And Server Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | Kafka | `LLCRealtimeClusterIntegrationTest`, `LLCRealtimeKafka3ClusterIntegrationTest`, `LLCRealtimeKafka4ClusterIntegrationTest`, `RetentionManagerIntegrationTest` |
| `C=1 B=1 S=2 M=0` | Kafka | `PeerDownloadLLCRealtimeClusterIntegrationTest` |

### Controller, Broker, And Server Config Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | none | `ControllerServiceDiscoveryIntegrationTest`, `CursorWithAuthIntegrationTest`, `TimeSeriesAuthIntegrationTest`, `TimeSeriesIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `RowLevelSecurityIntegrationTest` |
| `C=1 B=1 S=2 M=0` | Kafka | `DateTimeFieldSpecHybridClusterIntegrationTest`, `GrpcBrokerClusterIntegrationTest`, `HybridClusterIntegrationTest`, `TableRebalanceIntegrationTest` *(adds/stops servers during methods)*, `TenantRebalanceIntegrationTest` |

### Controller Starter And Failure Injection Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | Kafka | `PauselessRealtimeIngestionCommitEndMetadataFailureTest`, `PauselessRealtimeIngestionConsumingTransitionFailureTest`, `PauselessRealtimeIngestionIdealStateUpdateFailureTest`, `PauselessRealtimeIngestionIntegrationTest`, `PauselessRealtimeIngestionNewSegmentMetadataCreationFailureTest`, `PauselessRealtimeIngestionSegmentCommitFailureTest`, `TableRebalancePauselessIntegrationTest` |
| `C=1 B=1 S=2 M=0` | Kafka | `PauselessDedupRealtimeIngestionConsumingTransitionFailureTest`, `PauselessDedupRealtimeIngestionSegmentCommitFailureTest` |

### Minion Config Or Auth/TLS Overrides

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=1` | none | `AdminConsoleIntegrationTest` *(Swagger controller)*, `BasicAuthBatchIntegrationTest` *(controller/broker/server/minion auth overrides)* |
| `C=1 B=1 S=1 M=1` | Kafka | `TlsIntegrationTest`, `UrlAuthRealtimeIntegrationTest` |

### Restart Or Mutable-Participant Suites

These can still be grouped by baseline setup, but only if each class restores the
baseline before the next class runs.

| Baseline setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=1 M=0` | Kafka | `KafkaPartitionSubsetChaosIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Kafka | `DedupPreloadIntegrationTest`, `UpsertTableSegmentPreloadIntegrationTest` *(server config override plus restart)* |
| `C=1 B=1 S=2 M=0` | Kafka | `UpsertTableSegmentUploadIntegrationTest` |

### Special Infrastructure

| Setup | External infra | Classes |
| --- | --- | --- |
| `C=1 B=1 S=fake M=0` | Kafka | `SegmentCompletionIntegrationTest` |
| `C=1 B=1 S=1 M=0` | Docker LocalStack/Kinesis | `KinesisShardChangeTest`, `RealtimeKinesisIntegrationTest` |
| `C=1 B=1 S=1 M=1` | manual UDF cluster | `UdfTest` |
| `C=2 B=3 S=2 M=0` | two ZK-backed clusters | `MultiClusterIntegrationTest`, `SameTableNameMultiClusterIntegrationTest` |

## Must Stay Dedicated Initially

These tests have setup behavior that is too different to share with the standard
single-cluster suites without a deeper refactor:

- `KinesisShardChangeTest`: starts Docker-backed LocalStack/Kinesis.
- `RealtimeKinesisIntegrationTest`: starts Docker-backed LocalStack/Kinesis.
- `MultiClusterIntegrationTest`: starts two isolated Pinot clusters plus an extra broker.
- `SameTableNameMultiClusterIntegrationTest`: extends the multi-cluster setup.
- `UdfTest`: starts `IntegrationUdfTestCluster` manually and notes leaked non-daemon threads.
- `ChaosMonkeyIntegrationTest`: disabled test methods and external process management.
- `TPCHGeneratedQueryIntegrationTest`: generated-query test method is disabled.

## Implementation Notes

To get one setup/teardown per compatible group:

1. Introduce suite-level base classes per topology, using the custom test suite pattern.
2. Move infrastructure startup from `@BeforeClass` to `@BeforeSuite` for each group.
3. Keep schema/table/topic/segment setup in `@BeforeClass` and drop table-specific state in `@AfterClass`.
4. Make class-specific table names unique where a group would otherwise reuse `mytable`.
5. Split TestNG XML by these groups instead of the current alphabetical Maven profiles.
6. Keep tests with config overrides, custom starters, restarts, TLS/auth, Kinesis, UDF, and multi-cluster in dedicated suites until each has an explicit shared-infra contract.
