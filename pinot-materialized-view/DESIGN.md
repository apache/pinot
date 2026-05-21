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

# Pinot Materialized Views — Design

This document covers the design of the materialized-view (MV) subsystem and its
intended extension points.  It is meant for contributors who are about to extend
this module — particularly for the **fixed-partition MV** work that follows the
initial time-windowed MV implementation.

## 1. Today's design (PR 1 + PR 2)

PR 1 ships **time-windowed MVs only**.  PR 2 adds broker query rewrite on top of
the same partition model.  The runtime contract is:

- **Definition** (`MaterializedViewDefinitionMetadata`, persisted under
  `/CONFIGS/MATERIALIZED_VIEW/DEFINITION/<viewTableNameWithType>`):
    user-supplied SQL, source-table reference, source partition expressions,
    `rewriteEnabled`, `stalenessThresholdMs`.  Immutable post-create except for
    the rewrite flag and SLO knobs.
- **Runtime** (`MaterializedViewRuntimeMetadata`, persisted under
  `/CONFIGS/MATERIALIZED_VIEW/RUNTIME/<viewTableNameWithType>`):
    `watermarkMs` plus a `Map<Long, PartitionInfo>` keyed by `bucketStartMs`.
    Updated by the minion executor on task completion and by the controller-side
    `MaterializedViewConsistencyManager` on base-table segment changes.
- **Partitioning**: uniform-width time buckets defined by `bucketTimePeriod` on
  the MV's task config.  The MV's designated time column must be either an
  identity passthrough of the base time column or a `DATETRUNC` whose unit
  matches the bucket width — `TimeExprValidator` enforces both at create time.
- **STALE marking**: range-based.  Controller calls
  `MaterializedViewConsistencyManager.onBaseTableDataChange(table, startMs, endMs)`,
  the manager debounces (5 s) and then maps the range to overlapping buckets
  via floor-div arithmetic.
- **Task selection**: priority-ordered.  STALE partitions are OVERWRITE'd first
  (after fingerprint reconfirmation), then APPEND advances the watermark one
  bucket at a time, batched up to `maxTasksPerBatch`.

Strengths of this model: small constant memory per MV, deterministic bucket
arithmetic, naturally compatible with Pinot's existing time-partitioned segment
layout, and a single immutable-coverage assumption that closes the silent-stale
correctness hole at create time (upsert / dedup / dim / REFRESH base tables are
rejected by the analyzer).

Limitations of this model — all of which the **fixed-partition** extension
addresses:

- Real workloads sometimes want a categorical partition key (per-tenant
  rollups, per-country aggregates) rather than a time window.
- Some MVs want a single-row full-table aggregate — no partition key at all,
  just "refresh every N minutes."
- Range-based STALE marking is useless when the partition key is not derived
  from a base column the controller can read from segment metadata.

## 2. The fixed-partition extension

### 2.1 Two shapes to support

| Shape         | Description                                                  | Example                                                            |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------------ |
| `TIME`        | (today) Time-windowed buckets keyed by `bucketStartMs`.      | Per-day-per-carrier flight rollups.                                |
| `CATEGORICAL` | N fixed buckets keyed by a declared partition column value.  | Per-tenant rollups; one bucket per `tenant_id`.                    |
| `SINGLETON`   | Exactly one bucket; the MV is a full-table aggregate.        | Top-K customers across the entire warehouse, refreshed every 10 m. |

`SINGLETON` is a degenerate case of `CATEGORICAL` with one fixed key.  Treat
them as one extension point.

### 2.2 Where the boundary is

Subsystems that **do not change** (the interfaces here are already
partition-shape neutral; the docstrings now flag this explicitly):

- `MaterializedViewQueryExecutor` / `GrpcMaterializedViewQueryExecutor`: runs
  whatever SQL the scheduler hands it.  No knowledge of partition shape.
- `MaterializedViewTaskGeneratorContext`: pure metadata + ZK lookup.
- `PartitionFingerprint` + `PartitionState`: per-partition state machine works
  for any key type.

Subsystems that **land in PR 2** (not present in PR 1):

- `AggregationEquivalenceRegistry` + the equivalence rules
  (`SUM`/`COUNT`/`MIN`/`MAX`/`HLL`/`THETASKETCH`): re-aggregation is
  associative-commutative and partition-shape neutral, but the runtime that
  applies these rules is the broker rewrite engine (PR 2).  PR 1's analyzer
  holds a small `SUPPORTED_MATERIALIZED_VIEW_AGGREGATIONS` constant set with
  the same function names so create-time validation matches what PR 2's
  rewrite will support; the two must stay in sync.

Subsystems that **change**, ordered from least to most invasive:

1. **Definition schema**.  Add a `partitionKind: TIME | CATEGORICAL | SINGLETON`
   discriminator on `MaterializedViewDefinitionMetadata`.  `TIME` is the implicit
   default for back-compat on existing definitions.  For `CATEGORICAL`, also
   persist the partition column name and (optionally) a frozen list of expected
   partition keys — the freeze is required so the analyzer can validate the
   set at create time and the scheduler can enumerate refresh candidates.
2. **Analyzer**.  `MaterializedViewAnalyzer` dispatches on `partitionKind`:
    - `TIME` (today): require time column + `bucketTimePeriod`; reject
      mutable source types.
    - `CATEGORICAL`: require a partition column that exists in both the
      source schema and the MV schema, with the same type; reject if the
      source is not partitioned on the same column (we still allow it, but
      record a "scatter-gather refresh" mode that requires the executor to
      issue a `WHERE partitionColumn = ?` query per partition).
    - `SINGLETON`: no partition column; one task per refresh interval.
3. **Runtime metadata**.  `MaterializedViewRuntimeMetadata._partitions` changes
   from `Map<Long, PartitionInfo>` to `Map<String, PartitionInfo>` (the on-disk
   wire format is **already** string-keyed; this is an in-memory refactor only,
   no on-disk schema change).  Introduce a `PartitionKey` value type that wraps
   either a `Long bucketStartMs` (TIME) or a `String categoricalKey`
   (CATEGORICAL / SINGLETON) — keeps call sites typed.  `watermarkMs` becomes
   optional, populated only for `TIME` MVs.  `MaterializedViewRuntimeMetadata`
   gets a `PartitionKind` field so readers know how to interpret the keys.
4. **Consistency manager**.  Dispatch on `partitionKind`:
    - `TIME`: today's range-to-bucket sweep.
    - `CATEGORICAL` / `SINGLETON`: the controller does not know which
      categorical partition a base segment touched, so any base change
      routes to `onBaseTableFullInvalidation`.  Implementation: mark every
      partition in the runtime znode STALE.  The debounce + retry logic
      already in place applies unchanged.

    The public entry point `onBaseTableFullInvalidation(rawTableName)` exists
    today (PR 1 shipped it for the time-MV "unknown range" case); fixed-MV
    callers route through the same method, so the controller-side notification
    code does not need to grow new branches when fixed-MV lands.
5. **Scheduler**.  Two selection strategies behind a partition-kind dispatch:
    - `TIME` (today): watermark-based APPEND + STALE OVERWRITE prioritization.
    - `CATEGORICAL` / `SINGLETON`: FIFO over STALE partitions by
      `lastRefreshTime`, plus a "force refresh every N minutes regardless of
      STALE" knob driven by `stalenessThresholdMs`.  No watermark, no time
      arithmetic, no buffer period.  APPEND is meaningless for these shapes.

    The SQL the scheduler emits for a CATEGORICAL refresh is:
    `<definedSQL> WHERE <partitionColumn> = <key>`
    — the same text-splice pattern used today for time-range filters, but on
    a categorical predicate.
6. **Executor**.  The minion executor receives a SQL string and a partition
   key; it builds segments, calls `validateSourceFingerprintAtCommit`, and
   writes back to runtime metadata.  None of that depends on partition shape;
   the only change is that `partStartMs` / `partEndMs` task config keys become
   "partition key" — kept as separate `partitionKey` task config to preserve
   the time-MV path's existing semantics.

### 2.3 Wire-format compatibility

Two-step roll-out, no breaking changes:

1. Land definition `partitionKind` (default `TIME`) and a `PartitionKey` value
   type internally.  Existing TIME-MVs round-trip identically.
2. Land the CATEGORICAL/SINGLETON code paths behind the new discriminator.
   The runtime znode's map-field key remains a string in both shapes; the
   reader path branches on `partitionKind` to interpret the string as either a
   millisecond timestamp or a categorical key.

The mixed-version concern (older controllers reading a fixed-partition runtime
znode written by a newer controller) is closed by the `partitionKind` field
defaulting to `TIME` on absence — older controllers that don't know about fixed
partitions will refuse to parse the categorical keys (`NumberFormatException`
on `Long.parseLong(key)`) and the existing `fromZNRecord` forward-compat
catch-and-ignore preserves the rest of the metadata.

### 2.4 Interface cleanliness — what's already done and what isn't

| Interface                                           | State today                                       | Action needed when fixed-MV lands                                                                                |
| --------------------------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `MaterializedViewQueryExecutor`                     | Shape-neutral.                                    | None.                                                                                                            |
| `MaterializedViewTaskGeneratorContext`              | Shape-neutral (metadata + ZK lookup only).        | None.                                                                                                            |
| `MaterializedViewConsistencyManager.on…Change`      | Range-based; full-invalidation entry point added. | Dispatch on `partitionKind` internally; callers unchanged because they already use `onBaseTableFullInvalidation` |
|                                                     |                                                   | when they cannot supply a tight range.                                                                           |
| `MaterializedViewRuntimeMetadata`                   | `_partitions: Map<Long, PartitionInfo>`.          | Generalize to `Map<PartitionKey, PartitionInfo>` (or `Map<String, …>` with a separate `PartitionKind`).          |
|                                                     | `_watermarkMs: long`.                             | Make optional; populated only for `TIME`.                                                                        |
| `PartitionFingerprint` / `PartitionState`           | Shape-neutral.                                    | None.                                                                                                            |
| `AggregationEquivalenceRegistry` (lands in PR 2)    | Shape-neutral (re-agg algebra).                   | None.  PR 1 keeps just a string set in the analyzer for create-time validation.                                  |
| `MaterializedViewAnalyzer.validateSourceTable`      | Time-MV only.                                     | Sibling validator for CATEGORICAL: require partition column existence and (optional) frozen key list.            |
| `MaterializedViewAnalyzer.validateTaskConfigs`      | Hard-requires `bucketTimePeriod`.                 | Make required-on-`TIME`; optional / absent on CATEGORICAL.                                                       |
| `MaterializedViewTaskScheduler.generateTasks`       | Watermark + APPEND.                               | Branch on `partitionKind`; CATEGORICAL goes through FIFO-STALE + refresh-interval logic.                         |
| `MaterializedViewTaskExecutor.executeTask`          | Window-based.                                     | Branch on task-config `partitionKey` shape; segment building is unchanged.                                       |
| `MaterializedViewDefinitionMetadata` ZK wire format | Untyped; can carry extra fields.                  | Add `partitionKind` + `partitionColumn` keys; defaulted by older readers.                                        |

### 2.5 Minimum viable fixed-partition PR

If the next contributor wants to land CATEGORICAL support in one focused PR,
the smallest defensible scope is:

- `PartitionKind` enum + `MaterializedViewDefinitionMetadata.partitionKind`
  field with `TIME` default.
- New analyzer entry point `validateCategoricalSourceTable` that takes a
  partition-column name; reuse the existing `validateSourceTable` for the
  mutable-source-type guards.
- `MaterializedViewRuntimeMetadata`: switch `_partitions` key from `Long` to
  `PartitionKey` (sum type).  `_watermarkMs` stays for back-compat; readers
  treat it as advisory on non-TIME MVs.
- `MaterializedViewConsistencyManager`: branch on `partitionKind` in
  `markPartitionsDirty`; CATEGORICAL marks all partitions STALE in one CAS.
  No changes to public API.
- `MaterializedViewTaskScheduler`: new `generateCategoricalTasks` method that
  enumerates STALE partitions, builds `WHERE <partitionColumn> = <key>` SQL
  text, and emits one task per partition (bounded by `maxTasksPerBatch`).
- `MaterializedViewTaskExecutor`: read `partitionKey` from task config and
  treat it as opaque; pass-through to runtime metadata update.

`SINGLETON` falls out of `CATEGORICAL` by treating it as one fixed key — no
extra code path needed once the above lands.

## 3. Things explicitly out of scope (for this document)

- Streaming row-by-row consumption of the gRPC response in
  `MaterializedViewTaskExecutor`.  Tracked separately; orthogonal to partition
  shape.
- Broker-side rewrite of the user query against a fixed-partition MV.  The
  re-aggregation rules in `AggregationEquivalenceRegistry` are
  partition-shape-agnostic; what the broker side needs to add is a
  partition-key predicate on the rewrite output (so a user query that filters
  by `tenant_id` gets routed to the matching MV partition's segments).  That
  belongs in the broker rewrite PR series, not in the MV ingestion module.
- Cross-shape MVs (e.g. time × tenant): doable by composing
  `Map<TimeBucket, Map<CategoricalKey, PartitionInfo>>`, but no concrete user
  ask today.  Defer until there is.
