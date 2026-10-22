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
# Native minion task framework implementation plan

Status: **draft; the execution ledger now includes bounded occurrence scheduling, an authenticated minion worker,
and fenced publication for `NativeSegmentGenerationTask`. General task-plugin and task-API parity remain incomplete.**

The metadata read adapter is the first integration step. `MinionMetadataAccessor` exposes table, schema and segment
reads; `HelixMinionMetadataAccessor` preserves the current behavior. Existing property-store bindings install that
adapter. An explicit alternate read accessor cannot enable legacy task execution. The existing task factories still
accept `MinionTaskZkMetadataManager`, and existing plugins still contain direct Helix calls and unfenced publication.
The dedicated minion transport requires an explicit certificate binding with the `MINION_TASK` capability.
No minion receives a controller metadata identity or query-serving capability.

The internal ledger now supports table-owned submissions, queue pause/resume, explicit minion identity assignments,
session-fenced claims, monotonic progress, execution outcomes, bounded retries and cancellation. Captured table/schema
and declared input versions are immutable. Snapshot format 10 adds schedules and publication manifests to task queues,
tasks, attempt history and the durable request-stream sequence/floor. Formats 8 and 9 remain readable only with their
original record families and states. Typed
protobuf values expose bounded controller and own-minion projections. `EXECUTION_FINISHED` remains an execution-only
legacy ledger state. Output-producing native tasks can become `SUCCEEDED` only in the transaction that creates their
publication manifest and immutable offline content records.

Retry of a running attempt requires expiry, retirement or replacement of its exact session. Readiness and draining
changes alone cannot permit stealing that attempt. The first worker has one active attempt at a time, finite retries,
and immutable inputs and outputs. General plugin negotiation, resource pools, cron schedules, execution timeouts,
retry backoff and plugin checkpoint mutation remain future work.

Helix/ZooKeeper remains the default. Native query listeners remain disabled. A task type becomes available only
after its scheduler, executor, observers and publication paths satisfy
the gates below. An unavailable task type must return an explicit unsupported error, rather than an empty successful
queue or a task marked complete without its effects.

## Implemented execution slice

`StartMinion` now selects the native runtime only for `pinot.coordination.backend=RATIS`, before constructing a Helix
manager or applying ZK cluster configuration. It uses the normal minion instance/data-directory settings and the
existing native identity, endpoints and TLS settings. It never initializes legacy task factories or mutable
`MinionContext` bindings.

The qualified task generates a genuine Pinot OFFLINE segment from a digest-pinned JSON-lines file using Pinot's
segment creation driver. This loopback foundation requires approved local source and shared output directories:

```properties
pinot.coordination.backend=RATIS
pinot.minion.instance.id=Minion_native_0
dataDir=/absolute/minion-state
pinot.coordination.ratis.participantId=Minion_native_0
pinot.coordination.ratis.minion.sourceRoot=/absolute/task-input
pinot.coordination.ratis.minion.outputRoot=/absolute/task-output
```

Configure the usual `pinot.coordination.ratis.historyId`, `metadata.endpoints`, `tls.certChain`, `tls.privateKey` and
`tls.trustCerts` values as for other native participants. The controller's provider must explicitly bind the minion
certificate to this participant ID, role `MINION` and capability `MINION_TASK`. On each controller, configure
`pinot.coordination.ratis.controller.tasks.outputRoot=/absolute/task-output`. All output roots must identify the same
existing directory; runtime construction does not grant access to an arbitrary path from a REST request.

The controller exposes these experimental, authorized, table-scoped APIs:

- `PUT /tasks/native/{table}/schedules/{uuid}` creates an immutable schedule generation.
- `GET /tasks/native/{table}` returns committed schedules, queues, tasks and publications, plus the current round.
- `POST /tasks/native/{table}/{taskUuid}/cancel?expectedVersion={version}` fences the selected task version.

For an immediate one-shot task, submit the following body with a fresh schedule UUID:

```json
{
  "minionId": "Minion_native_0",
  "sourceUri": "file:///absolute/task-input/rows.json",
  "sourceSha256": "<64 lowercase hexadecimal SHA-256 characters>",
  "firstRound": 0,
  "intervalRounds": 0,
  "occurrences": 1,
  "maxAttempts": 2
}
```

The same schedule ID and identical request resolve the same schedule; different inputs conflict. A recurring finite
schedule uses an absolute `firstRound`, positive `intervalRounds` and bounded `occurrences`. It catches up one
occurrence per scheduler cycle. These are committed membership rounds, not cron or wall-clock timestamps; a membership
timing-policy change fences that schedule generation. Only its live table owner schedules or retries tasks.
Admission applies backpressure before a new runnable task can exceed the worker's complete bounded projection;
an unaccepted occurrence does not advance its schedule watermark.

The worker stores exact pending requests and outcomes in its durable directory, retries unknown responses without
changing their identity, and separates heartbeats from execution. A new process receives a new registration incarnation
and cannot replay an old attempt's publication. Lease uncertainty stops admission and generation; replicated validation
is the final authority. Cancellation acceptance revokes publication and does not assert that remote computation has
already stopped.

Files are created without overwrite under history/task/attempt/incarnation paths and synced before publication.
The publication transaction validates the current live minion session, attempt, table incarnation, captured input
versions and output identities. It atomically creates the immutable content records, publication manifest and
`SUCCEEDED` task/attempt states. A stale worker can leave an unreferenced artifact but cannot publish it. No
automatic physical cleanup or exactly-once computation is claimed.

The initial bounds are explicit: one active attempt per minion, a 128-record/2 MiB worker projection, a 4,096-record
controller task image, and 128 retained local process journals before explicit archival is required. Terminal
catalog history is retained; exceeding the controller image limit fails closed. Retention/compaction and larger-scale
qualification remain outstanding. Input files default to 16 MiB and 100,000 rows, with component-configured ceilings
of 64 MiB and 1,000,000 rows.

Publication means committed content, not replica installation or query availability. The controller now reconciles
all published immutable OFFLINE content, including task outputs and validated controller uploads, through the existing
native installation protocol. Its normal task scan shares one live
controller registration, table ownership and durable request stream with scheduling; each cycle attempts at most one
schedule, retry or output reconciliation, rotating fairly over all candidates. Unknown replies retain their request
identity. Controller replacement reclaims unfinished operations; completed current installations do not churn.

Automatic placement requires the administrator-owned cluster configuration key
`pinot.coordination.ratis.offline.placement`, with this strict JSON value:

```json
{"servers":{"Server_one":{"tenant":"DefaultTenant","failureDomain":"zone-a"},"Server_two":{"tenant":"DefaultTenant","failureDomain":"zone-b"}}}
```

Configure it through the normal authenticated cluster-config API. The policy supports at most 64 approved stable
SERVER IDs. `segmentsConfig.replication` must be an explicit integer from 1 through 16. A null server tenant retains
Pinot's `DefaultTenant` semantics; explicit tenant names must match the approval. Each replica requires a distinct
approved failure domain and a current ready, non-draining SERVER registration. Endpoint advertisements cannot grant
these approvals. Tag overrides, tiers, replica-group/fixed-replica routing and Helix assignment options are rejected,
not silently ignored. The supported placement policy and endpoint-approval keys can change with native content
present; other cluster changes retain their existing reconciliation gate.

The replicated command computes placement from current policy, table/schema versions, membership and assignments.
It creates an assignment and its deterministic installation intent atomically, then claims/completes that intent
through exact server receipts. A desired assignment alone never proves a physical installation. Policy changes take
effect on subsequent reconciliation without restart. Removing the policy pauses automatic placement and reports
`BLOCKED`; an explicit `{"servers":{}}` withdraws approvals for reconciliation. Neither action claims immediate
physical file deletion or query availability.
Eligible existing replicas are retained before selecting new servers. Explicit approval, tenant or failure-domain
changes can release old assignments before replacements install; this slice does not guarantee add-before-remove
migration or zero-downtime rebalance. Native query listeners remain disabled.

`GET /tasks/native/{tableName}` adds `placements` and `placementOperations`. Each output reports requested, current
eligible desired and installed replica counts separately. An unavailable count is `-1`. `BLOCKED` means missing or
invalid policy/control; `UNSUPPORTED` means unsupported table or output constraints; `WAITING_FOR_SERVERS` means insufficient
eligible failure domains; `UNDER_REPLICATED`, `INSTALLING` and `RECONCILING` expose unfinished transitions.
`INSTALLED` requires exact current assignment/materialization receipts and successful installation operations.
`FAILED` exposes terminal installation intent requiring operator recovery; the scheduler does not manufacture new
epochs merely to retry terminal errors. Receipts from replaced or expired server sessions do not count.
All of these statuses describe the captured metadata revision, not query-serving readiness.

Full existing `/tasks` REST/UI compatibility, remote deep-store adapters, plugin checkpoint mutation,
replacement/deletion/lineage and other legacy task types remain separate requirements below.

## Authority and durability requirements

- Task records, scheduling decisions, claims and outcomes are replicated through Ratis. A local scheduler queue or
  an executor future is not the durable source of truth.
- Every claim captures history, job/subtask identity, monotonic attempt epoch, exact participant incarnation,
  table object incarnation and relevant input object versions. A deleted and recreated name cannot reuse authority.
- Every result, checkpoint and publication validates the current attempt and dependencies atomically with the write.
  Rejecting an old attempt's final status alone does not prevent that attempt from uploading or deleting data.
- Retry is at-least-once computation with idempotent, fenced publication. No exactly-once external I/O claim is made.
- Quorum loss or an unknown write outcome must not be reported as absence, successful cancellation or completed work.
  Retry an identical request identity, or resolve its durable outcome before advancing the request stream.
- Cancellation revokes publication authority before acknowledging acceptance. It does not claim that the worker has
  stopped or undo completed external I/O. Cancellation acceptance and worker termination are separate observations.
- Runtime controls use component, cluster or table configuration as appropriate. Dynamic policy changes carry
  versions; no JVM system property or environment-variable fallback determines task behavior.

## Implementation checkpoints

Each checkpoint is independently reviewable and remains draft until its acceptance evidence is recorded.

### 1. Durable task model and scheduling ownership

Add dedicated `TaskProtocol` and `TaskCatalog` types. The current `OperationCatalog` models table-owned offline
segment reloads; it must not be relabeled as a general task scheduler. Introduce protected queue, job, attempt,
schedule and checkpoint records, with explicit schema/snapshot versions and bounded payloads.

Job input includes task type, immutable subtask configurations, target table identity, input dependencies, tenant/tag,
concurrency limits, timeout/retry policy and submission identity. Controller submissions require the appropriate
current ownership token: table shard for table tasks, or explicit tableless-task scope. Keep task names and subtask IDs
stable for existing API clients; durable object identity remains independent of a display name.

A scheduled occurrence is keyed by schedule identity, config generation and scheduled instant. Capture timezone and
misfire/catch-up policy. Atomically accept the generated job and advance its occurrence watermark. If a generation
exceeds the transaction size bound, stage immutable bounded chunks and make them runnable only through one validated
finalization record; incomplete generations never become eligible work. Controller failover recovers staged and
accepted generations without issuing duplicate jobs.

Acceptance: competing controllers submit one occurrence; repeated request returns its original outcome; changed input
under the same request identity is rejected; table recreation fences old jobs; interrupted generation cannot run a
partial batch; snapshot/restore retains every deduplication and ownership fence.

### 2. Claims, attempts, retry and cancellation

A claim transaction checks the live, ready, non-draining minion session, its authorized task capability and tag, queue
state, current job state, resource/concurrency limits and retry eligibility. It advances the attempt epoch and records
an exclusive worker assignment. Counters used for concurrency enforcement are updated in that same transaction.
Worker task-type advertisements are checked against configured authorization; self-advertisement grants no authority.

Persist bounded progress and the terminal outcome for every attempt. Distinguish retryable error, fatal error,
timeout, cancellation and successful publication. Queue pause stops new claims; stop/cancel and deletion have separate
state transitions. Deletion retains fences until no delayed worker or retained request can resurrect the job.

Membership expiry invalidates a worker's authority. Local monotonic timers stop admitting work on renewal uncertainty
and request cancellation, but the replicated state machine decides whether an old attempt may publish or a new attempt
may claim. Durable timeout and backoff eligibility require a qualified clock policy and explicit behavior across policy
changes; client-supplied wall-clock timestamps cannot establish authority. Do not hold old worker concurrency forever,
but do not count its fenced external work as safely stopped.

Acceptance: concurrent claims obey both per-instance and task limits; paused queues issue no new claims; retry budget
survives restart; cancelled/expired/replaced attempts cannot checkpoint or publish; delayed completion cannot overwrite
a newer attempt; timeout and policy-change tests exercise the persisted clock rules.

### 3. Authenticated minion transport

Add a typed `MinionTaskService` to `metadata.proto`, with a dedicated access policy, bounded codecs and explicit
request/outcome semantics. Support own eligible work, claim, report/checkpoint, outcome lookup and attempt-scoped input
metadata. Reuse certificate-bound participant registration/renewal only after adding deliberate MINION authorization.

Minions receive no controller `MetadataService` identity and no arbitrary catalog-mutation endpoint. Each operation
rechecks certificate identity, role, current participant token, authorized task type and attempt scope. Input metadata
is a coherent projection with dependencies, not unrestricted controller snapshots. Admission, payload/record bounds,
deadlines, revocation and audit behavior must match the existing transport contracts.

Acceptance: one minion cannot claim or report another's attempt, mutate arbitrary config or broaden its task scope;
revoked/replaced credentials cannot regain authority through retry; response loss resolves through retained outcome;
oversized or incomplete input projections fail explicitly.

### 4. Normal minion starter and worker recovery

Introduce a native minion runtime behind the normal starter's explicit backend selection. Preserve existing Helix
startup and plugin initialization. Register task capabilities only after their dependencies are ready; renew the exact
participant incarnation and stop admitting work when draining or uncertain.

Dispatch with an explicit attempt context carrying the claim fence and captured inputs. Bound executor concurrency,
observer callbacks and local queues. Retain an outbox for uncertain claim/report/publication requests and reconcile it
before rerunning work after restart. Task progress held only in worker memory is diagnostic; durable state remains in
the task ledger. A fresh process incarnation must not reuse an old participant or attempt token.

The initial read-accessor guard rejects all unknown providers, including decorators. It is a temporary conservative
boundary, not an extensible authorization mechanism. Replace it with startup-selected execution integration when this
runtime and attempt-bound publication exist. Metadata accessor binding is startup-only; it must not switch the shared
`MinionContext` underneath running tasks.

Acceptance: start/stop/drain/restart works with no ZK client or process; a worker paused beyond its lease cannot publish
after resuming; restart resolves uncertain committed work without duplicating effects; ordinary Helix minion behavior
and plugin initialization remain covered.

### 5. Attempt-bound publication and checkpoint storage

Introduce a checkpoint contract with opaque history/object/version identity and explicit create-if-absent semantics.
The existing integer version and `-1` wildcard APIs remain legacy-only. Native writes also require the current attempt
and target-table incarnation. Migrate observer checkpoint writes with executor writes; observers must not bypass the
attempt fence on success, failure or cancellation.

Write outputs to immutable attempt-specific locations. Controller publication atomically validates the attempt and
input dependencies while committing the visible output manifest, checkpoint updates and task outcome. Large manifests
use staged chunks and one bounded visible root. Lost responses resolve against the committed identity; no broad retry
with freshly read versions is allowed to change the meaning of the original operation.

Cover every output path: segment upload/metadata push, segment replacement, lineage, purge/deletion, materialized-view
updates and plugin-specific metadata. A read or local permit before an HTTP upload is insufficient: the controller
must validate authority at the transaction that makes the output visible. Cleanup may delete only retired immutable
objects whose references cannot be recreated, or the attempt's own uncommitted outputs. A delayed remote delete must
never target a shared mutable path or an object that a newer attempt can adopt.

Acceptance: old workers resumed after cancellation, lease expiry and reassignment cannot publish, checkpoint or delete
newer output; commit-before-response-loss produces one visible result; metadata/output consistency survives controller
failover; a real plugin output is accepted and installed through the normal native segment lifecycle.

### 6. Controller task facade and generators

Extract a backend-neutral controller task facade with a Helix adapter. Replace direct `TaskDriver`, property-store and
Helix-manager dependencies in the task REST resource through that facade. Scheduling still requires full generation
validation; do not route task writes through generic configuration transactions.

Refactor `ClusterInfoAccessor` through explicit metadata, assignment, lineage, controller-discovery and task-state
services. It currently exposes `IdealState`, `PinotHelixResourceManager`, the Helix task manager and leadership APIs.
The native generator uses a coherent input image and captures dependencies checked again on submission. Generators
that delete segments or mutate state must use dedicated fenced operations rather than hiding side effects in reads.

Acceptance: generate/schedule/list/debug/cancel/retry operations use the same public API shapes; ownership change during
generation rejects stale submission; no generator requires a fake Helix manager or null property store; all mutations
retain normal resource authorization.

### 7. REST and UI parity

Retain task IDs, JSON fields and public state spellings used by the existing console. Cover task types, queues, summary,
counts, state, configuration, debug information, scheduler information, cleanup/pause/resume/delete and minion status.
Document how native internal states map to public task and subtask states; do not mark work `COMPLETED` before its
publication transaction commits.

Keep durable progress separate from `/tasks/subtask/.../progress` worker observations. Missing/unavailable workers
must not become zero progress or a successful empty response. Preserve auth headers and bounded fan-out for minion
admin APIs. Cron information describes real durable schedules and their owner, not synthesized Quartz state.

Acceptance: existing console navigation and task operations work against both backends; failure/quorum-loss responses
remain visible; resource authorization and tenant filtering match the existing API contract.

### 8. Plugin qualification

Migrate shared read helpers first, then executor factories, event observers and generator dependencies. Inventory direct
Helix access in `BaseTaskExecutor`, `MinionTaskUtils`, `SegmentConversionUtils`, and individual task implementations.
MaterializedView, Purge, RefreshSegment, UpsertCompactMerge, UpsertCompaction, RealtimeToOfflineSegments and MergeRollup
have dependencies beyond the initial read seam. Tasks requiring realtime, lineage or segment-replacement support stay
unavailable until those domain operations are implemented.

Preserve existing plugin entry points for Helix. Native registration requires an explicit supported contract/version;
legacy plugins must not be auto-enabled because their constructors accept null or their metadata reads happen to work.
Maintain a task-type capability matrix with the exact migrated dependencies and validation evidence.

Acceptance: each enabled task runs through generation, execution, output publication and final status; its retry and
cancellation paths are fenced; unsupported plugins fail explicitly before scheduling; no silent fallback to ZK occurs.

### 9. Recovery, migration and qualification

Exercise a three-voter cluster across leader failure, quorum loss, worker partitions/pauses, controller/minion restarts
and snapshot recovery. Preserve task outcomes, attempts, schedule watermarks, checkpoints and tombstones during restore.
Bound retention and define reclamation only when delayed requests and workers cannot regain authority.

The first migration uses a maintenance window. Stop generation, drain or cancel executing Helix work, reconcile
uncertain effects, and import validated metadata and terminal outcomes into a new native history. Never import a live
Helix assignment or worker token as native authority. Verification and rollback must account for already published
outputs; resuming the old scheduler cannot blindly resubmit imported work.

Acceptance: recover the same durable job/outcome and visible output after every injected failure; no concurrent old and
new scheduler can publish; migration detects unresolved running work; retained evidence demonstrates full supported
task-type coverage. Only then describe the native task framework as a supported Helix replacement.
