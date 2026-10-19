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
# Ratis metadata coordination

This internal, experimental module develops metadata, recovery and native offline installation for embedded Apache
Ratis. It targets Java 25, keeps Ratis dependencies outside the Java 11 common/SPI artifacts, and uses Apache Ratis 3.3.1.

The module is not a complete ZooKeeper or Helix replacement. Existing Pinot starters and quickstarts continue to use
Helix/ZooKeeper by default. No normal service starts a Ratis voter merely because this module is built.

The initial catalog stores cluster configuration, schemas and table configurations. Conditional transactions check
history, object identity and version before applying any mutation. Deleted object identities remain reserved so a
stale update cannot affect a recreated name. These are internal catalog mutations; deleting a table configuration
does not perform the normal table deletion lifecycle.

Clients register an incarnation and epoch, then submit commands on serialized request streams. A stream retains its
last success or semantic rejection until the client acknowledges it. Retrying the same request returns its original
outcome; changing the request under the same identity is rejected. Acknowledged requests and retired epochs cannot
execute again. This protocol does not provide exactly-once external effects. The internal Raft protocol assumes a
trusted controller; the metadata gRPC endpoint binds the client ID to its mutually authenticated TLS identity.

Snapshots preserve catalog state, object tombstones, client epochs, request outcomes and the retained change journal.
The storage layer verifies snapshot identity and checksums before publishing the recovered image, and refuses
recovery when a required snapshot is missing. Membership commands use format version 2; other commands retain version 1.
Old membership log entries are rejected before apply because they do not preserve the timing guarantees below.
Catalog snapshot format 8 persists the minimum next-tick delay and the maximum configured permit lifetime since the
last tick. Formats 3–7 are accepted only when membership has never been initialized; initialized histories require an
explicit maintenance-window migration to a new history. This experimental boundary does not support rolling upgrades.
Eligible older snapshots preserve their catalog, journal and supported offline records without inventing authority.
Format 2 starts with empty membership; format 1 additionally starts a new journal at the restored revision without
claiming historical deltas. References in both the current image and retained change journal are checked before
installation. No migration creates live participants.
Commands are limited to 1 MiB and 128 mutations, clients to 16 streams, and snapshot payloads to 256 MiB. Safe tombstone
reclamation and capacity qualification remain future work.

## Metadata distribution

Each `Client.query` submits an internal `ReadBarrier` command and evaluates its bounded typed query at that command's
committed log position. This also applies to membership reads and retained request-outcome lookups. Outcome lookups
return the original operation result and revision; other reads return the barrier revision. Native read-only and
stale-read requests are rejected by the state machine. Public protobuf endpoints retain their existing typed
authorization boundary and cannot submit arbitrary internal barrier commands.

`RatisMetadataAccess` uses a committed revision read before capturing an image or changes on its local voter. A
follower must apply that revision before it can serve the response. Immutable records are captured under the apply
lock; image serialization and cache listener callbacks run outside consensus application. Each control-plane read
adds a log entry and an empty journal transaction, so it consumes write throughput and advances the retention floor.
Capacity qualification must include this traffic. `ServingPermitGuard` uses local state during query admission.

The three-voter partition test first primes a stable read index, isolates that leader, commits a newer value on the
other two voters and sends a fresh client to the still-ready old leader. It reproduced a stale response with Ratis
3.3.1's read-only path, which can reuse a previously acknowledged index. Committed reads require a majority even
before the old leader detects quorum loss. This test is one fault case, not complete release qualification.

The journal retains at most 1,024 transactions and 16 MiB. Commands that change no selected records still produce
progress, including read barriers, acknowledgements, retries and semantic rejections. Pages preserve whole transactions and bind
cursors to the history, selected record kinds and authorization scope. A cursor below the durable retention floor
returns `CURSOR_EXPIRED`; a transaction that cannot fit the requested page returns `RESPONSE_TOO_LARGE`.

`MetadataGrpcServer` and `MetadataGrpcClient` require mutual TLS and hostname verification. A separately supplied,
current `ControllerAccessPolicy` maps the certificate fingerprint to a controller identity and access generation.
Caller-controlled headers and message fields cannot select an identity. Trusted certificates without an authorized
controller mapping are denied. Requests and responses use a closed typed protobuf vocabulary; JSON is limited to
existing schema/table domain payloads, preserving exact decimal values and plugin fields.

The protobuf definitions and bounded `MetadataRpcClient` live in `pinot-common`, retaining Java 11 compatibility.
Workers can use this wire client without depending on Ratis server libraries or the controller metadata model.
`MetadataGrpcClient` adds model codecs on top of that client; existing wire names and tags are retained.

Snapshot handles retain a coherent image while the client reads bounded pages. Every page, including the final
handoff, checks the original revision against the current journal floor. Slow clients may need to resnapshot;
handles do not pin journal history indefinitely. Default limits are 16 handles, 8 MiB and 10,000 records per image,
32 MiB total retained serialized payload, 30-second handle lifetime, 2 MiB/128 records per page, 16 concurrent service
operations and a 15-second RPC deadline. Java object overhead is additional to serialized payload limits.

Authorization is checked at admission and before sending the response. Revoking access cannot roll back an admitted
write: a timeout or denied response can still mean an unknown write outcome. The audit hook records the controller,
operation and outcome without metadata payloads. This endpoint currently grants trusted controllers full catalog
authority. General worker metadata projections, delegated public API authorization, peer Raft TLS and live certificate
rotation are not implemented; this module does not claim production security completeness.

`MetadataImage` installs complete snapshots and applies whole change-feed transactions into an immutable local image.
Invalid histories, scopes, cursors or record identities leave the prior image untouched. Config/cache adapters are
outside this standalone foundation; it does not depend on the shared Helix config/cache extraction.

## Membership and serving permits

`startMembershipClock(MembershipClockSettings)` explicitly enables the local voter driver. Settings supply a qualified
monotonic clock, its maximum rate in parts per million and maximum uncertainty; there is no implicit clock or production
timing default. Enable it on all voters before admitting participants, then commit an explicit
`MembershipProtocol.Policy` through the trusted controller access API. Each leader first commits a control barrier
matching the actual Raft entry term, waits the persisted minimum delay and advances one logical round at a time.
The wait compares the current lower clock bound against the previous upper bound using checked rate arithmetic.
Clock regression, lost qualification, excessive uncertainty or arithmetic failure permanently fences that driver.
A pause never causes catch-up ticks;
one outstanding proposal remains owned until the native Ratis future terminates. A permanently wedged transport
future stops that driver's progress rather than creating overlapping proposals.

A policy requires at least two lease rounds and a permit lifetime no greater than `(leaseRounds - 1) * tickInterval`:
a renewal immediately before a tick has only that many full intervals left. Every policy change restarts the next-tick
wait for at least the largest previously configured permit lifetime since the last tick. Repeated changes accumulate
that bound; failover and snapshot recovery preserve it and wait a full interval again. After the delayed tick, old
permits have expired and subsequent ticks use the current policy interval. Renewal never shortens an existing lease,
including when a shorter-policy renewal commits but its response is lost.

Membership binds the durable participant ID and role to a process incarnation and registration epoch. A live ID
cannot be replaced. An expired or retired registration needs a new incarnation and expected epoch; a late renewal
cannot revive it. Renewals use contiguous sequences, retain their original success or semantic rejection, and validate
each member of a bounded batch separately. Expiry is evaluated against the committed round on every authority check,
without requiring a full membership scan or cleanup on each tick. Snapshots preserve these fences and retry outcomes.
Unregister is voluntary self-revocation: callers must close local serving authority before retiring a session.
These timing bounds do not yet provide safe administrative forced retirement of a still-serving participant.

The optional membership gRPC service derives participant ID, role and query capabilities from the authenticated
certificate's current `ParticipantAccessPolicy`. Requests can operate only on that participant; they cannot encode
control activation, clock ticks, policy changes or generic catalog writes. Two bounded membership workers are separate
from the four catalog workers, with separate Raft-client admission capacity so catalog requests with unknown outcomes
cannot consume the membership slots. Admission and reply authorization checks also apply to membership calls.
Handler isolation does not reserve HTTP/2 streams: normal-role wiring must use a dedicated membership connection
to prevent bulk requests on a shared connection from exhausting connection-level admission. Production credential
issuance and rotation remain unimplemented.

Read-only serving permits are separate from membership and never authorize ingestion, ownership or publication.
A permit includes the revision of its committed renewal, so a worker must apply that revision before admission.
`ServingPermitGuard` remembers the first local send time across retries and term changes, rejects expired or conflicting
grants, and checks a coherent local membership view. Its opaque query tickets have a finite deadline that renewals
cannot extend; capability removal, policy changes and lifetime narrowing fence previous tickets. Admission and result
delivery both recheck the local clock after reading the view. Clock regression, excessive uncertainty or image failure
closes admission. These checks require no RPC on the query path.

The guard requires an explicitly qualified clock and lifetime conversion policy. Test clocks are not production clock
qualification. The process-local guard is not yet wired into normal Pinot roles, and its tickets are not a cross-host
query protocol. Worker metadata projections, heartbeat scheduling/reconnect, additive query deadline propagation and
actual query/ingestion lifecycle integration remain required. A membership projection consumes the generic immutable image directly.

## Controller ownership and operations

An explicit immutable ownership layout starts with 24 table shards and the existing UTF-8 Murmur2 mapping. Database
qualification remains part of the raw table identity; OFFLINE/REALTIME companions share a shard. Cluster work and
tableless tasks have separate named scopes. Changing the shard count or hash requires a future fenced migration.

Only a coordinator command matching the actual committed Raft term and current control epoch can assign or revoke
ownership. Every assignment advances the scope's owner epoch. A publication checks the complete owner token against
the current assignment and the controller's exact participant registration, readiness, drain state and logical lease.
Expired ownership is invalid even if no cleanup has removed it. A Raft leader change does not by itself transfer table
ownership. Historical assignments remain in snapshots as fences and cannot become live merely through recovery.

Controller operations use the same durable request streams and retained outcomes as configuration transactions.
The typed offline segment reload intent records the table incarnation, immutable segment/content identities and
target participant, assignment and materialization identities. Accepted work can be claimed under a current owner;
each claim increases its attempt number. Checkpoint and completion require that exact owner, attempt and record
version. Completion checks the captured dependency versions before atomically publishing permitted table changes
and a terminal result. Cancellation fences late publication; it does not undo external work already performed.

The metadata RPC exposes typed operation commands only when the same TLS certificate has current controller client
and controller participant grants. The server derives the owner participant identity from that grant and rechecks
both grants before returning a result. An existing configuration client does not automatically gain operation access.
Ownership assignment remains a trusted embedded coordinator command. Snapshots and deltas expose typed ownership,
operation, content, assignment and installation receipt values; generic config transactions cannot write these families.

Native submission resolves every target against published content and current desired assignments, then captures
the exact content, assignment, table and schema versions. Callers cannot supply the captured dependencies. Claims
must retain them, and progress checks revalidate the target identities. A separate native command tag preserves
replay of earlier ledger-only commands; those old operations cannot acquire native execution rights. Native success
uses `CompleteInstalled`, which revalidates the current owner, attempt, dependencies and assignments, then records
the exact committed receipt versions for every target. Controller-supplied completion counters cannot declare success.

The SERVER installation RPC derives identity from `ParticipantAccessPolicy` and validates the exact live registration
against committed state. It exposes only that server's work for a supplied operation ID, receipt reporting and its own durable
request registration, acknowledgement and outcome lookup. Its request namespace includes the complete SERVER session;
normal metadata clients cannot use it. Authorization is rechecked before replies. Two separate installation workers
accept requests up to 64 KiB and responses up to 2 MiB without occupying membership workers. A lost report response
requires resolving or retrying the same request identity; it does not undo installation or imply completion.

`Snapshot` provides a complete own-session metadata projection after a committed read barrier, including before
readiness. It captures current participant/control state, cluster configuration, assignments targeting that exact
registration (including released assignments), their content and referenced table/schema records, plus IDs
of nonterminal assigned operations targeting that registration. Other targets, operation payloads and receipts are
omitted. Referenced config tombstones remain keyed by object identity after name recreation; they do not block
unrelated tables. A server advances `applied_revision` only after applying the entire projection; a work lookup's
revision does not certify metadata hydration. Requests bound the combined record/operation count to at most 4096
and the response to 1 KiB–2 MiB. Oversized images return `RESPONSE_TOO_LARGE` without a partial image. Capture scans
the current catalog; paged, indexed distribution for larger clusters remains future work. This projection creates
no serving or query authority and does not change disk snapshot formats.

## Offline content and loading inputs

Owner-fenced publication creates an immutable manifest with an unreused segment object ID, table incarnation,
SHA-256 content digest, download URI and native loader attributes. It does not verify external bytes during apply.
This initial publication protocol cannot overwrite an existing name in the same table incarnation; replacement,
retirement and physical reclamation need their own protocols.

Desired replica assignments bind that manifest to a complete server registration and exact table/schema versions.
Each reassignment or release advances an epoch and derives a new materialization ID. ONLINE assignment requires a
ready, live server. A current controller owner can release an old assignment after server or table replacement.
Desired ONLINE state is not an observation of installed or serving data.

`RatisOfflineTableMetadata` adapts one coherent committed image to the shared native offline loader input contract.
It rejects changed server/table incarnations and stale assignment compatibility. Config and schema objects are
detached from the published image. The ordinary offline table manager has an explicit metadata-based initializer;
the existing Helix initializer and provider values are preserved. Native initialization currently rejects realtime,
upsert, dimension tables, custom table-manager extensions and peer downloading until their adapters are implemented. The native
loading pipeline still uses a leaf `SegmentZKMetadata` value at its existing loader boundary; it creates no Helix
manager or property store. Loading inputs alone do not authorize installation or query serving.

`NativeInstanceDataManager` shares the table registry, lifecycle locks and executors with the default Helix instance
manager. It creates ordinary offline table managers and exposes conditional actions, rejecting legacy unfenced
mutations. A stable directory lock prevents two native instances from sharing installation paths; shutdown retains
that lock until in-flight actions drain. The injected `ServerInstance` construction path accepts this initialized
manager and access control without initializing Helix. It does not start query resources or grant query readiness.

`NativeServerParticipant` owns a real server registration and its independent membership connection, periodically
hydrates the complete own-session projection, and discovers durable operation IDs without relying on local wake-ups.
Its bounded workers use `NativeOfflineSegmentWorker` from `pinot-common`; server runtime code has no dependency on
Ratis consensus/model classes. Work is validated before creating a table manager. Cluster callbacks retain their
last successful delivery so failed updates, including removed keys, can be retried without advancing applied metadata.
The server rejects query-serving capability grants until query admission is integrated.

`InstallationOutbox` persists the exact SERVER session, request incarnation, sequence, report and outcome under the
exclusive native data-directory lock. Unknown outcomes reuse the same request; a result is fsynced before its
acknowledgement. Recovery runs independently of operation discovery, so a completed operation disappearing from a
snapshot cannot strand the report stream. Old-session files are retained as evidence, never replayed under a new
session. Evidence is bounded to 128 files of at most 256 KiB each; saturation requires explicit archival. Startup and
shutdown serialize; shutdown drains local actions and outbox leases before releasing directory ownership, then
retires the confirmed registration. Normal starter configuration, query admission and general table lifecycle remain
unimplemented; this runtime does not make the replacement complete or alter Helix defaults.

`RatisOfflineSegmentExecutor` connects fresh own-target work to the native `LOAD`, `RELOAD` or `RECONCILE` action and receipt publisher.
The installer verifies the downloaded SHA-256 and segment CRC, runs Pinot's existing preprocessing, and opens the
segment at its permanent materialization path. It fsyncs the files and checksummed manifest, atomically publishes the
selection manifest, then registers the segment with the table manager. Older directories remain stable for held readers;
their reclamation is deferred. Exact replay verifies the selected manifest and files and reuses its installation ID,
including after table-worker restart within the same SERVER session. A missing reload target returns `ABSENT` rather
than creating a segment. New process registrations require new assignment authority.

Explicit `RECONCILE` can restore an absent segment or replace the selected installation using an expected local
installation ID. With matching content and table/schema versions, it verifies the old manifest and exact file
inventory, copies those prepared bytes into a new permanent directory, and verifies the copy. Otherwise it downloads
and preprocesses the authoritative content. A new operation or server registration receives a fresh installation ID,
manifest and receipt; old receipts never authorize the new session. Only exact replay of that new operation reuses
its installation ID. Forced download bypasses local-byte reuse. Ordinary `LOAD` and `RELOAD` semantics are unchanged.

Publication refreshes work and checks local authority and the expected prior installation under the segment lock;
consensus revalidates the receipt against later global changes. Neither local installation nor a committed receipt
grants query-serving rights. This path currently supports ordinary OFFLINE tables with the default local segment
directory loader, without tiers, peer download, upsert or dimension tables. Normal-role starters, controller REST
actions, controller work scheduling, routing publication and complete query-serving fences remain absent. This is an internal execution
path, not a complete controller/server reload integration or a supported Helix/ZooKeeper replacement.

## Query identity and pinned content

The normal Thrift `InstanceRequest` (optional field 9) and gRPC `ServerRequest` (optional field 7) carry the same
versioned native query identity. `NativeQueryContext` normalizes history, broker and target-server registrations,
query/attempt UUIDs, exact table/schema versions and per-segment content, digest, assignment and materialization
identities. It bounds the context to 4096 segments and 2 MiB, rejects malformed identities and requires exact,
duplicate-free agreement with the legacy required/optional name lists. Requests without the context retain their
existing Helix behavior. Version 1 covers one ordinary OFFLINE table and grants no serving permission or deadline.

Native publication binds the installation identity and detached normalized schema to the exact reference-counted
segment manager. `SingleTableExecutionInfo` pins those handles before checking identities, retains their schema
with Pinot's built-in virtual columns, and releases all acquired handles if validation fails. Replacing a name or
refreshing the table cache cannot change a pinned reader's inputs. Wrong generations return structured query error
236 (`ServerQueryIdentityMismatch`). Missing required content returns `ServerSegmentMissing`; it cannot be hidden
by a recent-deletion hint or `ignoreMissingSegments`. Missing optional content remains optional, but present optional
content must match. Logical-table acquisition also releases earlier handles when a later table fails validation.

Native names-only, realtime and logical queries remain rejected. A native request with no acquired content cannot
yet obtain a version-checked schema-only catalog snapshot and is rejected instead of returning an empty scan.
Normal broker production, pinned routing images, feature-level negotiation, authenticated process admission,
qualified-clock permits, immutable end-to-end deadlines, response-delivery fencing and whole-query retry/drain
remain unimplemented. MSE/time-series and other unsupported paths cannot silently drop the context. Native
`ServerInstance` query resources remain disabled until those gates are implemented and tested together.

## Voter configuration and validation

The voter configuration uses the `pinot.coordination.ratis.` prefix with `clusterId`, `historyId`, `id`, `voters`,
`dataDir` and explicit first-start `bootstrap=true`. Voters are a comma-separated list of `id@host:port` entries.
This foundation requires exactly three voters on literal loopback addresses until authenticated peer transport is added.
The history UUID is also the Raft group identity. An existing data directory must match its original identity;
missing storage cannot be silently bootstrapped. Optional `snapshot.threshold` and `request.timeoutMs` settings default
to 10,000 entries and 15,000 milliseconds respectively. A timed-out write has an unknown outcome: resolve or retry the
same application request identity before acknowledging it.

Build the module and its dependencies from the repository root:

```bash
./mvnw -pl pinot-coordination-ratis -am \
  '-Dtest=MetadataCatalogTest,MetadataChangeFeedTest,MembershipStateTest,ControllerCoordinationTest,OfflineInstallationTest,RatisMetadataStoreTest,RatisReadSafetyTest,RatisMembershipClockTest,ServingPermitGuardTest,MetadataGrpcTransportTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Validate the shared worker and native server lifecycle, including existing Helix lifecycle coverage:

```bash
./mvnw -pl pinot-server -am \
  '-Dtest=NativeOfflineSegmentWorkerTest,NativeServerMetadataTest,NativeServerParticipantTest,InstallationOutboxTest,NativeInstanceDataManagerTest,HelixInstanceDataManagerConfigTest,HelixInstanceDataManagerTest,HelixInstanceDataManagerLifecycleTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Metadata durability, distribution and membership contracts are parts of the replacement. Normal role integration,
qualified liveness and query fencing, controller ownership integration, assignment and state transitions, realtime
ingestion, minion tasks, migration and a standalone Ratis quickstart require their own integration and qualification
before a supported replacement is claimed.
