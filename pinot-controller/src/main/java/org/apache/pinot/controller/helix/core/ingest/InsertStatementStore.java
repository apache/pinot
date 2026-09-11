/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// ZooKeeper-backed persistence layer for {@link InsertStatementManifest} objects.
///
/// Manifests are stored as ZNRecords under the path
/// `/INSERT_STATEMENTS/{tableNameWithType`/{statementId}}. The manifest JSON is stored
/// in a simple field of the ZNRecord so that the full object can be round-tripped without loss.
///
/// This class follows the same property-store pattern used by
/// {@link org.apache.pinot.common.lineage.SegmentLineageAccessHelper}.
///
/// Thread-safety: individual read/write operations are atomic at the ZK level.
/// Higher-level read-modify-write sequences must be coordinated by the caller.
///
/// ## ZK schema and rolling-upgrade protocol
///
/// Two ZK trees are maintained:
///
/// - `/INSERT_STATEMENTS/{table`/{statementId}} — each znode stores a ZNRecord with a
///       `manifest` field holding the JSON-serialized {@link InsertStatementManifest}.
///       The manifest JSON carries its own `schemaVersion` field
///       ({@link InsertStatementManifest#CURRENT_SCHEMA_VERSION}) so a controller can detect a
///       newer manifest and gate behavior accordingly.
/// - `/INSERT_REQUEST_IDS/{table`/{requestId}} — idempotency reservations. The ZNRecord
///       carries `statementId` plus a `schemaVersion` field
///       ({@link #RESERVATION_SCHEMA_VERSION}) for the same forward-compat purpose.
///
/// Backward compatibility is maintained by the JSON deserializer's
/// `@JsonIgnoreProperties(ignoreUnknown = true)` plus defaulting a missing
/// `schemaVersion` to `1` so rolling upgrades from pre-versioning controllers read
/// cleanly. To introduce a breaking change, bump the version AND gate new-format writes on a
/// cluster-config flag until all controllers are upgraded.
public class InsertStatementStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementStore.class);

  /// Top-level propertyStore prefixes are owned by ZKMetadataProvider so the full reserved
  /// namespace stays in one file. Local aliases keep call sites short.
  private static final String INSERT_STATEMENTS_PREFIX = ZKMetadataProvider.PROPERTYSTORE_INSERT_STATEMENTS_PREFIX;
  private static final String REQUEST_IDS_PREFIX = ZKMetadataProvider.PROPERTYSTORE_INSERT_REQUEST_IDS_PREFIX;
  private static final String MANIFEST_FIELD = "manifest";
  private static final String STATEMENT_ID_FIELD = "statementId";
  /// Reservation ZNRecord schemaVersion field. Used ONLY in the `/INSERT_REQUEST_IDS` tree.
  /// Distinct on the wire from the manifest envelope's `envelopeSchemaVersion` — this avoids
  /// the two version surfaces colliding at the JSON layer if a future change ever embeds them in
  /// the same record.
  private static final String SCHEMA_VERSION_FIELD = "schemaVersion";
  /// Manifest envelope ZNRecord schemaVersion field. Used ONLY in the `/INSERT_STATEMENTS`
  /// tree, in the outer ZNRecord that wraps the JSON-serialized {@link InsertStatementManifest}.
  /// The wrapped JSON body has its own `schemaVersion` property
  /// ({@link InsertStatementManifest#getSchemaVersion()}); using a distinct envelope-field name
  /// (rather than reusing `"schemaVersion"`) prevents either layer from accidentally
  /// shadowing the other when records are inspected as raw ZNRecords.
  private static final String ENVELOPE_SCHEMA_VERSION_FIELD = "envelopeSchemaVersion";
  private static final String TOMBSTONE_PREFIX = "__TOMBSTONE_";
  /// Sentinel prefix for tombstones the cleanup sweep has CAS-claimed for deletion. The rebind path
  /// refuses to rebind any record with this prefix, so once {@link #pruneStaleReservationTombstones}
  /// succeeds in CAS-setting this sentinel, the subsequent unconditional remove cannot wipe a fresh
  /// rebound reservation — any later rebind read will see the sentinel and back off.
  private static final String GC_PENDING_PREFIX = "__GC_PENDING_";

  /// Current schema version for `/INSERT_REQUEST_IDS` ZNRecord bodies. Written on create and
  /// rebind. Bump when a breaking change is made to the ZNRecord shape.
  public static final int RESERVATION_SCHEMA_VERSION = 1;

  /// Current schema version for `/INSERT_STATEMENTS/{table`/{stmt}} ZNRecord envelopes
  /// (i.e. the outer record holding the {@link #MANIFEST_FIELD} JSON). The JSON body itself also
  /// carries a `schemaVersion` via {@link InsertStatementManifest#getSchemaVersion()}; the
  /// envelope version exists separately so a future controller can introduce new ZNRecord-level
  /// fields (e.g. peer indexes) and old readers fail loudly rather than silently drop them.
  /// Bump when a breaking change is made to the manifest ZNRecord envelope shape.
  public static final int MANIFEST_ENVELOPE_SCHEMA_VERSION = 1;

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public InsertStatementStore(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /// Persists a new statement manifest in ZK. Fails if the node already exists.
  ///
  /// @return true if creation succeeded, false if the statement already exists
  public boolean createStatement(InsertStatementManifest manifest) {
    String path = buildPath(manifest.getTableNameWithType(), manifest.getStatementId());
    try {
      ZNRecord record = toZNRecord(manifest);
      return _propertyStore.create(path, record, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Failed to create insert statement manifest for statementId={}", manifest.getStatementId(), e);
      return false;
    }
  }

  /// Updates an existing manifest in ZK using optimistic concurrency (version check).
  ///
  /// @return true if the update succeeded, false on version conflict or other failure
  public boolean updateStatement(InsertStatementManifest manifest) {
    String path = buildPath(manifest.getTableNameWithType(), manifest.getStatementId());
    try {
      Stat stat = new Stat();
      ZNRecord existing = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
      if (existing == null) {
        LOGGER.warn("Cannot update non-existent insert statement: {}", manifest.getStatementId());
        return false;
      }
      ZNRecord record = toZNRecord(manifest);
      return _propertyStore.set(path, record, stat.getVersion(), AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      /// Version conflicts are expected under concurrent CAS retries; caller retries. Log at DEBUG
      /// to avoid log spam in production. Hard failures still log WARN/ERROR below.
      LOGGER.debug("Version conflict updating insert statement: {}", manifest.getStatementId());
      return false;
    } catch (Exception e) {
      LOGGER.error("Failed to update insert statement manifest for statementId={}", manifest.getStatementId(), e);
      return false;
    }
  }

  /// Reads a statement manifest from ZK.
  ///
  /// @return the manifest, or null if not found
  @Nullable
  public InsertStatementManifest getStatement(String tableNameWithType, String statementId) {
    String path = buildPath(tableNameWithType, statementId);
    try {
      ZNRecord record = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (record == null) {
        return null;
      }
      return fromZNRecord(record);
    } catch (Exception e) {
      LOGGER.error("Failed to read insert statement manifest for statementId={}", statementId, e);
      return null;
    }
  }

  /// Lists all statement manifests for a given table.
  ///
  /// @return list of manifests (never null; may be empty)
  /// @throws RuntimeException on ZK failure — callers must distinguish "no manifests" (empty list)
  ///   from "could not read ZK" (exception). Silently returning empty would let the cleanup sweep
  ///   skip cleanup and never retry.
  public List<InsertStatementManifest> listStatements(String tableNameWithType) {
    String parentPath = buildTablePath(tableNameWithType);
    try {
      /// Single batched ZK round-trip: pull all child ZNRecords in one shot rather than one
      /// get-per-child. With N manifests per table, this is O(1) round-trips instead of O(N).
      List<ZNRecord> records = _propertyStore.getChildren(parentPath, null, AccessOption.PERSISTENT);
      if (records == null || records.isEmpty()) {
        return List.of();
      }
      List<InsertStatementManifest> manifests = new ArrayList<>(records.size());
      int unreadable = 0;
      for (ZNRecord record : records) {
        if (record == null) {
          /// getChildren returns null for children that disappeared between the listing and the
          /// batch read (race with concurrent deleteStatement). Treat as a benign skip.
          unreadable++;
          continue;
        }
        try {
          manifests.add(fromZNRecord(record));
        } catch (Exception e) {
          /// Per-record deserialization failure (corrupt JSON, forward-incompat schemaVersion).
          /// Log loudly so the cleanup sweep can correlate "missing manifests" with
          /// INSERT_CLEANUP_SWEEP_FAILURES; do not throw — one bad record must not stall cleanup
          /// for healthy peers.
          unreadable++;
          LOGGER.warn("listStatements: failed to deserialize manifest for table={} record id={}; "
              + "skipping. Likely corrupt JSON or forward-incompat schemaVersion.",
              tableNameWithType, record.getId(), e);
        }
      }
      if (unreadable > 0) {
        LOGGER.warn("listStatements: {} of {} manifests for table={} were unreadable; cleanup "
            + "sweep will skip them until they become readable or are manually removed",
            unreadable, records.size(), tableNameWithType);
      }
      return manifests;
    } catch (Exception e) {
      LOGGER.error("Failed to list insert statements for table={}", tableNameWithType, e);
      throw new RuntimeException(
          "Failed to list insert statements for table=" + tableNameWithType + ": " + e.getMessage(), e);
    }
  }

  /// Deletes a statement manifest from ZK.
  ///
  /// @return true if deletion succeeded
  public boolean deleteStatement(String tableNameWithType, String statementId) {
    String path = buildPath(tableNameWithType, statementId);
    try {
      return _propertyStore.remove(path, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Failed to delete insert statement manifest for statementId={}", statementId, e);
      return false;
    }
  }

  /// Atomically reserves a requestId for a given table. If the requestId is already reserved,
  /// returns the existing statementId. Otherwise, creates a ZK node to reserve the mapping.
  ///
  /// This uses ZK's atomic `create` to prevent two concurrent retries from both
  /// creating statements for the same requestId. If the reservation cannot be reliably
  /// determined (ZK failure), throws an exception to fail closed rather than allowing
  /// duplicate executions.
  ///
  /// @param tableNameWithType the table name with type
  /// @param requestId         the client-supplied request ID for idempotency
  /// @param statementId       the statement ID to associate with this request
  /// @return null if the reservation succeeded (this caller wins), or the existing statementId
  ///         if already reserved by a prior request
  /// @throws RuntimeException if the reservation state cannot be determined due to ZK failure
  @Nullable
  public String reserveRequestId(String tableNameWithType, String requestId, String statementId) {
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      ZNRecord record = new ZNRecord(requestId);
      record.setSimpleField(STATEMENT_ID_FIELD, statementId);
      record.setSimpleField(SCHEMA_VERSION_FIELD, Integer.toString(RESERVATION_SCHEMA_VERSION));
      boolean created = _propertyStore.create(path, record, AccessOption.PERSISTENT);
      if (created) {
        return null;  /// This caller wins the reservation
      }
      /// Node already exists — read the existing statementId. A tombstone surfaces here as
      /// "__TOMBSTONE_<old>"; the coordinator's stale-rebind path passes that value to
      /// rebindRequestIdIfEquals, whose content check matches the prefix and rebinds atomically.
      /// We deliberately do NOT strip the prefix because the rebind CAS needs the exact ZK content.
      ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (existing != null) {
        return existing.getSimpleField(STATEMENT_ID_FIELD);
      }
      /// create returned false but node not readable — fail closed
      throw new RuntimeException("RequestId reservation in indeterminate state for requestId=" + requestId);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      /// ZK create failed — try to read to distinguish "already exists" from "ZK down"
      try {
        ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
        if (existing != null) {
          return existing.getSimpleField(STATEMENT_ID_FIELD);
        }
      } catch (Exception readEx) {
        LOGGER.error("Failed to read existing requestId reservation for requestId={}", requestId, readEx);
      }
      /// Cannot determine reservation state — fail closed to prevent duplicate execution
      throw new RuntimeException(
          "Failed to reserve requestId=" + requestId + " for statementId=" + statementId
              + "; failing closed to prevent duplicate execution", e);
    }
  }

  /// Removes a requestId reservation unconditionally. **Package-private:** the only
  /// safe caller in production is {@link #releaseRequestIdIfEquals}, which version-checks the
  /// tombstone write. An unconditional remove can wipe a freshly-rebound reservation written by a
  /// concurrent retry, enabling duplicate inserts. Visible to tests so they can drive the store
  /// directly without going through the rebind protocol.
  void releaseRequestId(String tableNameWithType, String requestId) {
    if (requestId == null) {
      return;
    }
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      _propertyStore.remove(path, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.warn("Failed to release requestId reservation for table={} requestId={}",
          tableNameWithType, requestId, e);
    }
  }

  /// Removes a requestId reservation only if it currently points at `expectedStatementId`.
  /// Use this from cleanup paths where a concurrent rebind may have re-pointed the reservation at a
  /// different (winner) statementId — in that case the original release is no longer valid because
  /// the node has been re-purposed for a different ongoing INSERT.
  ///
  /// @return true if the release happened (or the reservation didn't exist), false if the
  ///   reservation now points elsewhere and was therefore left in place
  public boolean releaseRequestIdIfEquals(String tableNameWithType, String requestId,
      String expectedStatementId) {
    if (requestId == null || expectedStatementId == null) {
      return true;
    }
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      /// Single-step soft-delete via version-checked overwrite to a tombstone marker. We do NOT
      /// call remove() afterwards: the unconditional remove from ZkHelixPropertyStore has no
      /// version check, so a concurrent rebind that bumps the node's version between our set and
      /// our remove would have its rebound reservation deleted, breaking idempotency for a third
      /// retry. The tombstone is observable to subsequent reserveRequestId calls as a "stale"
      /// reservation; the next retry enters the rebind branch and either claims it (winning the
      /// rebind CAS) or yields to a concurrent winner. Tombstones for never-retried requestIds
      /// accumulate but are bounded by the cleanup sweep's reservation-cleanup pass.
      Stat stat = new Stat();
      ZNRecord existing = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
      if (existing == null) {
        return true;  /// already gone — idempotent success
      }
      String currentStatementId = existing.getSimpleField(STATEMENT_ID_FIELD);
      /// Already a tombstone for the same statementId: idempotent success without rewriting. A
      /// rewrite would refresh ZK's mtime, defeating the prune sweep's mtime-based retention check.
      if ((TOMBSTONE_PREFIX + expectedStatementId).equals(currentStatementId)) {
        return true;
      }
      if (!expectedStatementId.equals(currentStatementId)) {
        /// Reservation has been rebound to a different statement (concurrent retry won the race),
        /// or it is a tombstone for a different statementId. Either way we must not overwrite.
        LOGGER.info("Skipping releaseRequestId for requestId={}: reservation now points at {}, not {}",
            requestId, currentStatementId, expectedStatementId);
        return false;
      }
      ZNRecord tombstone = new ZNRecord(requestId);
      tombstone.setSimpleField(STATEMENT_ID_FIELD, TOMBSTONE_PREFIX + expectedStatementId);
      if (!_propertyStore.set(path, tombstone, stat.getVersion(), AccessOption.PERSISTENT)) {
        LOGGER.info("releaseRequestId CAS lost for requestId={}: concurrent writer bumped version",
            requestId);
        return false;
      }
      return true;
    } catch (ZkBadVersionException e) {
      /// Legitimate CAS conflict — distinguish from ZK transient failures so the caller doesn't
      /// confuse "rebind raced" with "ZK is down".
      LOGGER.info("releaseRequestId CAS lost via ZkBadVersionException for requestId={}", requestId);
      return false;
    } catch (Exception e) {
      /// Transient ZK failure (connection loss, session expired). Re-throw so the caller fails
      /// closed rather than silently dropping the release request.
      LOGGER.warn("Transient ZK failure during releaseRequestIdIfEquals for table={} requestId={}",
          tableNameWithType, requestId, e);
      throw new RuntimeException("ZK failure during releaseRequestIdIfEquals", e);
    }
  }

  /// Rebinds a requestId reservation to a new statementId, but ONLY if the current reservation
  /// still points at `expectedStatementId`. Used when the previously reserved statement was
  /// GC'd but the reservation znode remains; this content-and-version-checked CAS closes the
  /// concurrent-rebind race where two callers both observe the same stale reservation.
  ///
  /// Race example without the content check: two concurrent callers both see stale reservation
  /// S in reserveRequestId, both create new manifests, both rebind. A version-only CAS allows both
  /// writes to succeed (the second reads the new version the first just wrote). Adding the content
  /// check forces the loser to observe the winner's statementId and abort.
  ///
  /// @param tableNameWithType   the table name with type
  /// @param requestId           the client-supplied request ID
  /// @param expectedStatementId the stale statementId we observed when reserveRequestId returned
  /// @param newStatementId      the new statement ID to associate with this requestId
  /// @return `true` if this caller won the rebind race; `false` if the reservation
  ///         node is gone, the content no longer matches `expectedStatementId` (a concurrent
  ///         rebind won), or the version-checked write failed
  public boolean rebindRequestIdIfEquals(String tableNameWithType, String requestId,
      String expectedStatementId, String newStatementId) {
    if (requestId == null || expectedStatementId == null) {
      return false;
    }
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      Stat stat = new Stat();
      ZNRecord existing = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
      if (existing == null) {
        LOGGER.info("RequestId={} reservation already deleted; skipping rebind to statementId={}",
            requestId, newStatementId);
        return false;
      }
      String currentStatementId = existing.getSimpleField(STATEMENT_ID_FIELD);
      if (currentStatementId != null && currentStatementId.startsWith(GC_PENDING_PREFIX)) {
        /// The cleanup sweep has CAS-claimed this tombstone for deletion. Refuse to rebind:
        /// doing so would let the imminent unconditional remove wipe a fresh reservation.
        LOGGER.info("Refusing rebind for requestId={}: reservation is pending GC", requestId);
        return false;
      }
      if (!expectedStatementId.equals(currentStatementId)) {
        /// Another concurrent caller already rebound this reservation. We lost the race.
        LOGGER.info("Rebind race lost for requestId={}: current={}, expected={}, attempted={}",
            requestId, currentStatementId, expectedStatementId, newStatementId);
        return false;
      }
      ZNRecord record = new ZNRecord(requestId);
      record.setSimpleField(STATEMENT_ID_FIELD, newStatementId);
      record.setSimpleField(SCHEMA_VERSION_FIELD, Integer.toString(RESERVATION_SCHEMA_VERSION));
      boolean updated = _propertyStore.set(path, record, stat.getVersion(), AccessOption.PERSISTENT);
      if (updated) {
        LOGGER.info("Rebound requestId={} from stale statementId={} to newStatementId={}",
            requestId, expectedStatementId, newStatementId);
      } else {
        LOGGER.warn("Failed to rebind requestId={} (version conflict)", requestId);
      }
      return updated;
    } catch (Exception e) {
      LOGGER.warn("Exception while rebinding requestId={} to statementId={}", requestId, newStatementId, e);
      return false;
    }
  }

  /// Reads the raw statementId value currently stored in the reservation node, without dereferencing
  /// to a manifest. Returns the value verbatim — including `__TOMBSTONE_*` and
  /// `__GC_PENDING_*` prefixes — so callers can distinguish "still points at me" from "rebound
  /// to someone else" or "tombstoned". Returns `null` ONLY when the reservation node truly
  /// does not exist; transient ZK failures throw {@link RuntimeException} so the caller can
  /// fail-closed instead of mis-classifying a flake as a rebind-loss.
  ///
  /// Used by `submitInsertInternal` to close the rebind-vs-create race: between a successful
  /// {@link #rebindRequestIdIfEquals} and a successful {@link #createStatement}, a concurrent retry
  /// for the same requestId can observe our reservation, see no manifest, treat our statementId as
  /// stale, and rebind to itself. Re-reading the reservation after createStatement detects that
  /// theft and lets the caller self-rollback.
  ///
  /// @return the raw statementId value, or `null` if the reservation does not exist
  /// @throws RuntimeException if the read fails for transient reasons (ZK down, session expired);
  ///   the caller must NOT treat this as a missing reservation
  @Nullable
  public String peekReservedStatementId(String tableNameWithType, String requestId) {
    if (requestId == null) {
      return null;
    }
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (existing == null) {
        return null;
      }
      return existing.getSimpleField(STATEMENT_ID_FIELD);
    } catch (Exception e) {
      LOGGER.warn("Transient ZK failure during peekReservedStatementId for table={} requestId={}",
          tableNameWithType, requestId, e);
      throw new RuntimeException(
          "ZK failure during peekReservedStatementId for requestId=" + requestId, e);
    }
  }

  /// Deletes reservation tombstones older than `olderThanMs` for the given table.
  ///
  /// {@link #releaseRequestIdIfEquals} writes a soft-delete tombstone instead of doing an
  /// unconditional remove (to avoid wiping a concurrent rebind). Without this sweep, those
  /// tombstones accumulate forever under `/INSERT_REQUEST_IDS/{table`}. This method is
  /// called from the cleanup sweep once per table per cycle.
  ///
  /// **Race-free protocol** (Helix has no version-checked remove):
  /// - Read the reservation with stat.
  /// - If it is a tombstone (prefix `__TOMBSTONE_`) older than the cutoff, attempt a
  ///       version-checked CAS-set to a `__GC_PENDING_` sentinel. A concurrent rebind that
  ///       beat us will have bumped the version, so our CAS fails and we skip.
  /// - {@link #rebindRequestIdIfEquals} refuses to rebind any record with the
  ///       `__GC_PENDING_` prefix, so once our CAS succeeds, no fresh rebind can land here.
  /// - Unconditionally remove the now-quiesced node.
  ///
  /// @return number of tombstones pruned
  public int pruneStaleReservationTombstones(String tableNameWithType, long olderThanMs) {
    String tablePath = REQUEST_IDS_PREFIX + "/" + tableNameWithType;
    List<String> children;
    try {
      children = _propertyStore.getChildNames(tablePath, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.warn("Failed to list reservation tombstones for table={}", tableNameWithType, e);
      return 0;
    }
    if (children == null || children.isEmpty()) {
      return 0;
    }
    long cutoffMs = System.currentTimeMillis() - olderThanMs;
    int pruned = 0;
    for (String requestId : children) {
      String path = tablePath + "/" + requestId;
      try {
        Stat stat = new Stat();
        ZNRecord record = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
        if (record == null) {
          continue;
        }
        String statementId = record.getSimpleField(STATEMENT_ID_FIELD);
        if (statementId == null) {
          continue;
        }
        boolean isTombstone = statementId.startsWith(TOMBSTONE_PREFIX);
        boolean isGcPending = statementId.startsWith(GC_PENDING_PREFIX);
        if (!isTombstone && !isGcPending) {
          continue;  /// active reservation, not a tombstone
        }
        /// GC_PENDING records are orphans left by a prior crashed sweep (CAS-set succeeded but
        /// the unconditional remove never ran). The rebind path refuses to reuse them, so they
        /// are safe to reap immediately regardless of mtime — waiting for retention would just
        /// delay cleanup of a record that no other path will ever touch.
        if (isTombstone && stat.getMtime() > cutoffMs) {
          continue;  /// tombstone still within retention; rebind path may still adopt it
        }
        String expectedSentinelId;
        if (isTombstone) {
          /// Version-checked claim: bumping the version blocks any concurrent rebind that holds the
          /// older version, and the GC_PENDING prefix tells future rebinds to back off.
          ZNRecord sentinel = new ZNRecord(requestId);
          expectedSentinelId = GC_PENDING_PREFIX + statementId.substring(TOMBSTONE_PREFIX.length());
          sentinel.setSimpleField(STATEMENT_ID_FIELD, expectedSentinelId);
          if (!_propertyStore.set(path, sentinel, stat.getVersion(), AccessOption.PERSISTENT)) {
            continue;  /// a concurrent rebind beat us; skip this entry
          }
        } else {
          /// Already a GC_PENDING orphan from a prior sweep that crashed mid-prune.
          expectedSentinelId = statementId;
        }
        /// Defense-in-depth before the unconditional remove: re-read and verify the content is
        /// still our GC_PENDING sentinel. If a future code path were to reuse a tombstoned
        /// reservation slot (e.g., a force-rebind admin tool that bypasses the prefix fence),
        /// this guard prevents the prune from wiping a fresh reservation. Today's protocol does
        /// not have such a path, so this is fragility insurance, not active correctness.
        Stat reverifyStat = new Stat();
        ZNRecord reverify = _propertyStore.get(path, reverifyStat, AccessOption.PERSISTENT);
        if (reverify == null) {
          /// Already gone (concurrent prune); count it as pruned for accounting.
          pruned++;
          continue;
        }
        if (!expectedSentinelId.equals(reverify.getSimpleField(STATEMENT_ID_FIELD))) {
          LOGGER.warn("Skipping prune for requestId={}: sentinel content changed after CAS-claim "
                  + "(expected {}, got {}). A future code path may have reused the slot.",
              requestId, expectedSentinelId, reverify.getSimpleField(STATEMENT_ID_FIELD));
          continue;
        }
        if (_propertyStore.remove(path, AccessOption.PERSISTENT)) {
          pruned++;
        }
      } catch (Exception e) {
        /// WARN-level so silent ZK failures during prune surface to operators. A persistent error
        /// here means tombstones accumulate forever; without the warning, an OPS team alerting on
        /// log levels would not see the issue until reservation-tree size triggered a separate
        /// alarm.
        LOGGER.warn("Skipping reservation tombstone prune for requestId={}: prune failed",
            requestId, e);
      }
    }
    if (pruned > 0) {
      LOGGER.info("Pruned {} reservation tombstone(s) older than {}ms for table={}",
          pruned, olderThanMs, tableNameWithType);
    }
    return pruned;
  }

  /// Finds a statement by requestId across all statements for a table. Used for idempotency
  /// checks (e.g., the rebind-loser path's `waitForWinnerManifest`).
  ///
  /// Returns `null` in three cases, all treated as "no live manifest available":
  /// - No reservation node exists for the requestId AND no manifest in the table carries
  ///   that requestId (legacy / orphaned data).
  /// - The reservation node exists but is a `__TOMBSTONE_` or `__GC_PENDING_`
  ///   sentinel — i.e., a prior owner released or queued the reservation for GC. The caller MUST
  ///   NOT fall back to scanning manifests in this case (a scan would silently surface a manifest
  ///   the operator considers reaped).
  /// - The reservation points at a statementId for which no manifest exists yet (the winner
  ///   has rebound but not yet called `createStatement`) — this is expected during a
  ///   rebind race; callers retry.
  ///
  /// @return the matching manifest, or null if not found / tombstoned / pending GC
  @Nullable
  public InsertStatementManifest findByRequestId(String tableNameWithType, String requestId) {
    /// First check the atomic reservation index
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      ZNRecord record = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (record != null) {
        String statementId = record.getSimpleField(STATEMENT_ID_FIELD);
        /// Tombstone / GC_PENDING entries do not point at a live manifest. Don't dereference them;
        /// the caller (typically waitForWinnerManifest in coordinator) treats null as "winner not
        /// yet visible, retry" rather than potentially returning a stale unrelated manifest.
        if (statementId != null && !statementId.startsWith(TOMBSTONE_PREFIX)
            && !statementId.startsWith(GC_PENDING_PREFIX)) {
          InsertStatementManifest manifest = getStatement(tableNameWithType, statementId);
          if (manifest != null) {
            return manifest;
          }
        } else {
          /// Reservation is tombstoned/pending-GC: skip the fallback scan and return null. A
          /// fallback scan here would silently surface a manifest the operator considers reaped.
          return null;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to lookup requestId reservation for requestId={}", requestId, e);
    }

    /// No active reservation found above — fall back to scanning manifests by requestId. Used by
    /// tests and as a defensive backstop for legacy data without an /INSERT_REQUEST_IDS entry.
    List<InsertStatementManifest> manifests = listStatements(tableNameWithType);
    for (InsertStatementManifest manifest : manifests) {
      if (requestId.equals(manifest.getRequestId())) {
        return manifest;
      }
    }
    return null;
  }

  /// Lists all table names that have at least one insert statement manifest in ZK.
  ///
  /// @return list of table names with type suffix (never null; may be empty)
  public List<String> listTablesWithStatements() {
    try {
      List<String> children = _propertyStore.getChildNames(INSERT_STATEMENTS_PREFIX, AccessOption.PERSISTENT);
      if (children == null || children.isEmpty()) {
        return List.of();
      }
      return children;
    } catch (Exception e) {
      LOGGER.error("Failed to list tables with insert statements", e);
      /// Throw rather than return empty: a silent empty list makes the cleanup sweep skip cleanup
      /// on transient ZK failures, accumulating stuck statements with no visible warning. The
      /// caller (cleanupAllTables) has a top-level catch that logs and retries on the next sweep.
      throw new RuntimeException("Failed to list tables with insert statements: " + e.getMessage(), e);
    }
  }

  /// Searches all tables for a statement with the given statementId.
  ///
  /// @return the manifest if found, or null
  @Nullable
  public InsertStatementManifest findStatementAcrossTables(String statementId) {
    List<String> tables = listTablesWithStatements();
    for (String table : tables) {
      InsertStatementManifest manifest = getStatement(table, statementId);
      if (manifest != null) {
        return manifest;
      }
    }
    return null;
  }

  private static String buildTablePath(String tableNameWithType) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType;
  }

  private static String buildPath(String tableNameWithType, String statementId) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType + "/" + statementId;
  }

  private static ZNRecord toZNRecord(InsertStatementManifest manifest)
      throws IOException {
    ZNRecord record = new ZNRecord(manifest.getStatementId());
    record.setSimpleField(MANIFEST_FIELD, manifest.toJsonString());
    record.setSimpleField(ENVELOPE_SCHEMA_VERSION_FIELD,
        Integer.toString(MANIFEST_ENVELOPE_SCHEMA_VERSION));
    return record;
  }

  private static InsertStatementManifest fromZNRecord(ZNRecord record)
      throws IOException {
    /// Forward-compat envelope check: refuse to silently parse records written by a future
    /// controller that bumped the envelope shape. Pre-versioning records (no field) are allowed.
    /// Read the envelope-specific field name to avoid shadowing with the body's own schemaVersion.
    String envelopeVersionStr = record.getSimpleField(ENVELOPE_SCHEMA_VERSION_FIELD);
    if (envelopeVersionStr != null) {
      try {
        int envelopeVersion = Integer.parseInt(envelopeVersionStr);
        if (envelopeVersion > MANIFEST_ENVELOPE_SCHEMA_VERSION) {
          throw new IOException("Unsupported manifest envelope schemaVersion=" + envelopeVersion
              + " (this controller supports up to " + MANIFEST_ENVELOPE_SCHEMA_VERSION
              + "). Upgrade controllers before reading newer manifests.");
        }
      } catch (NumberFormatException e) {
        throw new IOException("Malformed manifest envelope schemaVersion: " + envelopeVersionStr, e);
      }
    }
    String json = record.getSimpleField(MANIFEST_FIELD);
    if (json == null) {
      throw new IOException("Manifest ZNRecord is missing required field '" + MANIFEST_FIELD
          + "' for record id=" + record.getId());
    }
    return InsertStatementManifest.fromJsonString(json);
  }
}
