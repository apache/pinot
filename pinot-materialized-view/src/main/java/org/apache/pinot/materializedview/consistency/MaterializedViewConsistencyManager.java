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
package org.apache.pinot.materializedview.consistency;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewPartitionManager;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Manages MV partition consistency by reacting to base table segment changes.
///
/// When segments are added, replaced, or deleted in a base table, this manager
/// identifies which MV partitions overlap with the changed time range and marks
/// them as STALE in [MaterializedViewRuntimeMetadata].
///
/// Events are accumulated per base table using a debounce window ([#DEFAULT_DEBOUNCE_DELAY_MS]ms).
/// Multiple segment changes within the window are merged into a single time range and
/// processed as one ZK read-modify-write operation, avoiding excessive ZK traffic during
/// batch ingestion or bulk segment operations.
///
/// In addition to this event-driven path, a low-frequency periodic sweep
/// ([#sweepStrandedEmptyPartitions], every [#DEFAULT_EMPTY_SWEEP_INTERVAL_MS]ms by default)
/// re-evaluates in-coverage `VALID-empty` partitions against the current source and re-marks any
/// whose source window has regained segments STALE.  This backstops the narrow DELETE-vs-backfill
/// race the executor's commit guard ([MaterializedViewPartitionManager#clearValid]) cannot fully
/// close, so a stranded empty partition self-heals without waiting for a fresh base-table change.
///
/// Thread-safety: all public methods are thread-safe. The internal flush and the periodic sweep
/// both run on the same single-threaded scheduler, so ZK writes are serialized per base table.
///
/// <h3>Partition model (TIME-WINDOWED ONLY in PR 1)</h3>
///
/// This implementation assumes a single MV partition shape: time-windowed partitions of
/// uniform width `bucketTimePeriod`, keyed by `bucketStartMs`. Range-based notifications
/// (`onBaseTableDataChange(table, startMs, endMs)`) map directly to a contiguous bucket
/// sweep. Future fixed-partition (categorical) MVs will require:
///
///   - A `PartitionKind` discriminator on [MaterializedViewDefinitionMetadata].
///   - A non-`Long`-keyed partition map (or a `PartitionKey` abstraction) on
///     [MaterializedViewRuntimeMetadata].
///   - A separate STALE-marking path that does not iterate buckets by time.
///
/// See `pinot-materialized-view/DESIGN.md` for the migration plan. Callers that cannot
/// supply a tight time range (or that operate on fixed-partition MVs) should use
/// [#onBaseTableFullInvalidation] rather than passing sentinel time values — that entry
/// point will dispatch on `PartitionKind` once fixed-partition MVs land.
public class MaterializedViewConsistencyManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewConsistencyManager.class);

  /// Compile-time default debounce window for the consistency manager. Overridable per cluster
  /// (no restart) via `MaterializedViewTask.CLUSTER_CONFIG_KEY_CONSISTENCY_DEBOUNCE_MS`.
  static final long DEFAULT_DEBOUNCE_DELAY_MS = 5000;

  /// Compile-time default interval for the periodic VALID-empty re-evaluation sweep (5 minutes).
  /// Overridable per cluster (no restart) via
  /// `MaterializedViewTask.CLUSTER_CONFIG_KEY_CONSISTENCY_EMPTY_SWEEP_INTERVAL_MS`.  The sweep is a
  /// low-frequency safety net (the DELETE commit guard already handles the dominant race), so a
  /// coarse cadence keeps overhead negligible while still bounding stranded-bucket recovery time.
  static final long DEFAULT_EMPTY_SWEEP_INTERVAL_MS = 300_000;
  private static final String MATERIALIZED_VIEW_DEFINITION_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
  private static final String MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX =
      MATERIALIZED_VIEW_DEFINITION_PARENT_PATH + "/";

  /// Reverse index: rawBaseTableName → list of viewTableNameWithType.
  private final ConcurrentHashMap<String, List<String>> _baseTableToMaterializedViewTables = new ConcurrentHashMap<>();
  private final Object _reverseIndexLock = new Object();
  private final DefinitionChangeListener _definitionChangeListener = new DefinitionChangeListener();
  private final Set<String> _subscribedDefinitionPaths = ConcurrentHashMap.newKeySet();

  /// Debounce buffer: rawBaseTableName → [minAffectedStartMs, maxAffectedEndMs].
  private final ConcurrentHashMap<String, long[]> _pendingRanges = new ConcurrentHashMap<>();

  /// Per-base-table scheduled flush future for debounce cancellation/reset.
  private final ConcurrentHashMap<String, ScheduledFuture<?>> _pendingTimers = new ConcurrentHashMap<>();

  private final ScheduledExecutorService _scheduler;
  private volatile ZkHelixPropertyStore<ZNRecord> _propertyStore;
  /// Optional live cluster-config reader; controller wires this so debounce / retry caps can be
  /// overridden at runtime without restart. Null in unit tests; null lookup falls back to the
  /// compile-time defaults.
  private volatile Function<String, String> _clusterConfigReader;
  /// Centralized partition state-change DSL. Created once in [#init] with a config-reader
  /// indirection so a later [#setClusterConfigReader] call still propagates to the manager
  /// (the manager holds a final reference, but we close over the `_clusterConfigReader`
  /// field rather than its current value).
  private volatile MaterializedViewPartitionManager _partitionManager;

  public MaterializedViewConsistencyManager() {
    _scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "materialized-view-consistency-manager");
      t.setDaemon(true);
      return t;
    });
  }

  /// Wires the live cluster-config reader. Safe to call any time; nulls reset to default behavior.
  public void setClusterConfigReader(Function<String, String> clusterConfigReader) {
    _clusterConfigReader = clusterConfigReader;
  }

  /// Initializes the manager by scanning existing MV definition metadata to build the reverse index.
  /// Must be called after the PropertyStore is ready (after Controller startup).
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    // The lambda dereferences `_clusterConfigReader` on every call, so a later
    // setClusterConfigReader (controller wires this AFTER init in BaseControllerStarter)
    // takes effect for the manager's CAS-budget lookup without recreating the manager.
    _partitionManager = new MaterializedViewPartitionManager(propertyStore, configKey -> {
      Function<String, String> reader = _clusterConfigReader;
      return reader == null ? null : reader.apply(configKey);
    });
    _propertyStore.subscribeChildChanges(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, _definitionChangeListener);
    syncDefinitionDataSubscriptions(null);
    rebuildReverseIndex();
    scheduleNextEmptyPartitionSweep();
    LOGGER.info("MaterializedViewConsistencyManager initialized with {} base table mappings",
        _baseTableToMaterializedViewTables.size());
  }

  /// Shuts down the scheduler. Pending flushes are discarded.
  public void stop() {
    if (_propertyStore != null) {
      _propertyStore.unsubscribeChildChanges(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, _definitionChangeListener);
      for (String path : new ArrayList<>(_subscribedDefinitionPaths)) {
        _propertyStore.unsubscribeDataChanges(path, _definitionChangeListener);
      }
      _subscribedDefinitionPaths.clear();
    }
    _scheduler.shutdownNow();
    _pendingRanges.clear();
    _pendingTimers.clear();
    LOGGER.info("MaterializedViewConsistencyManager stopped");
  }

  /// Notifies that segments in a base table have changed. The affected time range is
  /// accumulated in a debounce buffer; multiple calls within [#DEFAULT_DEBOUNCE_DELAY_MS]ms
  /// for the same base table are merged into a single flush.
  ///
  /// This method is O(1) and non-blocking (just a map merge + timer reset).
  ///
  /// @param baseTableName raw table name without type suffix
  /// @param affectedStartMs earliest startTimeMs among changed segments
  /// @param affectedEndMs   latest endTimeMs among changed segments
  public void onBaseTableDataChange(String baseTableName, long affectedStartMs, long affectedEndMs) {
    if (affectedStartMs < 0 || affectedEndMs < 0) {
      LOGGER.debug("Skipping MV dirty marking for table {} with invalid time range [{}, {}]",
          baseTableName, affectedStartMs, affectedEndMs);
      return;
    }
    // Lock-free fast-path: skip the lock and merge entirely when no MV references this base
    // table. Every controller-side ZK segment write now flows through this method (see
    // `PinotHelixResourceManager.createSegmentZkMetadata`), so on clusters with zero MVs the
    // common case must not contend for `_reverseIndexLock`. ConcurrentHashMap.containsKey is
    // safe under concurrent registration / removal; the re-check inside the lock below
    // preserves correctness against the race with `onMaterializedViewTableDropped`.
    if (!_baseTableToMaterializedViewTables.containsKey(baseTableName)) {
      return;
    }
    // Hold _reverseIndexLock across the membership re-check, range merge, and timer reset so
    // a concurrent onMaterializedViewTableDropped cannot race in between the check and the
    // merge — without this guard, a dropped MV's base table could leave a pending range
    // buffered that the flush eventually no-ops on but still consumes scheduler time
    // proportional to the merge rate.
    synchronized (_reverseIndexLock) {
      if (!_baseTableToMaterializedViewTables.containsKey(baseTableName)) {
        return;
      }

      _pendingRanges.merge(baseTableName, new long[]{affectedStartMs, affectedEndMs},
          (existing, incoming) -> {
            existing[0] = Math.min(existing[0], incoming[0]);
            existing[1] = Math.max(existing[1], incoming[1]);
            return existing;
          });

      _pendingTimers.compute(baseTableName, (key, prev) -> {
        if (prev != null) {
          prev.cancel(false);
        }
        long debounceMs = MaterializedViewTaskUtils.readPositiveLongClusterConfigOrDefault(
            _clusterConfigReader,
            CommonConstants.MaterializedViewTask.CLUSTER_CONFIG_KEY_CONSISTENCY_DEBOUNCE_MS,
            DEFAULT_DEBOUNCE_DELAY_MS);
        return _scheduler.schedule(() -> flush(baseTableName), debounceMs, TimeUnit.MILLISECONDS);
      });
    }
  }

  /// Convenience entry point that marks every covered partition of every dependent MV STALE
  /// without requiring the caller to know which time range or partition key was affected.
  ///
  /// Used in two situations today:
  ///
  ///   - Time-windowed MVs whose base segments lack `startTimeMs` / `endTimeMs` metadata, so the
  ///     controller cannot compute a tight range. Conservative full-invalidation prevents leaking
  ///     stale VALID partitions; the consistency manager's per-MV watermark cap (`markPartitionsDirty`)
  ///     bounds the iteration cost so this remains cheap even on long-history MVs.
  ///   - Future fixed-partition MVs (see `DESIGN.md`), where the controller cannot in general
  ///     determine which categorical partition a base segment touched without inspecting its
  ///     data; full invalidation is the only honest signal at the consistency-manager layer.
  ///
  /// Implemented internally as a `[0, Long.MAX_VALUE)` invalidation routed through the same
  /// debounce + watermark-capped iteration path as range invalidations, so the on-disk effect
  /// (which partitions become STALE) is identical to an explicit "everything covered" range
  /// for time MVs. Callers should prefer this method over passing `Long.MAX_VALUE` directly to
  /// `onBaseTableDataChange` — the typed entry point keeps the intent visible at the call site
  /// and lets the implementation evolve (e.g. specialized fixed-partition path) without
  /// touching every caller.
  public void onBaseTableFullInvalidation(String baseTableName) {
    onBaseTableDataChange(baseTableName, 0L, Long.MAX_VALUE);
  }

  /// Returns the list of materialized-view tables (with `_OFFLINE` / `_REALTIME` suffix) that
  /// depend on `rawBaseTableName`. Returns an empty list when no MV references this base.
  ///
  /// Used by the controller to block base-table deletion when dependent MVs exist — operator
  /// must drop the MV(s) first to surface the dependency rather than orphan them.
  public List<String> getDependentMaterializedViews(String rawBaseTableName) {
    List<String> dependents = _baseTableToMaterializedViewTables.get(rawBaseTableName);
    if (dependents == null || dependents.isEmpty()) {
      return Collections.emptyList();
    }
    // Defensive copy so a caller doesn't accidentally mutate the live reverse index.
    return new ArrayList<>(dependents);
  }

  /// Updates the reverse index when a new MV table is created.
  public void onMaterializedViewTableCreated(String viewTableNameWithType, List<String> baseTables) {
    synchronized (_reverseIndexLock) {
      for (String baseTable : baseTables) {
        List<String> materializedViewTables =
            _baseTableToMaterializedViewTables.computeIfAbsent(baseTable, k -> new CopyOnWriteArrayList<>());
        if (!materializedViewTables.contains(viewTableNameWithType)) {
          materializedViewTables.add(viewTableNameWithType);
        }
      }
    }
    LOGGER.info("Registered MV table {} with base tables {}", viewTableNameWithType, baseTables);
  }

  /// Updates the reverse index when an MV table is dropped.
  public void onMaterializedViewTableDropped(String viewTableNameWithType, List<String> baseTables) {
    synchronized (_reverseIndexLock) {
      for (String baseTable : baseTables) {
        List<String> materializedViewTables = _baseTableToMaterializedViewTables.get(baseTable);
        if (materializedViewTables != null) {
          materializedViewTables.remove(viewTableNameWithType);
          if (materializedViewTables.isEmpty()) {
            _baseTableToMaterializedViewTables.remove(baseTable);
          }
        }
      }
    }
    LOGGER.info("Unregistered MV table {} from base tables {}", viewTableNameWithType, baseTables);
  }

  /// Processes all accumulated changes for a base table in one batch.
  /// For each affected MV, performs one ZK read + one ZK write on the runtime ZNode.
  @VisibleForTesting
  void flush(String baseTableName) {
    // Atomic snapshot-and-clear: drain `_pendingRanges` and `_pendingTimers` inside a single
    // `_pendingTimers.compute` lambda so a concurrent `onBaseTableDataChange` cannot land between
    // the two `remove()` calls. Without this, a debounce reschedule between the removes can drop
    // a pending range (the new future is removed from the map but the buffer entry it would have
    // flushed stays orphaned until the next event arrives).
    long[] range = drainPendingForFlush(baseTableName);
    if (range == null) {
      return;
    }

    List<String> materializedViewTables;
    synchronized (_reverseIndexLock) {
      materializedViewTables = _baseTableToMaterializedViewTables.get(baseTableName);
      if (materializedViewTables == null || materializedViewTables.isEmpty()) {
        return;
      }
      materializedViewTables = new ArrayList<>(materializedViewTables);
    }

    LOGGER.info("Flushing MV dirty marking for base table: {}, affected range: [{}, {}], "
        + "affected MV tables: {}", baseTableName, range[0], range[1], materializedViewTables);

    for (String viewTableName : materializedViewTables) {
      markStaleThroughManager(viewTableName, range[0], range[1]);
    }
  }

  /// Atomically removes both the pending range and the pending timer entry for a base table.
  /// Returns the range to flush, or `null` if there is nothing pending. The single
  /// `_pendingTimers.compute` lambda ensures `onBaseTableDataChange` cannot interleave
  /// between draining the buffer and clearing the timer — which would otherwise let a freshly
  /// scheduled timer be evicted from the map while its buffered range stays orphaned.
  private long[] drainPendingForFlush(String baseTableName) {
    long[][] drained = new long[1][];
    _pendingTimers.compute(baseTableName, (key, prev) -> {
      drained[0] = _pendingRanges.remove(baseTableName);
      // Returning null removes this map entry. Whether `prev` is the future this flush owns or a
      // newer one scheduled after our scheduler thread woke up, dropping it is safe — if a newer
      // event re-merges into _pendingRanges after this lambda returns, it will also call
      // _pendingTimers.compute and schedule a fresh timer.
      return null;
    });
    return drained[0];
  }

  /// Computes which buckets in `[affectedStartMs, affectedEndMs]` should be marked STALE for the
  /// given MV, then routes the flip through [MaterializedViewPartitionManager#markStale].
  ///
  /// Why two ZK reads: this method does one read-only snapshot fetch to derive `bucketMs` and
  /// the watermark cap (data the consistency manager owns the inference logic for); the manager
  /// does a second versioned fetch under its own CAS loop.  The cost is one extra GET per flush
  /// in exchange for routing every STALE-marking write through the same retry/backoff/budget
  /// engine the executor uses for APPEND/OVERWRITE — so a STALE marking can never be silently
  /// out-retried by a concurrent executor.
  ///
  /// VALID-only filtering (already-STALE buckets are no-ops) and absent-bucket-skip semantics
  /// are enforced inside [MaterializedViewPartitionManager#markStale] under the CAS lock, so a
  /// concurrent executor's APPEND/OVERWRITE between our snapshot read and the manager's CAS
  /// read cannot cause us to over-mark.
  private void markStaleThroughManager(String viewTableName, long affectedStartMs, long affectedEndMs) {
    List<Long> candidateBuckets;
    try {
      candidateBuckets = enumerateCandidateBuckets(viewTableName, affectedStartMs, affectedEndMs);
    } catch (Exception e) {
      LOGGER.error("Failed to enumerate candidate buckets for MV table: {} (range [{}, {}]); "
              + "MV may serve stale data until the next base-table change re-triggers consistency.",
          viewTableName, affectedStartMs, affectedEndMs, e);
      return;
    }
    if (candidateBuckets.isEmpty()) {
      return;
    }

    try {
      _partitionManager.markStale(viewTableName, candidateBuckets);
      LOGGER.info("Marked partitions STALE for MV table: {} (range [{}, {}], {} candidate bucket(s))",
          viewTableName, affectedStartMs, affectedEndMs, candidateBuckets.size());
    } catch (RuntimeException e) {
      // Includes CAS-budget exhaustion (manager wraps the last CasConflictException in a
      // RuntimeException) and validation failures.  Either way, log loud and move on — the next
      // base-table change for this MV will re-trigger this code path with a fresh snapshot.
      LOGGER.error("Failed to mark partitions STALE for MV table: {} (range [{}, {}]); "
              + "MV may serve stale data until the next base-table change re-triggers consistency.",
          viewTableName, affectedStartMs, affectedEndMs, e);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  Periodic VALID-empty re-evaluation sweep
  // ─────────────────────────────────────────────────────────────────────────────────────

  /// Self-rescheduling driver for the periodic VALID-empty sweep.  The interval is read lazily on
  /// every cycle (the cluster-config reader is wired AFTER [#init], so reading it here rather than
  /// at schedule-setup time honors live overrides) and the next cycle is rescheduled in a
  /// `finally` so a sweep that throws still keeps the cadence going.  No-op once the scheduler is
  /// shut down.
  private void scheduleNextEmptyPartitionSweep() {
    if (_scheduler.isShutdown()) {
      return;
    }
    long intervalMs = MaterializedViewTaskUtils.readPositiveLongClusterConfigOrDefault(
        _clusterConfigReader,
        CommonConstants.MaterializedViewTask.CLUSTER_CONFIG_KEY_CONSISTENCY_EMPTY_SWEEP_INTERVAL_MS,
        DEFAULT_EMPTY_SWEEP_INTERVAL_MS);
    try {
      _scheduler.schedule(() -> {
        try {
          sweepStrandedEmptyPartitions();
        } catch (Throwable t) {
          LOGGER.error("MV VALID-empty sweep failed; will retry on the next cycle", t);
        } finally {
          scheduleNextEmptyPartitionSweep();
        }
      }, intervalMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      LOGGER.debug("MV VALID-empty sweep not rescheduled; consistency manager is shutting down");
    }
  }

  /// One pass of the VALID-empty re-evaluation sweep across every registered MV.
  ///
  /// The DELETE commit guard ([MaterializedViewPartitionManager#clearValid]) re-reads the source
  /// inside its CAS loop and leaves a bucket STALE when a backfill is detected, closing the
  /// dominant race.  A vanishingly narrow residual remains: if a backfill's debounced STALE mark
  /// fires (as a no-op, while the bucket is still STALE) strictly between that read and the
  /// VALID-empty write, the bucket is stranded as a permanently-empty in-coverage partition that
  /// nothing re-marks until an unrelated later base-table change.  This sweep is the backstop — it
  /// re-evaluates in-coverage `VALID-empty` buckets against the current source and re-marks any
  /// whose source window has regained segments STALE, so the scheduler re-materializes them via
  /// OVERWRITE without waiting for a fresh base-table event.
  ///
  /// Runs on the same single-threaded scheduler as [#flush], so it never writes concurrently with
  /// a debounce flush; per-MV failures are isolated so one bad MV cannot abort the whole pass.
  @VisibleForTesting
  void sweepStrandedEmptyPartitions() {
    if (_propertyStore == null) {
      return;
    }
    // Snapshot (rawBaseTable, viewTableNameWithType) pairs under the lock, then process outside it
    // so the ZK reads below do not hold _reverseIndexLock.  Each pair maps an MV to one of its
    // source base tables; time-windowed MVs (the only shape in PR 1) have exactly one.
    List<String[]> baseTableAndViewPairs = new ArrayList<>();
    synchronized (_reverseIndexLock) {
      for (Map.Entry<String, List<String>> entry : _baseTableToMaterializedViewTables.entrySet()) {
        for (String viewTableName : entry.getValue()) {
          baseTableAndViewPairs.add(new String[]{entry.getKey(), viewTableName});
        }
      }
    }
    for (String[] pair : baseTableAndViewPairs) {
      String rawBaseTableName = pair[0];
      String viewTableName = pair[1];
      try {
        sweepStrandedEmptyPartitionsForView(viewTableName, rawBaseTableName);
      } catch (Exception e) {
        LOGGER.error("MV VALID-empty sweep failed for MV table: {} (base table: {}); "
            + "will retry on the next cycle", viewTableName, rawBaseTableName, e);
      }
    }
  }

  /// Re-evaluates one MV's in-coverage `VALID-empty` buckets against the current source and
  /// re-marks the stranded ones STALE.  Skips the (relatively expensive) source-segment read
  /// entirely when the MV has no in-coverage `VALID-empty` buckets — the common case.
  @VisibleForTesting
  void sweepStrandedEmptyPartitionsForView(String viewTableName, String rawBaseTableName) {
    Stat stat = new Stat();
    MaterializedViewRuntimeMetadata runtime =
        MaterializedViewRuntimeMetadataUtils.fetchWithVersion(_propertyStore, viewTableName, stat);
    if (runtime == null) {
      return;
    }
    Map<Long, PartitionInfo> partitions = runtime.getPartitions();
    long watermarkMs = runtime.getWatermarkMs();
    if (!hasInCoverageValidEmptyBucket(partitions, watermarkMs)) {
      return;
    }

    long bucketMs = inferBucketMs(viewTableName, partitions);
    if (bucketMs <= 0) {
      LOGGER.warn("MV table {}: cannot infer bucketMs for VALID-empty sweep; skipping this cycle",
          viewTableName);
      return;
    }
    String sourceTableWithType = resolveSourceTableWithType(rawBaseTableName);
    if (sourceTableWithType == null) {
      LOGGER.debug("MV table {}: source base table {} not found as OFFLINE or REALTIME; "
          + "skipping VALID-empty sweep this cycle", viewTableName, rawBaseTableName);
      return;
    }

    List<SegmentZKMetadata> sourceSegments =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, sourceTableWithType);
    List<Long> stranded = findStrandedEmptyBuckets(partitions, watermarkMs, sourceSegments, bucketMs);
    if (stranded.isEmpty()) {
      return;
    }

    LOGGER.info("MV table {}: re-marking {} stranded VALID-empty bucket(s) STALE — their source "
            + "window regained segments since the empty materialization: {}",
        viewTableName, stranded.size(), stranded);
    // The runtime + source reads above are an advisory snapshot; markStale re-fetches the runtime
    // under its own CAS loop and only flips buckets that are still VALID, so a concurrent executor
    // commit between our snapshot and the write cannot mismark — at worst a bucket already
    // re-materialized gets a redundant, idempotent OVERWRITE on the next scheduling cycle.
    try {
      _partitionManager.markStale(viewTableName, stranded);
    } catch (RuntimeException e) {
      LOGGER.error("MV table {}: failed to re-mark stranded VALID-empty buckets STALE; "
          + "will retry on the next sweep", viewTableName, e);
    }
  }

  /// True when at least one bucket is an in-coverage `VALID-empty` partition.  Cheap pre-filter so
  /// the sweep skips the source-segment read for MVs with nothing to re-evaluate.
  private static boolean hasInCoverageValidEmptyBucket(Map<Long, PartitionInfo> partitions, long watermarkMs) {
    for (Map.Entry<Long, PartitionInfo> entry : partitions.entrySet()) {
      if (isInCoverageValidEmpty(entry.getKey(), entry.getValue(), watermarkMs)) {
        return true;
      }
    }
    return false;
  }

  /// A bucket is an in-coverage `VALID-empty` partition iff it is VALID with a zero-segment
  /// fingerprint and starts strictly below the watermark (so it is genuinely covered, not the
  /// not-yet-materialized frontier).  `segmentCount == 0` is the canonical "empty" criterion used
  /// across the scheduler's DELETE dispatch and the executor's commit guard.
  private static boolean isInCoverageValidEmpty(long bucketStartMs, PartitionInfo info, long watermarkMs) {
    return info.getState() == PartitionState.VALID
        && info.getFingerprint().getSegmentCount() == 0
        && bucketStartMs < watermarkMs;
  }

  /// Returns the in-coverage `VALID-empty` buckets whose source window currently has overlapping
  /// segments — stranded empties that must be re-marked STALE so the scheduler re-materializes
  /// them.  Pure function of the snapshot inputs (no ZK), so the decision is unit-testable in
  /// isolation.  Buckets whose source is still empty are intentionally left untouched (re-marking
  /// them STALE would churn a no-op DELETE task every sweep).
  ///
  /// Reuses [MaterializedViewTaskUtils#computeWindowFingerprint] so this emptiness re-check is
  /// byte-for-byte the same overlap algorithm the scheduler's DELETE dispatch and the executor's
  /// commit guard use — there is deliberately no second overlap implementation to drift from.
  /// Cost is O(emptyBuckets * sourceSegments); the in-coverage `VALID-empty` set is expected to be
  /// small (gap windows + retention deletions), and the sweep is a low-frequency safety net.
  @VisibleForTesting
  static List<Long> findStrandedEmptyBuckets(Map<Long, PartitionInfo> partitions, long watermarkMs,
      List<SegmentZKMetadata> sourceSegments, long bucketMs) {
    List<Long> stranded = new ArrayList<>();
    for (Map.Entry<Long, PartitionInfo> entry : partitions.entrySet()) {
      long bucketStartMs = entry.getKey();
      if (!isInCoverageValidEmpty(bucketStartMs, entry.getValue(), watermarkMs)) {
        continue;
      }
      if (MaterializedViewTaskUtils.computeWindowFingerprint(
          sourceSegments, bucketStartMs, bucketStartMs + bucketMs).getSegmentCount() != 0) {
        stranded.add(bucketStartMs);
      }
    }
    return stranded;
  }

  /// Resolves the source base table to its `_OFFLINE` / `_REALTIME` form by probing which table
  /// config exists (OFFLINE first, then REALTIME), returning `null` when neither is present.
  private String resolveSourceTableWithType(String rawBaseTableName) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawBaseTableName);
    if (ZKMetadataProvider.getTableConfig(_propertyStore, offlineTableName) != null) {
      return offlineTableName;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawBaseTableName);
    if (ZKMetadataProvider.getTableConfig(_propertyStore, realtimeTableName) != null) {
      return realtimeTableName;
    }
    return null;
  }

  /// Returns the existing partition keys whose bucket overlaps
  /// `[floorDiv(affectedStartMs, bucketMs) * bucketMs, cappedEnd]` — the candidates the manager
  /// should consider marking STALE.  The manager filters this list down to entries that are still
  /// VALID inside its CAS loop; this method's job is to (a) bound the range by the current
  /// watermark (so a `Long.MAX_VALUE` full-invalidation does not iterate past real partitions) and
  /// (b) restrict to buckets that actually exist in the partition map.
  ///
  /// Returns an empty list when the runtime znode is missing or the affected range falls
  /// entirely past the watermark.
  private List<Long> enumerateCandidateBuckets(String viewTableName, long affectedStartMs, long affectedEndMs) {
    Stat stat = new Stat();
    MaterializedViewRuntimeMetadata runtime =
        MaterializedViewRuntimeMetadataUtils.fetchWithVersion(_propertyStore, viewTableName, stat);
    if (runtime == null) {
      return Collections.emptyList();
    }

    long bucketMs = inferBucketMs(viewTableName, runtime.getPartitions());
    // bucketMs is required by the analyzer at MV-table creation, so it should always be > 0
    // here.  Fail loud if not — a missing bucket config means MV state is unrecoverable
    // without operator intervention; silent over-marking would only make it worse.
    Preconditions.checkState(bucketMs > 0,
        "MV table %s: bucketTimePeriod is required but bucketMs=%s; consistency manager cannot "
            + "mark partitions without a bucket size.  Repair the MV table config.",
        viewTableName, bucketMs);

    // Cap affectedEndMs at watermarkMs.  No partition with partStart > watermarkMs can exist
    // (the writer invariant), so any bucket beyond that cannot be marked STALE anyway.  This
    // bounds the candidate range for a caller passing Long.MAX_VALUE (full-range invalidation from
    // notifyMaterializedViewConsistencyManager paths when segment startTime/endTime is unknown).
    long cappedEnd = Math.min(affectedEndMs, runtime.getWatermarkMs());
    if (cappedEnd < affectedStartMs) {
      LOGGER.debug("Affected range [{}, {}] is past watermarkMs ({}) for MV table: {}; nothing to mark",
          affectedStartMs, affectedEndMs, runtime.getWatermarkMs(), viewTableName);
      return Collections.emptyList();
    }

    // Select the EXISTING partition keys whose bucket [partStart, partStart+bucketMs) overlaps the
    // affected range [affectedStartMs, cappedEnd] — i.e. partStart in
    // [floorDiv(affectedStartMs, bucketMs) * bucketMs, cappedEnd].  Iterating the existing keys
    // (rather than enumerating every slot in the range) bounds this list to the partition count,
    // so a full-range invalidation with a small bucketMs and a large watermark horizon cannot
    // allocate a list proportional to watermarkMs/bucketMs.  Absent buckets are intentionally NOT
    // synthesized: under Design C a bucket's absence already means "MV does not cover this range"
    // and the broker routes those queries to the base; the manager's markStale would no-op them
    // anyway, so they are simply not candidates.  floorDiv (instead of /) defends a future caller
    // passing a negative affectedStartMs.
    long firstBucketStartMs = Math.floorDiv(affectedStartMs, bucketMs) * bucketMs;
    List<Long> buckets = new ArrayList<>();
    for (long partStart : runtime.getPartitions().keySet()) {
      if (partStart >= firstBucketStartMs && partStart <= cappedEnd) {
        buckets.add(partStart);
      }
    }
    return buckets;
  }

  /// Infers the bucket size in millis for the given MV table. First tries to read it from
  /// the MV table's task config; falls back to computing the GCD of consecutive partition
  /// start times from the partitionInfos map.
  private long inferBucketMs(String viewTableName, Map<Long, PartitionInfo> partitionInfos) {
    long fromConfig = readBucketMsFromTableConfig(viewTableName);
    if (fromConfig > 0) {
      return fromConfig;
    }
    return inferBucketMsFromPartitions(partitionInfos);
  }

  private long readBucketMsFromTableConfig(String viewTableName) {
    try {
      String viewTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(viewTableName);
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, viewTableWithType);
      if (tableConfig == null) {
        return -1;
      }
      TableTaskConfig taskConfig = tableConfig.getTaskConfig();
      if (taskConfig == null) {
        return -1;
      }
      Map<String, String> viewTaskConfigs =
          taskConfig.getConfigsForTaskType(CommonConstants.MaterializedViewTask.TASK_TYPE);
      if (viewTaskConfigs == null) {
        return -1;
      }
      String bucketPeriod = viewTaskConfigs.get(CommonConstants.MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
      if (bucketPeriod == null || bucketPeriod.isEmpty()) {
        return -1;
      }
      return TimeUtils.convertPeriodToMillis(bucketPeriod);
    } catch (Exception e) {
      LOGGER.debug("Failed to read bucket config for MV table: {}", viewTableName, e);
      return -1;
    }
  }

  /// Computes bucket size by finding the minimum gap between sorted partition start times.
  private long inferBucketMsFromPartitions(Map<Long, PartitionInfo> partitionInfos) {
    if (partitionInfos.size() < 2) {
      return -1;
    }
    List<Long> sortedKeys = new ArrayList<>(partitionInfos.keySet());
    sortedKeys.sort(Long::compareTo);

    long minGap = Long.MAX_VALUE;
    for (int i = 1; i < sortedKeys.size(); i++) {
      long gap = sortedKeys.get(i) - sortedKeys.get(i - 1);
      if (gap > 0) {
        minGap = Math.min(minGap, gap);
      }
    }
    return minGap == Long.MAX_VALUE ? -1 : minGap;
  }

  /// Scans all MV definition ZNodes to build the baseTable → materializedViewTable reverse index.
  private void rebuildReverseIndex() {
    String defBasePath = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    Map<String, List<String>> rebuiltIndex = new HashMap<>();
    List<String> children = _propertyStore.getChildNames(defBasePath, AccessOption.PERSISTENT);
    if (children == null || children.isEmpty()) {
      synchronized (_reverseIndexLock) {
        _baseTableToMaterializedViewTables.clear();
      }
      LOGGER.debug("No MV definition metadata found during reverse index rebuild");
      return;
    }

    for (String viewTableName : children) {
      try {
        // Verify the MV's TableConfig still exists and is an MV. A previous DROP that
        // succeeded at removing the TableConfig but failed best-effort znode cleanup would
        // leave a stale definition znode here; re-registering it would resurrect a ghost
        // MV that source-table updates would chase. Skip the orphan and surface a WARN so
        // operators can clean up the stranded znode.
        TableConfig viewConfig = ZKMetadataProvider.getTableConfig(_propertyStore, viewTableName);
        if (viewConfig == null || !viewConfig.isMaterializedView()) {
          LOGGER.warn("Skipping orphan MV definition znode '{}': underlying TableConfig is "
              + "missing or no longer an MV. Operator should DELETE /materializedViewDefinitions "
              + "to remove the stale znode.", viewTableName);
          continue;
        }
        String fullPath = defBasePath + "/" + viewTableName;
        ZNRecord record = _propertyStore.get(fullPath, null, AccessOption.PERSISTENT);
        if (record == null) {
          continue;
        }
        MaterializedViewDefinitionMetadata definition = MaterializedViewDefinitionMetadata.fromZNRecord(record);
        for (String baseTable : definition.getBaseTables()) {
          rebuiltIndex.computeIfAbsent(baseTable, k -> new ArrayList<>())
              .add(viewTableName);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to load MV definition for: {}", viewTableName, e);
      }
    }
    synchronized (_reverseIndexLock) {
      _baseTableToMaterializedViewTables.clear();
      for (Map.Entry<String, List<String>> entry : rebuiltIndex.entrySet()) {
        _baseTableToMaterializedViewTables.put(entry.getKey(), new CopyOnWriteArrayList<>(entry.getValue()));
      }
    }
    LOGGER.info("Rebuilt MV reverse index: {}", _baseTableToMaterializedViewTables);
  }

  private void syncDefinitionDataSubscriptions(List<String> children) {
    List<String> definitionChildren = children != null ? children
        : _propertyStore.getChildNames(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, AccessOption.PERSISTENT);
    Set<String> currentPaths = new HashSet<>();
    if (definitionChildren != null) {
      for (String viewTableName : definitionChildren) {
        String path = MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + viewTableName;
        currentPaths.add(path);
        if (_subscribedDefinitionPaths.add(path)) {
          _propertyStore.subscribeDataChanges(path, _definitionChangeListener);
        }
      }
    }
    for (String path : new ArrayList<>(_subscribedDefinitionPaths)) {
      if (!currentPaths.contains(path) && _subscribedDefinitionPaths.remove(path)) {
        _propertyStore.unsubscribeDataChanges(path, _definitionChangeListener);
      }
    }
  }

  private class DefinitionChangeListener implements IZkChildListener, IZkDataListener {
    @Override
    public void handleChildChange(String path, List<String> children) {
      syncDefinitionDataSubscriptions(children);
      rebuildReverseIndex();
    }

    @Override
    public void handleDataChange(String path, Object data) {
      rebuildReverseIndex();
    }

    @Override
    public void handleDataDeleted(String path) {
      if (_subscribedDefinitionPaths.remove(path)) {
        _propertyStore.unsubscribeDataChanges(path, this);
      }
      rebuildReverseIndex();
    }
  }
}
