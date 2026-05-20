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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
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
/// them as [PartitionState#STALE] in [MaterializedViewRuntimeMetadata].
///
/// Events are accumulated per base table using a debounce window ([#DEFAULT_DEBOUNCE_DELAY_MS]ms).
/// Multiple segment changes within the window are merged into a single time range and
/// processed as one ZK read-modify-write operation, avoiding excessive ZK traffic during
/// batch ingestion or bulk segment operations.
///
/// Thread-safety: all public methods are thread-safe. The internal flush runs on a
/// single-threaded scheduler to serialize ZK writes per base table.
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
  private static final String MATERIALIZED_VIEW_DEFINITION_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
  private static final String MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX =
      MATERIALIZED_VIEW_DEFINITION_PARENT_PATH + "/";
  /// CAS retry budget for STALE-marking writes on the runtime znode. Sized to match the executor's
  /// `MAX_RUNTIME_UPDATE_ATTEMPTS` so a STALE marking is never silently dropped in favor of
  /// an executor's coverage advance under contention from up to `maxTasksPerBatch` (default 4,
  /// cap 1000) parallel completions. With ~5–25 ms jittered backoff per retry, 128 attempts cap
  /// total wait near 25 s.
  private static final int MAX_MARK_RETRIES = 128;



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
    _propertyStore.subscribeChildChanges(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, _definitionChangeListener);
    syncDefinitionDataSubscriptions(null);
    rebuildReverseIndex();
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
      markPartitionsDirtyWithRetry(viewTableName, range[0], range[1]);
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

  private void markPartitionsDirtyWithRetry(String viewTableName, long affectedStartMs, long affectedEndMs) {
    // Retry only on CAS conflict (markPartitionsDirty returns false). Any thrown exception
    // is a real error (corrupt znode, ZK unavailability, serialization bug, etc.) — retrying
    // 128× burns ~3 s of scheduler thread time and the operator never learns about the bug.
    // Fail loud after one occurrence so monitoring picks it up; the next base-table change
    // for the same MV will re-trigger this code path and try again from a known-bad-state log.
    for (int attempt = 0; attempt < MAX_MARK_RETRIES; attempt++) {
      try {
        if (markPartitionsDirty(viewTableName, affectedStartMs, affectedEndMs)) {
          return;
        }
        LOGGER.debug("CAS conflict on attempt {} for MV table: {}, retrying", attempt + 1, viewTableName);
      } catch (Exception e) {
        LOGGER.error("Failed to mark dirty partitions for MV table: {} on attempt {} due to a "
                + "non-CAS exception. Aborting retries — investigate the underlying ZK/serialization "
                + "issue. MV may serve stale data until the next base-table change re-triggers "
                + "consistency.", viewTableName, attempt + 1, e);
        return;
      }
      // Jittered backoff: 5–25 ms × 128 attempts ≤ ~3 s total. Prevents tight CAS-loop livelock
      // against the executor (which uses the same backoff window).
      try {
        Thread.sleep(5L + ThreadLocalRandom.current().nextInt(20));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while marking dirty for MV table: {}", viewTableName);
        return;
      }
    }
    LOGGER.error("Failed to mark dirty partitions for MV table: {} after {} CAS retries. "
        + "MV may serve stale data until the next base-table change re-triggers consistency.",
        viewTableName, MAX_MARK_RETRIES);
  }

  /// Marks overlapping VALID partitions as STALE in the MV runtime metadata.
  ///
  /// @return true if succeeded or nothing to mark; false if CAS failed (caller should retry)
  private boolean markPartitionsDirty(String viewTableName, long affectedStartMs, long affectedEndMs) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(viewTableName);
    Stat stat = new Stat();
    ZNRecord znRecord = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return true;
    }

    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);
    Map<Long, PartitionInfo> partitionInfos = runtime.getPartitions();

    long bucketMs = inferBucketMs(viewTableName, partitionInfos);

    // Cap affectedEndMs at watermarkMs.  No partition with partStart > watermarkMs can exist
    // (the writer invariant), so any bucket beyond that cannot be marked STALE anyway.  This
    // protects the bucket-known iteration loop below from a caller passing Long.MAX_VALUE
    // (full-range invalidation from notifyMaterializedViewConsistencyManager paths when
    // segment startTime/endTime is unknown), which would otherwise loop ~watermarkMs/bucketMs
    // times — orders of magnitude more than the number of real partitions.
    affectedEndMs = Math.min(affectedEndMs, runtime.getWatermarkMs());
    if (affectedEndMs < affectedStartMs) {
      LOGGER.debug("Affected range [{}, {}] is past watermarkMs ({}) for MV table: {}; nothing to mark",
          affectedStartMs, affectedEndMs, runtime.getWatermarkMs(), viewTableName);
      return true;
    }

    Map<Long, PartitionInfo> updatedInfos = new HashMap<>(partitionInfos);
    boolean anyChanged = false;
    int markedCount = 0;

    // bucketMs is required by the analyzer at MV-table creation, so it should always be > 0
    // here.  Fail loud if not — a missing bucket config means MV state is unrecoverable
    // without operator intervention; silent over-marking would only make it worse.
    Preconditions.checkState(bucketMs > 0,
        "MV table %s: bucketTimePeriod is required but bucketMs=%s; consistency manager cannot "
            + "mark partitions without a bucket size.  Repair the MV table config.",
        viewTableName, bucketMs);

    // Iterate every bucket [partStart, partStart+bucketMs) that overlaps the affected range
    // [affectedStartMs, affectedEndMs]. Two ranges overlap when start1 <= end2 AND end1 >= start2.
    // The first overlapping bucket has partStart = floorDiv(affectedStartMs, bucketMs) * bucketMs;
    // the last has partStart <= affectedEndMs (any bucket whose start is past affectedEndMs
    // cannot overlap because partStart > affectedEndMs >= affectedStartMs implies the bucket
    // starts after the affected range ends). floorDiv (instead of /) is used defensively so a
    // future caller passing negative affectedStartMs would still produce the correct floor.
    // Only flip existing VALID entries to STALE.  Absent buckets are NOT synthesized — under
    // Design C, a bucket's absence from the partition map already means "MV does not cover this
    // range", so the broker rewrite (PR 2) routes those queries to the base table.  Synthesizing
    // STALE entries for every uncovered bucket below `watermarkMs` would explode the znode size
    // on a full-range invalidation (~watermarkMs / bucketMs entries) without affecting routing
    // correctness — the bucket-iteration loop below stays O(affectedRange / bucketMs) but the
    // persisted map grows only with real partitions.
    long partStart = Math.floorDiv(affectedStartMs, bucketMs) * bucketMs;
    while (partStart <= affectedEndMs) {
      PartitionInfo info = updatedInfos.get(partStart);
      if (info != null && info.getState() == PartitionState.VALID) {
        updatedInfos.put(partStart, info.withState(PartitionState.STALE));
        anyChanged = true;
        markedCount++;
      }
      partStart += bucketMs;
    }

    if (!anyChanged) {
      LOGGER.debug("No VALID partitions to mark STALE for MV table: {} in range [{}, {}]",
          viewTableName, affectedStartMs, affectedEndMs);
      return true;
    }

    MaterializedViewRuntimeMetadata updated = new MaterializedViewRuntimeMetadata(
        runtime.getMaterializedViewTableNameWithType(), runtime.getWatermarkMs(), updatedInfos);

    // Route through `persist()` so every writer site is funnelled through the same
    // `validateForPersist` gate. Translate ONLY `CasConflictException` into the retry-loop
    // boolean — real validation failures (IllegalStateException / IllegalArgumentException
    // from validateForPersist) and underlying ZK errors propagate so they surface at the
    // task / log level rather than being silently retried 128×.
    try {
      MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, updated, stat.getVersion());
      LOGGER.info("Marked {} partition(s) STALE for MV table: {} (range [{}, {}])",
          markedCount, viewTableName, affectedStartMs, affectedEndMs);
      return true;
    } catch (MaterializedViewRuntimeMetadataUtils.CasConflictException e) {
      LOGGER.debug("CAS conflict marking partitions STALE for MV table: {} (range [{}, {}]); will retry",
          viewTableName, affectedStartMs, affectedEndMs);
      return false;
    }
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
