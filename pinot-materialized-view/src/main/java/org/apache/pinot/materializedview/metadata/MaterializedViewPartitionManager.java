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
package org.apache.pinot.materializedview.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskUtils;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Single source of truth for every per-partition state transition on the
/// [MaterializedViewRuntimeMetadata] runtime znode.
///
/// <h3>Why this class exists</h3>
///
/// Before this manager landed, the runtime znode was mutated by three separate sites — the
/// minion executor (APPEND / OVERWRITE / DELETE task commits), the scheduler (false-positive
/// STALE reverts), and the consistency manager (VALID → STALE markings).  Each site shipped
/// its own version-checked CAS loop, with subtly different retry budgets, backoff jitter,
/// and exception-classification rules.  Subsequent state-machine changes (e.g. extending the
/// VACANT → STALE synthesize for in-coverage backfill, or revoking the VALID-empty state)
/// would have required coordinated edits at all three sites — exactly the failure mode that
/// caused the earlier `computeWindowFingerprint` duplication scare.
///
/// This class consolidates all per-partition mutations behind a state-change DSL.  Each
/// public method maps to exactly one operation in the per-partition state machine; the
/// ZK CAS retry loop, the writer-side `validateForPersist` invariant gate, and the watermark
/// recompute on APPEND all live behind the public API.  Callers express intent
/// (`appendValid`, `markStale`, ...) and never see the persistence machinery.
///
/// <h3>Architecture</h3>
///
/// Three layers, deliberately separated:
///
///   - **Public state-change DSL** — one method per state-machine op.  Each method has a
///     well-defined precondition and postcondition (see per-method javadoc) and selects the
///     appropriate retry profile.  Callers never construct `PartitionInfo` directly.
///   - **Private CRUD primitives** — `addPartitionInfo` / `updatePartitionInfo` /
///     `removePartitionInfo`.  Each enforces a structural invariant on the in-memory map
///     (e.g. "cannot add an already-existing entry") that fail-loud on violation, surfacing
///     races as observable exceptions instead of silent overwrites.
///   - **Single CAS engine** — `applyMutation` runs a caller-supplied mutator under the
///     fetch / mutate / version-checked-write loop, with retry classification:
///       - [MaterializedViewRuntimeMetadataUtils.CasConflictException] → re-fetch + retry
///       - other [ZkException] (transport / session) → log warn + retry
///       - [IllegalStateException] / [IllegalArgumentException] (validateForPersist) →
///         propagate (fail-fast, no retry)
///       - any other exception → propagate
///
/// <h3>Retry profile</h3>
///
/// Two profiles, calibrated to op semantics:
///
///   - **Critical** ([#DEFAULT_CRITICAL_MAX_ATTEMPTS] = 128, cluster-tunable via
///     `pinot.materialized.view.executor.runtime.update.max.attempts`) — used by every
///     state-changing op except `revertValid`.  Failure forces a minion task retry at the
///     Helix level, which is far more expensive than a CAS retry, so the budget is sized
///     to absorb realistic contention with `maxTasksPerBatch` parallel writers.
///   - **Revert** ([#REVERT_MAX_ATTEMPTS] = 8) — used by `revertValid` only.  Failure
///     means we did not avoid one OVERWRITE task, which the next scheduling cycle will
///     either run for real or revert again.  Spending 128 retries to save one task is
///     wasteful.
///
/// <h3>Concurrency</h3>
///
/// The manager is thread-safe and stateless above the ZK property store handle.  Concurrent
/// callers contending on the same MV runtime znode are serialized by ZK CAS; one thread's
/// commit invalidates the other's snapshot, the loser re-fetches and re-runs its mutator.
/// Different MVs map to different znodes — no cross-MV contention.
///
/// <h3>Scope discipline</h3>
///
/// This class manages exactly the partition map and `watermarkMs` field of
/// [MaterializedViewRuntimeMetadata].  It MUST NOT acquire other znodes
/// (e.g. [MaterializedViewDefinitionMetadata]), call source-table or table-config services,
/// or grow a method count beyond the public DSL listed below.  See `red lines` in the PR
/// description.
public final class MaterializedViewPartitionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewPartitionManager.class);

  /// Compile-time default for the critical retry budget.  Overridable per cluster (no
  /// restart) via `pinot.materialized.view.executor.runtime.update.max.attempts`.  Sized so
  /// `maxTasksPerBatch` parallel APPEND completions cannot exhaust the budget under realistic
  /// contention.  Reused by ConsistencyMgr-style mark ops so a STALE marking is never
  /// silently dropped.
  static final int DEFAULT_CRITICAL_MAX_ATTEMPTS = 128;

  /// Compile-time retry budget for `revertValid` only.  Failure is recovered by the next
  /// scheduling cycle (the partition stays STALE and is retried), so a small budget is
  /// appropriate.  Not cluster-tunable — operationally, raising this would not buy
  /// correctness, only delay.
  static final int REVERT_MAX_ATTEMPTS = 8;

  /// Backoff envelope for the critical profile.  Total wait at the cap with worst-case
  /// jitter is about `128 * (50 + 150) = 25.6 s`, well within the implicit per-task timeout.
  private static final long CRITICAL_BACKOFF_BASE_MS = 50L;
  private static final int CRITICAL_BACKOFF_JITTER_MS = 150;

  /// Backoff envelope for the revert profile.  Total wait at the cap is about
  /// `8 * (5 + 20) = 200 ms`.  Tight CAS-loop livelock is the failure to defend against;
  /// scheduler thread blocking is the failure to avoid.
  private static final long REVERT_BACKOFF_BASE_MS = 5L;
  private static final int REVERT_BACKOFF_JITTER_MS = 20;

  private final HelixPropertyStore<ZNRecord> _propertyStore;

  /// Optional live cluster-config reader used to override [#DEFAULT_CRITICAL_MAX_ATTEMPTS]
  /// at runtime.  Null in unit tests; null reader falls back to the compile-time default.
  @Nullable
  private final Function<String, String> _clusterConfigReader;

  public MaterializedViewPartitionManager(HelixPropertyStore<ZNRecord> propertyStore,
      @Nullable Function<String, String> clusterConfigReader) {
    Preconditions.checkArgument(propertyStore != null, "propertyStore must not be null");
    _propertyStore = propertyStore;
    _clusterConfigReader = clusterConfigReader;
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  Public state-change DSL
  // ─────────────────────────────────────────────────────────────────────────────────────

  /// APPEND op: VACANT → VALID(fp).  Invoked by the minion executor at task-commit time
  /// after a successful materialization of a window that was previously absent from the
  /// partition map.
  ///
  /// Side effects (all in a single CAS write):
  ///
  ///   - Inserts a new `PartitionInfo(VALID, fp, now)` at `windowStartMs`.
  ///   - Advances `watermarkMs` to the highest contiguous VALID upper bound starting at
  ///     the prior watermark, derived via
  ///     [MaterializedViewTaskUtils#computeContiguousUpperMs] over the resulting map.
  ///     The bucket size used for the walk is `windowEndMs - windowStartMs`.
  ///
  /// Watermark advancement is bundled with the bucket transition because watermark is a
  /// derived field of the map state and must stay consistent under concurrent writers.
  /// Splitting them across two CAS writes would create a transient window where the map
  /// has the new bucket but the watermark is stale.
  ///
  /// Strict precondition: `windowStartMs` must NOT be present in the map.  A violation
  /// (raced double-dispatch from the scheduler, mid-flight task replay) throws
  /// [IllegalStateException] inside the CAS loop, which propagates without retry — the
  /// scheduler dispatch invariant guarantees one APPEND per bucket per cycle, so this
  /// firing indicates a real bug, not transient contention.
  ///
  /// Retry profile: critical (cluster-tunable budget).
  public void appendValid(String tableNameWithType, long windowStartMs, long windowEndMs,
      PartitionFingerprint fingerprint) {
    Preconditions.checkArgument(fingerprint != null, "fingerprint must not be null");
    Preconditions.checkArgument(windowEndMs > windowStartMs,
        "Invalid window: windowEndMs (%s) <= windowStartMs (%s) for table %s",
        windowEndMs, windowStartMs, tableNameWithType);
    long bucketMs = windowEndMs - windowStartMs;
    applyMutation(tableNameWithType, current -> {
      Preconditions.checkState(current != null,
          "appendValid called before MV runtime znode was initialized for table: %s "
              + "(cold-start path skipped?)", tableNameWithType);
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      addPartitionInfo(updated, windowStartMs,
          new PartitionInfo(PartitionState.VALID, fingerprint, System.currentTimeMillis()));
      long newWatermarkMs = MaterializedViewTaskUtils.computeContiguousUpperMs(
          current.getWatermarkMs(), updated, bucketMs);
      LOGGER.info("appendValid: table={} bucket={} watermarkMs {}->{} (partitions={})",
          tableNameWithType, windowStartMs, current.getWatermarkMs(), newWatermarkMs, updated.size());
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), newWatermarkMs, updated);
    }, criticalMaxAttempts(), CRITICAL_BACKOFF_BASE_MS, CRITICAL_BACKOFF_JITTER_MS);
  }

  /// OVERWRITE op: STALE → VALID(fp).  Invoked by the minion executor at task-commit time
  /// after re-materializing a STALE bucket whose source-side fingerprint has actually
  /// changed.  Updates the in-place fingerprint and refreshes `lastRefreshTime`; watermark
  /// is unchanged because OVERWRITE only refreshes existing coverage.
  ///
  /// Strict precondition: `windowStartMs` must be present and STALE.  A non-STALE entry
  /// indicates the scheduler dispatch invariant was violated and throws
  /// [IllegalStateException] without retry.
  ///
  /// Retry profile: critical.
  public void refreshValid(String tableNameWithType, long windowStartMs,
      PartitionFingerprint fingerprint) {
    Preconditions.checkArgument(fingerprint != null, "fingerprint must not be null");
    applyMutation(tableNameWithType, current -> {
      Preconditions.checkState(current != null,
          "refreshValid called before MV runtime znode was initialized for table: %s",
          tableNameWithType);
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      PartitionInfo existing = updated.get(windowStartMs);
      Preconditions.checkState(existing != null && existing.getState() == PartitionState.STALE,
          "refreshValid expects bucket %s to be STALE for table %s, got: %s",
          windowStartMs, tableNameWithType, existing);
      updatePartitionInfo(updated, windowStartMs,
          new PartitionInfo(PartitionState.VALID, fingerprint, System.currentTimeMillis()));
      LOGGER.info("refreshValid: table={} bucket={} STALE->VALID (fp={})",
          tableNameWithType, windowStartMs, fingerprint);
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), current.getWatermarkMs(), updated);
    }, criticalMaxAttempts(), CRITICAL_BACKOFF_BASE_MS, CRITICAL_BACKOFF_JITTER_MS);
  }

  /// DELETE op: STALE → VALID(EMPTY), guarded by a commit-time source-emptiness re-check.
  /// Invoked by the minion executor at task-commit time after retention-deleting MV segments
  /// for a window whose source data is now empty.  Persists the `VALID + PartitionFingerprint.EMPTY`
  /// shape rather than removing the entry — the coverage model treats every processed bucket as
  /// "tracked", so a later base-table backfill flows through the standard VALID → STALE →
  /// OVERWRITE cycle.
  ///
  /// <p><b>Commit-time backfill guard.</b> The scheduler dispatched DELETE because the source
  /// window had zero overlapping segments, but a backfill can land between dispatch and this
  /// commit.  `sourceFingerprintSupplier` recomputes the source fingerprint for the window from
  /// live segment ZK metadata and is invoked <i>inside</i> the CAS mutator, so emptiness is
  /// re-evaluated on every attempt immediately before the version-checked write — not against a
  /// snapshot captured before the (under contention, possibly multi-second) retry loop.  If the
  /// supplier reports the source is no longer empty (`getSegmentCount() != 0`), the op is a no-op
  /// and the bucket is intentionally left STALE so the next scheduling cycle re-materializes it
  /// via OVERWRITE.  Writing `VALID + EMPTY` over a now-non-empty source would silently drop the
  /// backfilled rows: the consistency manager's create-segment event for the backfill is a no-op
  /// while the bucket is still STALE, so nothing would re-mark the resulting `VALID-empty` entry
  /// as STALE — stranding it as a permanently-empty in-coverage bucket the broker would route
  /// empty results from.
  ///
  /// <p>Full atomicity across the two ZooKeeper subsystems (source-table segment metadata and the
  /// MV runtime znode) is not achievable, so a narrow residual window remains within a single CAS
  /// attempt: if a backfill segment appears AND the consistency manager's (debounced) STALE mark
  /// for it fires — as a no-op, because the bucket is still STALE — strictly between this attempt's
  /// supplier read and its runtime-znode write, the following `VALID-empty` write strands the
  /// bucket, since that backfill's only mark was already consumed.  This residual is NOT guaranteed
  /// to self-heal (only a later, unrelated base-table change in the window would recover it);
  /// fully closing it requires the consistency manager to also re-evaluate in-coverage
  /// `VALID-empty` buckets against the source — see the open-question note below.  The in-mutator
  /// re-read still eliminates the dominant, contention-driven window that the pre-fix
  /// snapshot-before-loop left wide open (the snapshot could be stale for the entire CAS retry
  /// budget, up to several seconds).
  ///
  /// <p><b>Scope note:</b> the supplier is a caller-provided callback — the manager invokes it but
  /// keeps no compile-time dependency on source-table services, preserving the scope discipline
  /// documented at the class level.
  ///
  /// <p><b>Design context (open question):</b> The continued existence of the `VALID-empty`
  /// shape is under review.  An alternative model removes the entry entirely on DELETE
  /// (treating empty buckets as VACANT), which requires extending
  /// [MaterializedViewTaskUtils#computeContiguousUpperMs] to walk past VACANT buckets and
  /// extending the consistency manager to synthesize STALE entries for in-coverage VACANT
  /// buckets on backfill.  That same consistency-manager extension would also close the residual
  /// backfill window described above, by re-marking in-coverage `VALID-empty` buckets STALE.
  /// Until that design lands, this method preserves today's `VALID-empty` semantics so the
  /// scheduler's contiguous-VALID watermark walk continues to work without modification.
  ///
  /// Strict precondition: `windowStartMs` must be present and STALE.  Non-STALE entries
  /// indicate the scheduler dispatch invariant was violated; fail-loud surfaces the bug.
  ///
  /// Retry profile: critical.
  public void clearValid(String tableNameWithType, long windowStartMs,
      Supplier<PartitionFingerprint> sourceFingerprintSupplier) {
    Preconditions.checkArgument(sourceFingerprintSupplier != null,
        "sourceFingerprintSupplier must not be null");
    applyMutation(tableNameWithType, current -> {
      Preconditions.checkState(current != null,
          "clearValid called before MV runtime znode was initialized for table: %s",
          tableNameWithType);
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      PartitionInfo existing = updated.get(windowStartMs);
      Preconditions.checkState(existing != null && existing.getState() == PartitionState.STALE,
          "clearValid expects bucket %s to be STALE for table %s, got: %s",
          windowStartMs, tableNameWithType, existing);
      // Backfill guard: re-read the source on THIS attempt and leave the bucket STALE (no-op) if
      // it is no longer empty, so the next scheduling cycle re-materializes it via OVERWRITE
      // instead of writing a permanently-empty in-coverage bucket.  `segmentCount == 0` is the
      // canonical "empty source window" criterion — identical to the scheduler's DELETE dispatch
      // test (MaterializedViewTaskScheduler#tryHandleStalePartition) and equivalent to
      // PartitionFingerprint.EMPTY (computeWindowFingerprint maps an empty overlap to EMPTY).
      // See the method javadoc for the full rationale.
      PartitionFingerprint sourceFingerprint = sourceFingerprintSupplier.get();
      if (sourceFingerprint.getSegmentCount() != 0) {
        LOGGER.info("clearValid: bucket {} for table {} has a backfilled source ({} overlapping "
                + "segment(s)); leaving STALE for OVERWRITE instead of writing VALID-empty",
            windowStartMs, tableNameWithType, sourceFingerprint.getSegmentCount());
        return null;
      }
      updatePartitionInfo(updated, windowStartMs,
          new PartitionInfo(PartitionState.VALID, PartitionFingerprint.EMPTY,
              System.currentTimeMillis()));
      LOGGER.info("clearValid: table={} bucket={} STALE->VALID-empty",
          tableNameWithType, windowStartMs);
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), current.getWatermarkMs(), updated);
    }, criticalMaxAttempts(), CRITICAL_BACKOFF_BASE_MS, CRITICAL_BACKOFF_JITTER_MS);
  }

  /// STALE_REVERT op: STALE → VALID, fingerprint and `lastRefreshTime` preserved.  Invoked
  /// by the scheduler after a precise fingerprint comparison concludes that a STALE marking
  /// was a false positive (the source did not actually change in the affected window).  The
  /// underlying data was not re-materialized, so neither fingerprint nor `lastRefreshTime`
  /// move.
  ///
  /// Lenient precondition: if the bucket is no longer STALE on retry (either already
  /// reverted by a concurrent write or the consistency manager re-marked it for a new
  /// reason), the call exits successfully without writing — the desired transition is
  /// already done or no longer applicable.
  ///
  /// Retry profile: revert (small budget; failure is recovered by the next scheduling
  /// cycle).
  public void revertValid(String tableNameWithType, long windowStartMs) {
    applyMutation(tableNameWithType, current -> {
      if (current == null) {
        LOGGER.debug("revertValid: runtime znode missing for table: {}; skipping",
            tableNameWithType);
        return null;
      }
      PartitionInfo existing = current.getPartitions().get(windowStartMs);
      if (existing == null || existing.getState() != PartitionState.STALE) {
        LOGGER.info("revertValid: bucket {} for table {} is not STALE (got: {}); skipping",
            windowStartMs, tableNameWithType, existing);
        return null;
      }
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      updatePartitionInfo(updated, windowStartMs, existing.withState(PartitionState.VALID));
      LOGGER.info("revertValid: table={} bucket={} STALE->VALID (false-positive, fp={})",
          tableNameWithType, windowStartMs, existing.getFingerprint());
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), current.getWatermarkMs(), updated);
    }, REVERT_MAX_ATTEMPTS, REVERT_BACKOFF_BASE_MS, REVERT_BACKOFF_JITTER_MS);
  }

  /// MARK_STALE op (single-bucket): VALID → STALE.  Invoked by the consistency manager (and
  /// future manual REFRESH commands) when base-table data has changed in a way that affects
  /// `windowStartMs`.  Fingerprint and `lastRefreshTime` are preserved so the scheduler's
  /// false-positive revert can compare against the prior snapshot.
  ///
  /// Lenient: if the bucket is absent or already STALE, the call is a no-op.  Today the
  /// in-coverage VACANT case is a deliberate skip (matching the prior consistency manager
  /// behavior); a future change will synthesize a STALE entry for in-coverage VACANT
  /// buckets to fix the backfill silent-data-loss case.
  ///
  /// Retry profile: critical.
  public void markStale(String tableNameWithType, long windowStartMs) {
    markStale(tableNameWithType, java.util.Collections.singletonList(windowStartMs));
  }

  /// MARK_STALE op (batch): for each bucket in the input collection, flip VALID → STALE.
  /// All updates are applied in a single CAS write, so a range invalidation that spans
  /// many buckets does not amplify CAS contention with concurrent executors.
  ///
  /// Per-bucket lenient: absent buckets and already-STALE buckets are no-ops within the
  /// same call.  Same future synthesize note as the single-bucket overload.
  ///
  /// Retry profile: critical.
  public void markStale(String tableNameWithType, Collection<Long> windowStartMsBuckets) {
    Preconditions.checkArgument(windowStartMsBuckets != null,
        "windowStartMsBuckets must not be null for table: %s", tableNameWithType);
    if (windowStartMsBuckets.isEmpty()) {
      return;
    }
    applyMutation(tableNameWithType, current -> {
      if (current == null) {
        LOGGER.debug("markStale: runtime znode missing for table: {}; nothing to mark",
            tableNameWithType);
        return null;
      }
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      int markedCount = 0;
      for (long bucket : windowStartMsBuckets) {
        PartitionInfo existing = updated.get(bucket);
        if (existing != null && existing.getState() == PartitionState.VALID) {
          updatePartitionInfo(updated, bucket, existing.withState(PartitionState.STALE));
          markedCount++;
        }
      }
      if (markedCount == 0) {
        LOGGER.debug("markStale: no VALID partitions to mark for table {} (buckets={})",
            tableNameWithType, windowStartMsBuckets.size());
        return null;
      }
      LOGGER.info("markStale: table={} marked {}/{} bucket(s) STALE",
          tableNameWithType, markedCount, windowStartMsBuckets.size());
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), current.getWatermarkMs(), updated);
    }, criticalMaxAttempts(), CRITICAL_BACKOFF_BASE_MS, CRITICAL_BACKOFF_JITTER_MS);
  }

  /// DELETE_PARTITION op: any state → VACANT (entry removed from the map).  Hard-delete
  /// counterpart to `clearValid` (which writes VALID-empty).  Reserved for future manual
  /// admin commands; not currently invoked from any automatic path.
  ///
  /// Lenient: if the bucket is already absent, the call is a no-op.
  ///
  /// Retry profile: critical.
  public void deletePartition(String tableNameWithType, long windowStartMs) {
    applyMutation(tableNameWithType, current -> {
      if (current == null) {
        LOGGER.debug("deletePartition: runtime znode missing for table: {}; skipping",
            tableNameWithType);
        return null;
      }
      if (!current.getPartitions().containsKey(windowStartMs)) {
        LOGGER.debug("deletePartition: bucket {} already absent for table {}; skipping",
            windowStartMs, tableNameWithType);
        return null;
      }
      Map<Long, PartitionInfo> updated = new HashMap<>(current.getPartitions());
      removePartitionInfo(updated, windowStartMs);
      LOGGER.info("deletePartition: table={} bucket={} removed", tableNameWithType, windowStartMs);
      return new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(), current.getWatermarkMs(), updated);
    }, criticalMaxAttempts(), CRITICAL_BACKOFF_BASE_MS, CRITICAL_BACKOFF_JITTER_MS);
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  Private CRUD primitives — fail-loud invariant checks on the in-memory map
  // ─────────────────────────────────────────────────────────────────────────────────────

  /// Adds an entry that the caller asserts is currently absent.  Throws on violation —
  /// silent overwrite would mask races (e.g. a double-dispatched APPEND for the same
  /// bucket).  Use [#updatePartitionInfo] when overwriting an existing entry is intended.
  private static void addPartitionInfo(Map<Long, PartitionInfo> map, long windowStartMs,
      PartitionInfo info) {
    Preconditions.checkArgument(info != null, "PartitionInfo must not be null");
    Preconditions.checkState(!map.containsKey(windowStartMs),
        "addPartitionInfo: bucket %s already present (existing: %s)",
        windowStartMs, map.get(windowStartMs));
    map.put(windowStartMs, info);
  }

  /// Replaces an existing entry the caller asserts is present.  Throws on violation — a
  /// missing entry under an "update" op indicates the caller's precondition check passed
  /// against a stale snapshot, which is a logic bug worth surfacing.
  private static void updatePartitionInfo(Map<Long, PartitionInfo> map, long windowStartMs,
      PartitionInfo info) {
    Preconditions.checkArgument(info != null, "PartitionInfo must not be null");
    Preconditions.checkState(map.containsKey(windowStartMs),
        "updatePartitionInfo: bucket %s missing", windowStartMs);
    map.put(windowStartMs, info);
  }

  /// Removes an entry the caller asserts is present.  Throws on violation.
  private static void removePartitionInfo(Map<Long, PartitionInfo> map, long windowStartMs) {
    Preconditions.checkState(map.containsKey(windowStartMs),
        "removePartitionInfo: bucket %s missing", windowStartMs);
    map.remove(windowStartMs);
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  Private CAS engine
  // ─────────────────────────────────────────────────────────────────────────────────────

  /// Mutator contract: given the current snapshot, return the new
  /// [MaterializedViewRuntimeMetadata] to persist, or `null` to indicate the call is a
  /// no-op (success, no CAS write).  Mutators MAY throw to abort with fail-fast — the
  /// applyMutation loop does not retry on mutator-thrown exceptions.
  @FunctionalInterface
  @VisibleForTesting
  interface Mutator {
    @Nullable
    MaterializedViewRuntimeMetadata apply(@Nullable MaterializedViewRuntimeMetadata current);
  }

  /// Runs `mutator` under a version-checked CAS retry loop.  See class-level docs for the
  /// retry classification rules.
  ///
  /// On budget exhaust, throws [RuntimeException] wrapping the last CAS or transport
  /// exception so the caller (typically a minion task) sees a recoverable failure path
  /// rather than silent corruption.
  private void applyMutation(String tableNameWithType, Mutator mutator,
      int maxAttempts, long backoffBaseMs, int backoffJitterMs) {
    Preconditions.checkArgument(maxAttempts > 0, "maxAttempts must be positive");
    ZkException lastZkException = null;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        sleepWithJitter(tableNameWithType, backoffBaseMs, backoffJitterMs);
      }
      Stat stat = new Stat();
      MaterializedViewRuntimeMetadata current;
      try {
        current = MaterializedViewRuntimeMetadataUtils.fetchWithVersion(_propertyStore, tableNameWithType, stat);
      } catch (ZkException e) {
        // Transient read-side ZK / session failure: count it against the retry budget and re-fetch
        // on the next attempt rather than letting it escape unretried.  Mutator precondition
        // failures (below) are deliberately NOT caught here, so they still fail-fast.
        lastZkException = e;
        LOGGER.warn("ZK transport error reading MV runtime for table {} on attempt {}",
            tableNameWithType, attempt + 1, e);
        continue;
      }
      int writeVersion = (current != null) ? stat.getVersion() : -1;

      MaterializedViewRuntimeMetadata updated = mutator.apply(current);
      if (updated == null) {
        // Mutator signalled no-op (idempotent path or precondition no longer applies).
        return;
      }

      try {
        MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, updated, writeVersion);
        return;
      } catch (MaterializedViewRuntimeMetadataUtils.CasConflictException e) {
        lastZkException = e;
        LOGGER.debug("CAS conflict for table {} on attempt {}; will retry",
            tableNameWithType, attempt + 1);
      } catch (ZkException e) {
        // Transport / session level failures: log and retry with backoff.  A transient
        // ZK reconnect should not abort an otherwise-correct mutation.
        lastZkException = e;
        LOGGER.warn("ZK transport error persisting MV runtime for table {} on attempt {}",
            tableNameWithType, attempt + 1, e);
      }
      // IllegalStateException / IllegalArgumentException from validateForPersist are NOT
      // caught here: they indicate the produced metadata is structurally invalid, which
      // retrying cannot fix.  They propagate to the caller verbatim.
    }
    throw new RuntimeException(
        "Failed to update MV runtime for table: " + tableNameWithType
            + " after " + maxAttempts + " attempts", lastZkException);
  }

  private static void sleepWithJitter(String tableNameWithType, long baseMs, int jitterMs) {
    try {
      Thread.sleep(baseMs + ThreadLocalRandom.current().nextInt(Math.max(jitterMs, 1)));
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted while retrying MV runtime update for table: " + tableNameWithType, ie);
    }
  }

  private int criticalMaxAttempts() {
    return MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
        _clusterConfigReader,
        MaterializedViewTask.CLUSTER_CONFIG_KEY_MAX_RUNTIME_UPDATE_ATTEMPTS,
        DEFAULT_CRITICAL_MAX_ATTEMPTS);
  }
}
