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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingest.InsertConsistencyMode;
import org.apache.pinot.spi.ingest.InsertErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Central coordinator for the push-based INSERT INTO statement lifecycle.
///
/// Manages the v1 state machine (ACCEPTED -> VISIBLE on success, ACCEPTED -> ABORTED on failure; GC'd by
/// deleting the manifest from ZK after a retention window),
/// idempotency checking, hybrid table validation, delegation to {@link InsertExecutor} backends,
/// and background cleanup of stuck statements.
///
/// The coordinator is resilient to controller failover: on startup, it resumes cleanup from
/// the ZK-persisted state. No in-memory state is required for correctness; the ZK manifests are
/// the source of truth.
///
/// This class is thread-safe. It is instantiated once during controller startup and shared
/// across REST API handlers.
public class InsertStatementCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementCoordinator.class);

  /// Default timeout for statements stuck in ACCEPTED state (30 minutes).
  public static final long DEFAULT_STATEMENT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(30);

  /// Default retention period before VISIBLE statements are moved to GC (24 hours).
  public static final long DEFAULT_VISIBLE_RETENTION_MS = TimeUnit.HOURS.toMillis(24);

  /// Long retention before stuck-ACCEPTED ROW manifests are GC'd (7 days). Such manifests can occur
  /// when the ROW executor's success path uploads/registers segments but the subsequent manifest
  /// CAS-persist to VISIBLE fails after retries — the data is live but the state is stuck. We do
  /// not abort these manifests (would falsely report failure) but we do reap them after a long
  /// window so /insert/list does not accumulate stuck rows indefinitely. Operators have multiple
  /// days to investigate before the manifest disappears.
  public static final long ACCEPTED_ROW_RETENTION_MS = TimeUnit.DAYS.toMillis(7);

  /// Interval between cleanup sweeps (5 minutes).
  private static final long CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

  /// Standard Pinot segment-name character set. Requires the FIRST character to be alnum or
  /// underscore/dash (NOT a dot) so adversarial inputs like ".", "..", "..." cannot survive — those
  /// would otherwise be accepted by a naive `[A-Za-z0-9_.\-]+` regex and become path-traversal
  /// vectors if downstream code joins segment names into deep-store paths. Also rejects consecutive
  /// dots (e.g. `seg..bad`) so that adversarial relative-path fragments mid-name cannot
  /// survive operating-system-specific path normalization downstream.
  private static final Pattern SEGMENT_NAME_PATTERN =
      Pattern.compile("[A-Za-z0-9_\\-][A-Za-z0-9_\\-]*(\\.[A-Za-z0-9_\\-]+)*");

  /// Per-segment-name length cap for `/insert/complete`. ZK znodes have a 1MB default limit;
  /// a malicious caller submitting a 1MB segment-name would otherwise wedge the manifest write.
  /// 256 chars matches Pinot's typical segment-name budget.
  private static final int MAX_SEGMENT_NAME_LENGTH = 256;

  /// Per-completion segment-name list cap. 10K is far above any realistic single-batch size and
  /// keeps the manifest's segmentNames within ZK's znode size budget even with maximum-length names.
  private static final int MAX_SEGMENT_NAMES_PER_COMPLETION = 10_000;

  private final PinotHelixResourceManager _helixResourceManager;
  private final InsertStatementStore _statementStore;
  private final ControllerMetrics _controllerMetrics;
  /// Mutable until {@link #start()} freezes the registration into {@link #_executorSnapshot}. After
  /// start, no thread reads this map — all lookups go through the immutable snapshot. EnumMap is
  /// used because {@link InsertType} is a small fixed enum; the EnumMap variant is faster than
  /// HashMap (array-backed) and better self-documents the closed key domain.
  private final EnumMap<InsertType, InsertExecutor> _executors;
  /// Immutable snapshot of {@link #_executors} published at {@link #start()}. The `volatile`
  /// write in `start()` establishes a happens-before with every subsequent volatile read on
  /// the request paths, removing the need to reason about the underlying map's per-key visibility.
  /// `null` until `start()` runs.
  private volatile Map<InsertType, InsertExecutor> _executorSnapshot;
  private final long _statementTimeoutMs;
  private final long _visibleRetentionMs;

  /// Tables that are known to have insert statements. Populated as statements are submitted
  /// and during cleanup sweeps. This set is an optimization to avoid scanning all tables; the
  /// ZK state is always the source of truth.
  private final Set<String> _tablesWithStatements = ConcurrentHashMap.newKeySet();

  /// `true` once {@link #start()} has been called. Controls whether the REST layer will
  /// accept traffic and whether {@link #stop()} needs to shut down the scheduler. When the feature
  /// flag is off, the coordinator is still constructed (so Jersey binding resolves) but this stays
  /// `false`.
  private volatile boolean _started = false;
  private volatile ScheduledExecutorService _cleanupSchedulerInstance;
  /// Counts in-flight `submitInsert` calls (after `_started` check, before the executor
  /// call returns). `stop()` awaits this counter draining to zero before returning, so a
  /// long-running `executor.execute()` cannot land a ZK write after the coordinator is
  /// officially stopped — which would break failover semantics if a new leader has begun running.
  private final AtomicLong _inflightSubmitCount = new AtomicLong(0);

  /// Counts in-flight {@link #cleanupAllTables} sweep iterations. The sweep mutates ZK on every
  /// state transition (CAS-flips, manifest deletes) and can run for tens of seconds with thousands
  /// of manifests. `stop()` awaits this counter draining alongside `_inflightSubmitCount`
  /// so a sweep cannot land a write after stop() returns and a new leader on a different controller
  /// begins running.
  private final AtomicLong _inflightSweepCount = new AtomicLong(0);

  /// Atomic counter of statements currently in a non-terminal state. In v1 the only non-terminal
  /// state is {@link InsertStatementState#ACCEPTED} — PREPARED/COMMITTED are reserved for v2's
  /// two-phase commit. The counter is seeded once at {@link #start()} via
  /// {@link #computeActiveStatementCountFromZk()}, adjusted incrementally by transition sites
  /// (submit, abort, complete, cleanup-sweep), and periodically reseeded by the cleanup sweep
  /// (every {@link #ACTIVE_COUNT_RESEED_EVERY_N_SWEEPS} passes) to reconcile any drift from rare
  /// race-induced double-counts (e.g., the EXECUTOR_ERROR_BUT_DURABLE path can decrement
  /// speculatively when the manifest re-read raced with a writer). Constant-time read; converges
  /// to ZK truth on every controller restart and on each periodic reseed.
  private final AtomicLong _activeStatementCount = new AtomicLong(0);

  /// Counter of cleanup sweeps since {@link #start()}; used to drive periodic reseed of
  /// {@link #_activeStatementCount} from ZK without doing it on every 5-minute sweep.
  private final AtomicLong _cleanupSweepCount = new AtomicLong(0);

  /// Reseed the active-statement counter from ZK every Nth cleanup sweep. At a 5-minute sweep
  /// interval, 12 sweeps = 1 hour between reseeds. Reseeding every sweep doubles the ZK list cost
  /// for clusters with thousands of manifests; bounding drift to 1 hour is acceptable for an
  /// advisory gauge. The incremental counter is the authoritative source between reseeds.
  private static final int ACTIVE_COUNT_RESEED_EVERY_N_SWEEPS = 12;

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics) {
    this(helixResourceManager, statementStore, controllerMetrics,
        DEFAULT_STATEMENT_TIMEOUT_MS, DEFAULT_VISIBLE_RETENTION_MS);
  }

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics,
      long statementTimeoutMs, long visibleRetentionMs) {
    _helixResourceManager = helixResourceManager;
    _statementStore = statementStore;
    _controllerMetrics = controllerMetrics;
    _executors = new EnumMap<>(InsertType.class);
    _statementTimeoutMs = statementTimeoutMs;
    _visibleRetentionMs = visibleRetentionMs;
    /// Do NOT create the scheduler thread here — when the feature flag is disabled we never call
    /// start() and the thread would be leaked. Deferred to start().
  }

  /// Starts the background cleanup task for stuck statements. Creates the scheduler thread lazily
  /// so that when the feature flag is off and `start()` is never called, no thread is
  /// allocated.
  /// On startup (including after controller failover), the first sweep picks up any
  /// stuck statements from ZK.
  public synchronized void start() {
    if (_started) {
      return;
    }
    /// Publish the executor snapshot, seed the active-counter, and flip _started=true BEFORE arming
    /// the scheduler. The cleanup sweep's first iteration must observe a fully-initialized
    /// coordinator; arming the scheduler last ensures that even the rare interleaving where the
    /// scheduled task fires immediately (theoretical on busy CPU) sees the published state.
    _executorSnapshot = Collections.unmodifiableMap(new EnumMap<>(_executors));
    /// Seed the active-statement counter from ZK so that recovery after a previous leader's crash
    /// starts from the correct value. We discover tables lazily — _tablesWithStatements is empty
    /// here; the first cleanup sweep will populate it and reseed.
    _activeStatementCount.set(computeActiveStatementCountFromZk());
    _started = true;

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "insert-statement-cleanup");
      t.setDaemon(true);
      return t;
    });
    try {
      scheduler.scheduleWithFixedDelay(this::cleanupAllTables, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS,
          TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      /// Don't leak the executor if scheduling fails — roll back _started and surface the error.
      _started = false;
      _executorSnapshot = null;
      scheduler.shutdownNow();
      throw e;
    }
    _cleanupSchedulerInstance = scheduler;
    LOGGER.info("InsertStatementCoordinator started with statementTimeout={}ms, visibleRetention={}ms",
        _statementTimeoutMs, _visibleRetentionMs);
  }

  /// @return `true` once {@link #start()} has been called. REST handlers use this to fail
  ///   closed with HTTP 503 when the coordinator is idle (feature flag off).
  public boolean isStarted() {
    return _started;
  }

  /// Stops the coordinator and shuts down the cleanup scheduler. Order matters: flip
  /// `_started=false` BEFORE the shutdown so any new sweep iteration bails immediately, then
  /// `shutdownNow()` interrupts in-flight work, then await termination so a sweep that was
  /// mid-write to ZK gets time to finish before `stop()` returns. Without the await, an
  /// in-flight ZK write can land after the controller is officially stopped — and a new leader on
  /// a different controller observes the dying leader's mutation, breaking failover semantics.
  public synchronized void stop() {
    _started = false;
    /// Await any in-flight submitInsert calls. New calls bail at the _started check above; existing
    /// ones (already past the check, possibly mid-executor.execute()) may take longer than the
    /// sweep scheduler's timeout. Polling once per 100ms is fine for stop() — it runs once per
    /// controller lifetime. Cap at 30s to avoid blocking shutdown indefinitely if a request is
    /// truly wedged (e.g., a Minion call that never returns); operators see the WARN.
    long awaitDeadlineMs = System.currentTimeMillis() + 30_000;
    while ((_inflightSubmitCount.get() > 0 || _inflightSweepCount.get() > 0)
        && System.currentTimeMillis() < awaitDeadlineMs) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    long stillInflightSubmits = _inflightSubmitCount.get();
    long stillInflightSweeps = _inflightSweepCount.get();
    if (stillInflightSubmits > 0 || stillInflightSweeps > 0) {
      LOGGER.warn("InsertStatementCoordinator stop(): {} in-flight submitInsert calls and {} "
              + "in-flight sweep iterations did not drain within 30s; ZK writes from those may "
              + "land after stop() returns", stillInflightSubmits, stillInflightSweeps);
    }
    if (_cleanupSchedulerInstance != null) {
      _cleanupSchedulerInstance.shutdownNow();
      try {
        if (!_cleanupSchedulerInstance.awaitTermination(10, TimeUnit.SECONDS)) {
          LOGGER.warn("InsertStatementCoordinator cleanup scheduler did not terminate within 10s; "
              + "in-flight ZK writes may complete after stop() returns");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for cleanup scheduler shutdown");
      }
      _cleanupSchedulerInstance = null;
    }
    /// Defense-in-depth: drop the frozen executor snapshot symmetrically with the start()
    /// rollback path. The _started flag is the load-bearing guard for stopping new submissions
    /// and sweep iterations; this null-out only ensures the snapshot is not held longer than the
    /// coordinator's lifecycle (lookupExecutor falls back to the live _executors map, so this is
    /// not a hard fence — it's hygiene).
    _executorSnapshot = null;
    LOGGER.info("InsertStatementCoordinator stopped");
  }

  /// Registers an {@link InsertExecutor} for a specific insert type. Must be called BEFORE
  /// {@link #start()} — the cleanup sweep and submitInsert paths read from `_executors`
  /// without holding a lock, on the assumption that the registration set is frozen at start time.
  ///
  /// **v1 limitation:** `InsertType.FILE` requires the in-tree {@link FileInsertExecutor}
  /// concrete class. The coordinator hardcodes `instanceof FileInsertExecutor` checks at
  /// several lifecycle sites (task-name caching, Minion task polling, completion). A third-party
  /// plugin that implements {@link InsertExecutor} alone and registers as `FILE` would
  /// silently bypass those hooks, leaving manifests stuck in `ACCEPTED`. We fail fast here
  /// rather than silently accept a half-functional registration. v2 will lift FILE-specific hooks
  /// into the SPI as default methods so third-party implementations can opt in.
  ///
  /// @throws IllegalStateException if called after {@link #start()} has flipped `_started`
  /// @throws UnsupportedOperationException if the executor for `InsertType.FILE` is not a
  ///     subclass of the in-tree {@link FileInsertExecutor}
  public synchronized void registerExecutor(String executorType, InsertExecutor executor) {
    if (_started) {
      throw new IllegalStateException("registerExecutor must be called before start(); coordinator is already started. "
          + "Late registration would race with the cleanup sweep and submitInsert.");
    }
    InsertType type;
    try {
      type = InsertType.valueOf(executorType);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown InsertExecutor type: '" + executorType
          + "'. Supported values: " + Arrays.toString(InsertType.values()), e);
    }
    if (type == InsertType.FILE && !(executor instanceof FileInsertExecutor)) {
      throw new UnsupportedOperationException("InsertType.FILE requires a FileInsertExecutor (or subclass) in v1. "
          + "Pluggable third-party FILE executors are not supported until v2 lifts FILE-specific hooks "
          + "(getScheduledTaskName, taskCompleted, prepareCompletionResult) into the InsertExecutor SPI. "
          + "Got executor of class: " + executor.getClass().getName());
    }
    _executors.put(type, executor);
    LOGGER.info("Registered InsertExecutor for type={}", type);
  }

  /// Reads an executor from the immutable post-{@link #start()} snapshot. Falls back to the
  /// still-mutable {@link #_executors} map only when `_started=false` (i.e., from tests that
  /// register and read without starting). Production code paths always go through the snapshot.
  @Nullable
  private InsertExecutor lookupExecutor(InsertType type) {
    Map<InsertType, InsertExecutor> snapshot = _executorSnapshot;
    if (snapshot != null) {
      return snapshot.get(type);
    }
    return _executors.get(type);
  }

  @Nullable
  private InsertExecutor lookupExecutor(String executorTypeName) {
    try {
      return lookupExecutor(InsertType.valueOf(executorTypeName));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /// Submits an INSERT INTO request for execution.
  ///
  /// Performs idempotency checks, hybrid table validation, creates a manifest,
  /// and delegates to the appropriate executor.
  ///
  /// @param request the insert request
  /// @return the result reflecting the initial state of the statement
  public InsertResult submitInsert(InsertRequest request) {
    /// Increment the in-flight counter BEFORE the _started check. This ordering is load-bearing
    /// for stop()'s drain: stop() flips _started=false then awaits _inflightSubmitCount==0. If we
    /// checked _started first and a concurrent stop() raced between the check and the increment,
    /// stop() would return while we were still about to do work — ZK writes could land after the
    /// coordinator is officially stopped, breaking failover semantics. By incrementing first then
    /// re-checking _started, any request that gets past the check is guaranteed observable to
    /// stop()'s drain loop.
    _inflightSubmitCount.incrementAndGet();
    try {
      if (!_started) {
        /// Feature flag is off OR coordinator is mid-startup/shutdown. Fail fast for in-process
        /// callers (e.g., the local SqlQueryExecutor wrapper) — REST callers see HTTP 503 via
        /// InsertStatementResource.checkEnabled(). Both paths must reject submissions consistently.
        return rejectResult(request.getStatementId(), InsertErrorCode.COORDINATOR_NOT_READY,
            "InsertStatementCoordinator is not started (feature disabled or controller starting/stopping)");
      }
      return submitInsertInternal(request);
    } finally {
      _inflightSubmitCount.decrementAndGet();
    }
  }

  private InsertResult submitInsertInternal(InsertRequest request) {

    /// 1. Resolve table name to physical table with type
    String tableNameWithType;
    try {
      tableNameWithType = resolveTableName(request.getTableName(), request.getTableType());
    } catch (IllegalArgumentException e) {
      return rejectResult(request.getStatementId(), InsertErrorCode.TABLE_RESOLUTION_ERROR, e.getMessage());
    }

    /// 2. Idempotency check — atomic reservation via ZK to prevent concurrent retries from
    ///    both creating statements for the same requestId. Fails closed on ZK errors.
    ///
    /// ownReservation=true means THIS request created the ZK reservation node and is responsible
    /// for releasing it on failure. When ownReservation=false, a prior request created the node
    /// and we must never delete it (doing so would break idempotency for the original client).
    boolean ownReservation = false;
    boolean needsRebind = false;
    String staleStatementId = null;
    if (request.getRequestId() != null) {
      String existingStatementId;
      try {
        existingStatementId = _statementStore.reserveRequestId(
            tableNameWithType, request.getRequestId(), request.getStatementId());
      } catch (RuntimeException e) {
        LOGGER.error("RequestId reservation failed for requestId={}", request.getRequestId(), e);
        return rejectResult(request.getStatementId(), InsertErrorCode.IDEMPOTENCY_ERROR,
            "Cannot verify idempotency due to ZK failure: " + e.getMessage());
      }
      if (existingStatementId != null) {
        /// GC_PENDING reservations are claimed-for-deletion by a sweep; rebinding would race the
        /// imminent unconditional remove. Surface a retryable error directly rather than entering
        /// the stale-rebind path that will eventually return REBIND_RACE_LOST anyway.
        if (existingStatementId.startsWith("__GC_PENDING_")) {
          LOGGER.info("RequestId={} reservation is pending GC; client should retry shortly",
              request.getRequestId());
          return rejectResult(request.getStatementId(), InsertErrorCode.REBIND_RACE_LOST,
              "Reservation for requestId is being garbage-collected; retry the request");
        }
        /// A prior request created this reservation — we do not own it and must not delete it.
        InsertStatementManifest existing = _statementStore.getStatement(tableNameWithType, existingStatementId);
        if (existing != null) {
          return handleIdempotency(request, existing);
        }
        /// Reservation exists but manifest was GC'd. Defer rebind until after the new manifest
        /// is persisted (step 5) so that concurrent retries reading the rebound reservation
        /// will find the real manifest rather than hitting the stale branch again.
        LOGGER.warn("RequestId={} has stale reservation for GC'd statementId={}. "
                + "Will rebind to new statementId={} after manifest is persisted.",
            request.getRequestId(), existingStatementId, request.getStatementId());
        needsRebind = true;
        staleStatementId = existingStatementId;
      } else {
        /// This request won the create() race and owns the reservation node.
        ownReservation = true;
      }
    }

    /// 3. Validate consistency mode — only WAIT_FOR_ACCEPT is shipped in v1. Unknown enum values
    /// are rejected at JSON deserialization (InsertConsistencyMode.fromJson); this branch catches
    /// any future-pluggable callers that bypass the SPI deserializer with a different mode.
    if (request.getConsistencyMode() != InsertConsistencyMode.WAIT_FOR_ACCEPT) {
      if (ownReservation) {
        releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
      }
      return rejectResult(request.getStatementId(), InsertErrorCode.UNSUPPORTED_CONSISTENCY_MODE,
          "Only WAIT_FOR_ACCEPT consistency mode is supported in this version. "
              + "Received: " + request.getConsistencyMode());
    }

    /// 4. Check executor availability
    String executorType = request.getInsertType().name();
    InsertExecutor executor = lookupExecutor(executorType);
    if (executor == null) {
      if (ownReservation) {
        releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
      }
      return rejectResult(request.getStatementId(), InsertErrorCode.NO_EXECUTOR,
          "No InsertExecutor registered for type: " + executorType);
    }

    /// 4b. ROW-specific pre-acceptance validation: empty rows is a deterministic input error and
    /// belongs in REJECTED, not as an ABORTED manifest. (FILE inserts have no equivalent — the
    /// file URI is checked downstream where deep-store access is needed.)
    if (request.getInsertType() == InsertType.ROW
        && (request.getRows() == null || request.getRows().isEmpty())) {
      if (ownReservation) {
        releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
      }
      return rejectResult(request.getStatementId(), InsertErrorCode.EMPTY_ROWS,
          "INSERT INTO ... VALUES requires at least one row");
    }

    /// 5. If we observed a stale reservation, win the rebind FIRST — before publishing any orphan
    /// manifest in ZK. rebindRequestIdIfEquals uses a content-and-version-checked CAS: a concurrent
    /// retry that also observed the same stale reservation will lose here, and must yield to the
    /// winner. Doing this before createStatement means the loser never publishes a manifest that
    /// could be observed by listStatements/findStatementAcrossTables/cleanup-sweep during the brief
    /// window between create and delete in the previous (buggy) ordering.
    if (needsRebind) {
      LOGGER.info("Rebinding requestId={} from stale statementId={} to new statementId={}",
          request.getRequestId(), staleStatementId, request.getStatementId());
      boolean rebound = _statementStore.rebindRequestIdIfEquals(tableNameWithType, request.getRequestId(),
          staleStatementId, request.getStatementId());
      if (!rebound) {
        /// Lost the rebind race. The reservation now points at the winner's statementId. Briefly
        /// poll for the winner's manifest to be findable: the winner may not have called
        /// createStatement yet between its successful rebind and its manifest persist.
        LOGGER.warn("Lost rebind race for requestId={}: another concurrent caller claimed the reservation.",
            request.getRequestId());
        InsertStatementManifest winner = waitForWinnerManifest(tableNameWithType, request.getRequestId(),
            request.getStatementId());
        if (winner != null && !winner.getStatementId().equals(request.getStatementId())) {
          return handleIdempotency(request, winner);
        }
        /// Winner manifest still not visible after polling — fail closed. Caller can retry; the
        /// retry will see the now-rebound reservation in handleIdempotency on the first read.
        return rejectResult(request.getStatementId(), InsertErrorCode.REBIND_RACE_LOST,
            "Lost the rebind race; winner's manifest not yet visible. Retry the request.");
      }
    }

    /// 6. Create manifest. If needsRebind=true we won the rebind above, but a concurrent retry
    /// arriving between our rebind and our createStatement can observe the reservation pointing at
    /// our statementId, look up the manifest (not yet persisted), treat our statementId as stale,
    /// and rebind the reservation to its own statementId. We close that residual race below by
    /// re-reading the reservation after createStatement and self-rolling-back if it no longer
    /// points at us.
    long now = System.currentTimeMillis();
    InsertStatementManifest manifest =
        new InsertStatementManifest(request.getStatementId(), request.getRequestId(),
            request.getPayloadHash(), tableNameWithType, request.getInsertType(),
            InsertStatementState.ACCEPTED, now, now, Collections.emptyList(), null, null, null);

    if (!_statementStore.createStatement(manifest)) {
      if (ownReservation) {
        releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
      } else if (needsRebind) {
        /// We won the rebind but cannot create the manifest. The reservation now points at our
        /// orphan statementId for which no manifest exists. Tombstone it directly so the prune
        /// sweep can reap it on its retention schedule. We do NOT attempt to restore the rebind
        /// back to the original stale statementId — that path would leave the reservation pointing
        /// at a still-GC'd statementId, and every future retry would re-enter the same orphan
        /// dance forever (the prune sweep only reaps __TOMBSTONE_/__GC_PENDING_ records, not bare
        /// stale-statementId references whose manifests don't exist). releaseRequestIdIfEquals is
        /// a content-checked CAS that writes a tombstone iff the reservation still points at our
        /// statementId — safe under concurrent retries.
        LOGGER.warn("createStatement failed for requestId={}; tombstoning orphan reservation "
                + "pointing at statementId {} so prune sweep can reap it.",
            request.getRequestId(), request.getStatementId());
        try {
          _statementStore.releaseRequestIdIfEquals(tableNameWithType, request.getRequestId(),
              request.getStatementId());
        } catch (Exception ex) {
          LOGGER.warn("Failed to tombstone orphan reservation for requestId={}; will require "
              + "manual cleanup or wait for retention-based prune.", request.getRequestId(), ex);
        }
      }
      return rejectResult(request.getStatementId(), InsertErrorCode.STORE_ERROR,
          "Failed to persist statement manifest in ZooKeeper");
    }

    /// 6a. Close the rebind-vs-create residual race for the needsRebind path. Between our
    /// successful rebindRequestIdIfEquals (step 5) and the createStatement above, a concurrent
    /// retry of the SAME requestId can:
    ///   1) read the reservation, observe it points at our statementId,
    ///   2) call getStatement(our_statementId) which returns null (not yet persisted),
    ///   3) enter the stale-reservation branch with staleStatementId=our_statementId,
    ///   4) rebindRequestIdIfEquals(our_statementId, retry_statementId) — and win the CAS.
    /// The reservation now points at the retry; our manifest is an orphan that does not match the
    /// current reservation. Self-rollback by deleting our manifest and returning REBIND_RACE_LOST.
    /// Skipping this check on the ownReservation path is safe: there, the reservation was created
    /// atomically by reserveRequestId (ZK create-only) so no concurrent caller can have rebound it.
    ///
    /// Best-effort detection: peek-then-act is not a closed CAS. The cleanup sweep cannot promote a
    /// fresh (non-tombstone) reservation to GC_PENDING, so the only failure mode is a true rebind
    /// race. peekReservedStatementId throws on transient ZK failures (rather than returning null) so
    /// we do not mis-classify a flake as a rebind loss and incorrectly delete a valid manifest.
    if (needsRebind) {
      String currentReservedStatementId;
      try {
        currentReservedStatementId =
            _statementStore.peekReservedStatementId(tableNameWithType, request.getRequestId());
      } catch (RuntimeException ex) {
        /// Transient ZK failure during the post-create reservation re-read. We cannot prove the
        /// reservation was rebound, so we MUST NOT delete the manifest (deleting on a flake would
        /// destroy a valid statement). Leave the manifest in place; the cleanup sweep will reconcile
        /// if it really is orphaned. Surface a state-persist error annotated with the durable state
        /// (ACCEPTED) so the caller's response state matches what getStatus would observe.
        /// Bump the active-statement counter and submitted meter symmetrically with the normal
        /// success path: the manifest is durable in ACCEPTED, so its eventual cleanup-sweep abort
        /// will decrement the counter — without the +1 here, that decrement drifts the gauge
        /// negative until the periodic ZK reseed reconciles it.
        LOGGER.warn("Failed to verify reservation ownership after createStatement for requestId={} "
                + "statementId={}; manifest left in place. Cleanup sweep will reconcile if orphaned.",
            request.getRequestId(), request.getStatementId(), ex);
        _tablesWithStatements.add(tableNameWithType);
        bumpActiveStatementCount(1);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_SUBMITTED, 1);
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE,
            getActiveStatementCount());
        return errorResult(request.getStatementId(), InsertStatementState.ACCEPTED,
            InsertErrorCode.STATE_PERSIST_ERROR,
            "Manifest persisted but post-create reservation re-read failed: " + ex.getMessage());
      }
      if (!request.getStatementId().equals(currentReservedStatementId)) {
        LOGGER.warn("Rebind-vs-create race lost for requestId={}: reservation now points at {}, "
                + "rolling back orphan manifest at statementId={}",
            request.getRequestId(), currentReservedStatementId, request.getStatementId());
        try {
          _statementStore.deleteStatement(tableNameWithType, request.getStatementId());
        } catch (Exception ex) {
          LOGGER.warn("Failed to delete orphan manifest for statementId={} after rebind race; "
                  + "cleanup sweep will reap it.", request.getStatementId(), ex);
        }
        return rejectResult(request.getStatementId(), InsertErrorCode.REBIND_RACE_LOST,
            "Lost the rebind-vs-create race; reservation rebound to a concurrent retry. "
                + "Retry the request to observe the winner's manifest.");
      }
    }

    _tablesWithStatements.add(tableNameWithType);
    bumpActiveStatementCount(1);  /// newly ACCEPTED manifest
    /// Increment SUBMITTED only after the manifest is durably persisted so probes with bad table
    /// names or other rejected requests don't inflate the meter.
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_SUBMITTED, 1);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());

    /// 6. Delegate to executor with the resolved table name so downstream code does not
    ///    need to re-resolve (and potentially default to OFFLINE for realtime-only tables).
    InsertRequest resolvedRequest = request.withResolvedTable(tableNameWithType);
    /// Tracks whether the success-path decrement already ran for this submit. If a metric/gauge
    /// call AFTER the decrement throws, control reaches the catch block — it must not decrement
    /// again. This prevents double-decrement that would drift _activeStatementCount negative.
    boolean activeCounterDecremented = false;
    try {
      InsertResult executorResult = executor.execute(resolvedRequest);

      /// Persist the executor's result state into the manifest so that ZK reflects actual progress.
      InsertStatementState resultState = executorResult.getState();

      /// For FILE inserts that stay in ACCEPTED, persist the Minion task name to ZK so that a new
      /// controller leader can still poll task state after failover without relying on the in-memory
      /// _taskNames map inside FileInsertExecutor. If we cannot persist the task name, we fail the
      /// INSERT closed: the alternative (return ACCEPTED but the new leader has no way to find the
      /// running task) creates orphan segments after failover.
      if ((resultState == null || resultState == InsertStatementState.ACCEPTED)
          && executor instanceof FileInsertExecutor) {
        String taskName = ((FileInsertExecutor) executor).getScheduledTaskName(request.getStatementId());
        if (taskName != null) {
          CasResult taskNameCas = persistWithCasRetry(tableNameWithType, request.getStatementId(),
              m -> m.getState() == InsertStatementState.ACCEPTED,
              fresh -> fresh.setMinionTaskName(taskName));
          if (taskNameCas != CasResult.SUCCESS && taskNameCas != CasResult.PRECHECK_FAILED) {
            LOGGER.error("Failed to persist minionTaskName={} for statementId={} ({}). "
                    + "Aborting to prevent orphan task after failover.",
                taskName, request.getStatementId(), taskNameCas);
            /// Surface this rare branch as a dedicated metric so operators can alert on it; the
            /// Minion task may still be running and push orphan segments that operators must
            /// manually reconcile (documented v1 limitation; v2 will add lineage-aware revert).
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_TASK_NAME_PERSIST_FAILED, 1);
            /// Best-effort abort of the manifest. The Minion task itself may keep running and push
            /// segments — operator cleanup is the v1 fallback (documented as a v1 limitation).
            CasResult abortCas = persistWithCasRetry(tableNameWithType, request.getStatementId(),
                m -> m.getState() == InsertStatementState.ACCEPTED,
                fresh -> {
                  fresh.setState(InsertStatementState.ABORTED);
                  fresh.setErrorMessage(
                      "Could not persist Minion task name to ZK; aborted to prevent failover orphan");
                });
            /// Only bump aborted-meter and decrement active counter when WE are the actor that
            /// achieved the transition. PRECHECK_FAILED means a concurrent writer already moved the
            /// manifest to a terminal state (and incremented its own counters); double-counting
            /// would drift the gauge.
            if (abortCas == CasResult.SUCCESS) {
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
              bumpActiveStatementCount(-1);  /// ACCEPTED → ABORTED
              /// SPI contract: every transition into ABORTED must invoke executor.abort() so plugin
              /// implementations can release resources (caches, scheduled tasks, segment lineage).
              /// FileInsertExecutor.abort() drops the _taskNames cache entry, so a separate
              /// taskCompleted() call would be redundant.
              InsertStatementManifest abortedManifest = _statementStore.getStatement(tableNameWithType,
                  request.getStatementId());
              if (abortedManifest != null) {
                delegateAbortToExecutor(abortedManifest);
              }
            } else if (abortCas == CasResult.PRECHECK_FAILED) {
              /// A concurrent writer already moved the manifest to a terminal state — they ran their
              /// own delegateAbortToExecutor (or success path's taskCompleted) and dropped the cache.
              /// But if that concurrent path was a different controller leader's cleanup-sweep, our
              /// local _taskNames cache still holds the entry. Drop it here as defense-in-depth so
              /// long-lived controllers don't leak cache entries on this rare race.
              if (executor instanceof FileInsertExecutor) {
                ((FileInsertExecutor) executor).taskCompleted(request.getStatementId());
              }
            }
            /// Do NOT release the requestId. The Minion task is already scheduled and may still
            /// push segments to the table. Releasing the reservation would let a retry create a
            /// fresh statementId and schedule a SECOND task against the same input file, producing
            /// duplicate segments. Keep the reservation; it will be GC'd by the cleanup sweep
            /// along with the ABORTED manifest after visibleRetentionMs.
            return errorResult(request.getStatementId(), InsertErrorCode.TASK_NAME_PERSIST_ERROR,
                "Could not persist Minion task name to ZK. Manifest aborted but the underlying Minion task may "
                    + "still push segments — operator cleanup may be required. RequestId reservation kept to "
                    + "prevent duplicate task scheduling on retry.");
          }
        }
      }

      if (resultState != null && resultState != InsertStatementState.ACCEPTED) {
        /// Apply terminal state transition (ACCEPTED -> VISIBLE/ABORTED/COMMITTED) with a CAS
        /// retry loop. Precondition guard: only upgrade from ACCEPTED — a concurrent abort writer
        /// already writing ABORTED must win, and we must not resurrect it with VISIBLE/COMMITTED.
        CasResult result = persistWithCasRetry(tableNameWithType, request.getStatementId(),
            m -> m.getState() == InsertStatementState.ACCEPTED,
            fresh -> {
              fresh.setState(resultState);
              if (executorResult.getMessage() != null) {
                fresh.setErrorMessage(executorResult.getMessage());
              }
              /// Always overwrite segmentNames with the executor's view at the terminal-state CAS,
              /// even when empty: a successful insert that produced zero segments (e.g., metadata
              /// only, or a sweep auto-complete that does not surface produced segments) MUST
              /// clear any prior speculative names rather than inheriting them. The executor
              /// contract is "this is what I produced"; honor it. Null is treated as "no opinion"
              /// and leaves the prior list untouched.
              if (executorResult.getSegmentNames() != null) {
                fresh.setSegmentNames(executorResult.getSegmentNames());
              }
            });

        if (result == CasResult.PRECHECK_FAILED) {
          /// A concurrent writer (user abort, cleanup sweep) already reached a terminal state.
          /// Surface that state to the caller; do not overwrite it.
          LOGGER.warn("State transition to {} for statementId={} rejected — concurrent writer won.",
              resultState, request.getStatementId());
          return errorResult(request.getStatementId(), InsertErrorCode.CONCURRENT_STATE_CHANGE,
              "Another actor (abort/cleanup) already transitioned this statement to a terminal state");
        }
        if (result != CasResult.SUCCESS) {
          LOGGER.error("Failed to persist state {} for statementId={} to ZK: {}. "
                  + "Manifest may be stale; cleanup sweep will reconcile.",
              resultState, request.getStatementId(), result);
          if (resultState == InsertStatementState.VISIBLE) {
            /// Execution succeeded and data may already be visible. Do NOT release the requestId —
            /// allowing a retry to create a fresh statementId would insert duplicate data. The
            /// reservation is GC'd by the cleanup sweep after the retention period.
            /// Surface the actual executor state (VISIBLE) — using ABORTED here would mislead
            /// callers since the data may be queryable.
            return errorResult(request.getStatementId(), resultState, InsertErrorCode.STATE_PERSIST_ERROR,
                "Statement executed but ZK state update failed. Segments may be visible but state is uncertain. "
                    + "Do not retry — data may already be queryable");
          }
          /// ABORTED with persist failure. Distinguish NOT_FOUND (manifest deleted — sweep already
          /// handled it, safe to release) from VERSION_CONFLICT (manifest still exists at unknown
          /// state — cannot safely release because a concurrent writer may have moved it to a
          /// durable terminal state).
          if (result == CasResult.VERSION_CONFLICT) {
            return errorResult(request.getStatementId(), InsertErrorCode.STATE_PERSIST_ERROR,
                "Could not persist ABORTED state after CAS retries; leaving requestId reserved so a concurrent "
                    + "writer can be reconciled by the cleanup sweep");
          }
          /// NOT_FOUND: manifest is already gone — another actor (cleanup sweep, concurrent abort)
          /// owns the transition and has already incremented its own counters. Attempt the
          /// content-checked release for hygiene but DO NOT increment INSERT_STATEMENTS_ABORTED or
          /// decrement the active counter — that would double-count and drift the gauge negative.
          if (resultState == InsertStatementState.ABORTED) {
            releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
            return executorResult;
          }
        }

        if (resultState == InsertStatementState.ABORTED) {
          _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
          bumpActiveStatementCount(-1);  /// ACCEPTED → ABORTED
          activeCounterDecremented = true;
          /// Reservation hygiene: only release when NO partial data was registered. If the executor's
          /// ABORTED result lists non-empty segmentNames (e.g., SEGMENT_UPLOAD_FAILED_PARTIAL from
          /// ControllerRowInsertExecutor when destructive rollback is disabled), the partial segments
          /// remain in IdealState and a retry under the same requestId would build duplicate segments
          /// alongside them. Keep the reservation so the retry hits handleIdempotency() and surfaces
          /// the prior manifest rather than producing duplicates. Content-checked release otherwise:
          /// a concurrent retry that rebound the reservation to a different statementId must not have
          /// its reservation deleted by our cleanup.
          List<String> partialSegments = executorResult.getSegmentNames();
          if (partialSegments == null || partialSegments.isEmpty()) {
            releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
          } else {
            LOGGER.warn("Keeping requestId reservation for statementId={} (errorCode={}); "
                    + "{} partial segment(s) remain registered. A retry of the same requestId will "
                    + "surface the prior manifest via idempotency rather than build duplicates.",
                request.getStatementId(), executorResult.getErrorCode(), partialSegments.size());
          }
        } else if (resultState == InsertStatementState.VISIBLE) {
          _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_VISIBLE, 1);
          bumpActiveStatementCount(-1);  /// ACCEPTED → VISIBLE
          activeCounterDecremented = true;
          _controllerMetrics.setValueOfGlobalGauge(
              ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
          /// Drop FILE executor's _taskNames cache: a fast Minion task that completes within the
          /// submit frame leaves the cache populated otherwise, and the cleanup-sweep would never
          /// observe a non-terminal manifest to GC the entry.
          if (executor instanceof FileInsertExecutor) {
            ((FileInsertExecutor) executor).taskCompleted(request.getStatementId());
          }
        }
      }
      return executorResult;
    } catch (Exception e) {
      /// Distinguish post-success bookkeeping failures (metric registry contention, gauge update,
      /// taskCompleted cache drop) from real executor failures. If the success-path of this try
      /// block already decremented the active-count, the executor + CAS both succeeded — the throw
      /// is from observability code AFTER the durable transition. Surface a distinct error code
      /// and the actual durable state, rather than a misleading EXECUTOR_ERROR_BUT_DURABLE.
      if (activeCounterDecremented) {
        LOGGER.warn("Post-success bookkeeping failed for statementId={}; manifest is durable, "
            + "metric/gauge update did not complete", request.getStatementId(), e);
        InsertStatementManifest latest = _statementStore.getStatement(tableNameWithType,
            request.getStatementId());
        InsertStatementState durableState = latest != null ? latest.getState()
            : InsertStatementState.VISIBLE;
        return errorResult(request.getStatementId(), durableState, InsertErrorCode.POST_SUCCESS_BOOKKEEPING_ERROR,
            "Insert succeeded (state=" + durableState + ") but post-success bookkeeping threw: "
                + e.getMessage());
      }
      LOGGER.error("Executor failed for statementId={}", request.getStatementId(), e);
      String errorMessage = "Executor error: " + e.getMessage();
      /// Use a precondition predicate so we only ABORT when the manifest is still ACCEPTED. If the
      /// executor's success path partially landed (e.g., segments uploaded and ZK manifest was
      /// already flipped to VISIBLE/COMMITTED before an exception bubbled up from a metric or
      /// gauge call), do NOT overwrite that state with ABORTED — and do NOT release the requestId,
      /// because the data may already be queryable. Releasing would let a retry produce duplicate
      /// inserts.
      CasResult cas = persistWithCasRetry(tableNameWithType, request.getStatementId(),
          m -> m.getState() == InsertStatementState.ACCEPTED,
          fresh -> {
            fresh.setState(InsertStatementState.ABORTED);
            fresh.setErrorMessage(errorMessage);
          });
      if (cas == CasResult.SUCCESS) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
        bumpActiveStatementCount(-1);  /// ACCEPTED → ABORTED
        activeCounterDecremented = true;
        releaseRequestIdOnFailure(tableNameWithType, request.getRequestId(), request.getStatementId());
        return errorResult(request.getStatementId(), InsertErrorCode.EXECUTOR_ERROR,
            "Executor failed: " + e.getMessage());
      }
      /// PRECHECK_FAILED: a concurrent writer (e.g., the success path moments earlier) reached a
      /// terminal state. Surface that state to the caller; data may be visible.
      InsertStatementManifest latest = _statementStore.getStatement(tableNameWithType, request.getStatementId());
      InsertStatementState currentState = latest != null ? latest.getState() : InsertStatementState.ABORTED;
      LOGGER.warn("Executor exception observed but manifest is in state {} for statementId={}; "
              + "not releasing requestId — data may be durable.",
          currentState, request.getStatementId());
      /// Drop the FILE executor's _taskNames cache entry ONLY if the manifest is in a terminal
      /// state. The PRECHECK_FAILED branch lands here when the success path racily moved the
      /// manifest to VISIBLE/ABORTED, but the manifest re-read can also legitimately observe
      /// ACCEPTED if the racing writer hadn't completed yet — in that case the cleanup-sweep
      /// poller will still need this cache entry to auto-complete the FILE insert. Dropping
      /// prematurely would orphan the manifest until the 30-min timeout-abort.
      boolean isTerminal = currentState == InsertStatementState.VISIBLE
          || currentState == InsertStatementState.ABORTED;
      if (isTerminal && executor instanceof FileInsertExecutor) {
        ((FileInsertExecutor) executor).taskCompleted(request.getStatementId());
      }
      /// Decrement the active counter only when the manifest reached a terminal state AND the
      /// success-path of this same try-block did not already decrement. If a metric/gauge call
      /// AFTER the success-path decrement throws (e.g., transient registry contention), control
      /// lands here observing VISIBLE/ABORTED — but the -1 was already applied. The
      /// `activeCounterDecremented` flag prevents double-counting that would drift the gauge
      /// permanently negative until the next periodic ZK reseed.
      if (isTerminal && !activeCounterDecremented) {
        bumpActiveStatementCount(-1);
      }
      return errorResult(request.getStatementId(), currentState, InsertErrorCode.EXECUTOR_ERROR_BUT_DURABLE,
          "Executor threw but manifest is in state " + currentState + ": " + e.getMessage());
    }
  }

  /// Aborts a statement, releasing any resources it holds. Double-abort is idempotent.
  ///
  /// @param statementId the statement identifier
  /// @param tableNameWithType the table name with type
  /// @return the result reflecting the aborted state
  public InsertResult abortStatement(String statementId, @Nullable String tableNameWithType) {
    if (!_started) {
      return rejectResult(statementId, InsertErrorCode.COORDINATOR_NOT_READY,
          "InsertStatementCoordinator is not started (feature disabled or controller starting/stopping)");
    }
    /// If tableNameWithType is not provided, search across all tables
    InsertStatementManifest manifest = null;
    if (tableNameWithType != null) {
      manifest = _statementStore.getStatement(tableNameWithType, statementId);
    } else {
      manifest = _statementStore.findStatementAcrossTables(statementId);
      if (manifest != null) {
        tableNameWithType = manifest.getTableNameWithType();
      }
    }

    if (manifest == null) {
      return rejectResult(statementId, InsertErrorCode.NOT_FOUND, "Statement not found: " + statementId);
    }

    InsertStatementState currentState = manifest.getState();

    /// Double-abort idempotency: already in terminal state
    if (currentState == InsertStatementState.ABORTED) {
      return new InsertResult.Builder().setStatementId(statementId).setState(currentState)
          .setMessage("Statement already in terminal state: " + currentState).build();
    }

    /// Cannot abort a VISIBLE statement (data is already queryable). Surface the real state so the
    /// caller can distinguish "abort failed because already visible" from "abort succeeded".
    if (currentState == InsertStatementState.VISIBLE) {
      return errorResult(statementId, InsertStatementState.VISIBLE, InsertErrorCode.INVALID_STATE,
          "Cannot abort a VISIBLE statement. Data is already queryable.");
    }

    /// CAS-flip the manifest to ABORTED first. Only after the CAS succeeds do we invoke the
    /// executor's best-effort cleanup hook. This ordering prevents a TOCTOU where a concurrent
    /// VISIBLE transition wins between our state-read and our executor.abort call: we'd be running
    /// segment-cleanup against live, queryable data. With the CAS-first ordering, executor.abort
    /// is only ever called after we've durably staked the ABORTED state.
    CasResult abortResult = persistWithCasRetry(tableNameWithType, statementId,
        m -> m.getState() != InsertStatementState.VISIBLE && m.getState() != InsertStatementState.ABORTED,
        fresh -> {
          fresh.setState(InsertStatementState.ABORTED);
          fresh.setErrorMessage("Aborted by user request");
        });
    if (abortResult == CasResult.PRECHECK_FAILED) {
      /// Concurrent writer already reached VISIBLE or ABORTED — return the current state.
      InsertStatementManifest latest = _statementStore.getStatement(tableNameWithType, statementId);
      if (latest != null && latest.getState() == InsertStatementState.VISIBLE) {
        /// Use the 4-arg overload to surface the actual VISIBLE state in the response. The 3-arg
        /// overload defaults state=ABORTED, which would contradict the message saying VISIBLE.
        return errorResult(statementId, InsertStatementState.VISIBLE, InsertErrorCode.INVALID_STATE,
            "Cannot abort a VISIBLE statement. Data is already queryable.");
      }
      return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
          .setMessage("Statement already in terminal state").build();
    }
    if (abortResult != CasResult.SUCCESS) {
      return errorResult(statementId, InsertErrorCode.STATE_PERSIST_ERROR,
          "Could not persist ABORTED state: " + abortResult);
    }

    /// CAS succeeded — manifest is now durably ABORTED. Safe to invoke executor cleanup.
    String executorType = manifest.getInsertType().name();
    InsertExecutor executor = lookupExecutor(executorType);
    if (executor != null) {
      try {
        executor.abort(statementId);
      } catch (Exception e) {
        LOGGER.warn("Executor abort failed for statementId={}", statementId, e);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_ABORT_HOOK_FAILED, 1);
      }
    }

    bumpActiveStatementCount(-1);  /// non-terminal → ABORTED
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());

    LOGGER.info("Statement {} aborted (was in state {})", statementId, currentState);

    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
        .setMessage("Statement aborted").build();
  }

  /// Returns the current status of a statement.
  ///
  /// @param statementId the statement identifier
  /// @param tableNameWithType the table name with type
  /// @return the result reflecting the current state
  public InsertResult getStatus(String statementId, @Nullable String tableNameWithType) {
    if (!_started) {
      return rejectResult(statementId, InsertErrorCode.COORDINATOR_NOT_READY,
          "InsertStatementCoordinator is not started (feature disabled or controller starting/stopping)");
    }
    InsertStatementManifest manifest;
    if (tableNameWithType != null) {
      manifest = _statementStore.getStatement(tableNameWithType, statementId);
    } else {
      /// Cross-table scan: iterates every table that has insert statements in ZK.
      /// Callers should provide tableNameWithType when known to avoid this scan.
      manifest = _statementStore.findStatementAcrossTables(statementId);
    }
    if (manifest == null) {
      return rejectResult(statementId, InsertErrorCode.NOT_FOUND, "Statement not found: " + statementId);
    }

    return new InsertResult.Builder().setStatementId(statementId).setState(manifest.getState())
        .setMessage(manifest.getErrorMessage())
        .setInformationalMessage(manifest.getInformationalMessage())
        .setSegmentNames(manifest.getSegmentNames()).build();
  }

  /// Lists all statements for a given table.
  ///
  /// @param tableNameWithType the table name with type
  /// @return list of results for all statements
  public List<InsertResult> listStatements(String tableNameWithType) {
    if (!_started) {
      return Collections.emptyList();
    }
    List<InsertStatementManifest> manifests = _statementStore.listStatements(tableNameWithType);
    List<InsertResult> results = new ArrayList<>();
    for (InsertStatementManifest manifest : manifests) {
      results.add(new InsertResult.Builder().setStatementId(manifest.getStatementId()).setState(manifest.getState())
          .setMessage(manifest.getErrorMessage())
          .setInformationalMessage(manifest.getInformationalMessage())
          .setSegmentNames(manifest.getSegmentNames()).build());
    }
    return results;
  }

  /// Completes a file-based INSERT statement by finalizing the segment lineage and transitioning
  /// the manifest to VISIBLE. This is called when the Minion task has finished generating and
  /// pushing segments.
  ///
  /// @param statementId    the statement to complete
  /// @param segmentNames   the segment names produced by the task
  /// @return the result reflecting the new state
  public InsertResult completeFileInsert(String statementId, List<String> segmentNames) {
    return completeFileInsert(statementId, null, segmentNames);
  }

  /// Variant that accepts an optional `tableNameWithType` to avoid the cross-table ZK scan in
  /// {@link #findStatementAcrossTables}. The Minion task knows the target table at push time, so
  /// passing it through saves O(N_tables) ZK reads per completion call.
  public InsertResult completeFileInsert(String statementId, @Nullable String tableNameWithType,
      List<String> segmentNames) {
    if (!_started) {
      return rejectResult(statementId, InsertErrorCode.COORDINATOR_NOT_READY,
          "InsertStatementCoordinator is not started (feature disabled or controller starting/stopping)");
    }
    /// Sanity-validate segment names. Real names come from the Minion task and follow standard
    /// Pinot segment naming (alnum, underscore, dash, dot). Reject anything else so that a caller
    /// with UPDATE access cannot smuggle paths or control characters into a foreign manifest's
    /// recorded segment list. Tighter authentication (mTLS, per-task tokens) is the proper v2 fix
    /// — this is the v1 minimal injection guard.
    if (segmentNames != null) {
      /// Bound the list size and per-name length so a caller with UPDATE access cannot balloon
      /// the manifest's ZNRecord beyond ZK's 1MB-per-znode limit.
      if (segmentNames.size() > MAX_SEGMENT_NAMES_PER_COMPLETION) {
        return rejectResult(statementId, InsertErrorCode.INVALID_SEGMENT_NAME,
            "segmentNames list size " + segmentNames.size() + " exceeds the per-completion cap "
                + MAX_SEGMENT_NAMES_PER_COMPLETION + ". Split the upload into multiple inserts.");
      }
      for (String segmentName : segmentNames) {
        if (segmentName == null || segmentName.isEmpty() || !SEGMENT_NAME_PATTERN.matcher(segmentName).matches()) {
          return rejectResult(statementId, InsertErrorCode.INVALID_SEGMENT_NAME,
              "Segment name '" + segmentName + "' is not a valid Pinot segment identifier "
                  + "(allowed: alphanumeric, underscore, dash, dot)");
        }
        if (segmentName.length() > MAX_SEGMENT_NAME_LENGTH) {
          return rejectResult(statementId, InsertErrorCode.INVALID_SEGMENT_NAME,
              "Segment name length " + segmentName.length() + " exceeds the cap "
                  + MAX_SEGMENT_NAME_LENGTH + " (segment-name-prefix: '"
                  + segmentName.substring(0, Math.min(64, segmentName.length())) + "...')");
        }
      }
    }
    /// When the caller provides an explicit tableNameWithType, scope the lookup strictly to that
    /// table — the REST layer authorized the request against that table name, and falling back to
    /// findStatementAcrossTables would let a caller with UPDATE access on table A complete a
    /// statement that actually belongs to table B. The fallback path is reachable only via the
    /// 2-arg internal overload (no tableName supplied), which the REST resource refuses.
    InsertStatementManifest manifest;
    if (tableNameWithType != null && !tableNameWithType.isEmpty()) {
      manifest = _statementStore.getStatement(tableNameWithType, statementId);
      if (manifest == null) {
        return rejectResult(statementId, InsertErrorCode.NOT_FOUND,
            "Statement not found in table " + tableNameWithType + ": " + statementId);
      }
      /// Defense-in-depth: assert the manifest's actual table matches the auth-scoped parameter.
      /// getStatement is keyed by (table, statementId) so a mismatch would normally return null,
      /// but if a future refactor were to relax that key (e.g. introduce statementId aliasing),
      /// this guard prevents a caller with UPDATE access on table A from completing a statement
      /// belonging to table B.
      if (!tableNameWithType.equals(manifest.getTableNameWithType())) {
        LOGGER.error("completeFileInsert table mismatch: caller supplied {} but manifest belongs "
                + "to {} (statementId={})",
            tableNameWithType, manifest.getTableNameWithType(), statementId);
        return rejectResult(statementId, InsertErrorCode.NOT_FOUND,
            "Statement not found in table " + tableNameWithType + ": " + statementId);
      }
    } else {
      manifest = _statementStore.findStatementAcrossTables(statementId);
      if (manifest == null) {
        return rejectResult(statementId, InsertErrorCode.NOT_FOUND, "Statement not found: " + statementId);
      }
    }

    tableNameWithType = manifest.getTableNameWithType();

    /// Anti-hijack: every supplied segment name must already exist in the table's IdealState. The
    /// Minion task's SegmentGenerationAndPushTask calls addNewSegment as it produces each segment,
    /// so by the time /insert/complete is invoked, every legitimate name is already registered.
    /// A caller with UPDATE-access who fabricates a name (or claims an existing victim segment)
    /// would otherwise be able to attach foreign segments to this manifest's reported list.
    /// Fail-closed: if we cannot read IdealState (transient ZK error, etc.), we cannot verify the
    /// anti-hijack invariant — reject the completion. The Minion task can retry /insert/complete
    /// once ZK recovers; the caller's data is unaffected because the cleanup sweep's auto-complete
    /// path also proceeds independently.
    if (segmentNames != null && !segmentNames.isEmpty()) {
      Set<String> registered;
      try {
        registered = new HashSet<>(_helixResourceManager.getSegmentsFor(tableNameWithType, false));
      } catch (Exception e) {
        LOGGER.error("completeFileInsert failed-closed: could not read IdealState for table {} "
            + "during segment anti-hijack cross-check (statementId={})",
            tableNameWithType, statementId, e);
        return rejectResult(statementId, InsertErrorCode.STORE_ERROR,
            "Could not verify supplied segments against IdealState for table " + tableNameWithType
                + " — retry /insert/complete after ZK recovers, or wait for the cleanup sweep "
                + "auto-complete path. Reason: " + e.getMessage());
      }
      for (String name : segmentNames) {
        if (!registered.contains(name)) {
          LOGGER.warn("completeFileInsert rejected: segment '{}' is not registered in IdealState "
                  + "for table {} (statementId={})", name, tableNameWithType, statementId);
          return rejectResult(statementId, InsertErrorCode.INVALID_SEGMENT_NAME,
              "Segment '" + name + "' is not registered in IdealState for table " + tableNameWithType
                  + ". The Minion task's segments must be visible before /insert/complete.");
        }
      }
    }

    /// Delegate to the file executor for lineage finalization
    InsertExecutor executor = lookupExecutor(InsertType.FILE.name());
    if (executor == null) {
      return rejectResult(statementId, InsertErrorCode.NO_EXECUTOR, "No FILE executor registered");
    }

    if (!(executor instanceof FileInsertExecutor)) {
      return rejectResult(statementId, InsertErrorCode.WRONG_EXECUTOR, "FILE executor is not a FileInsertExecutor");
    }

    /// Pass the ZK manifest so the executor can recover lineage state after controller failover
    InsertResult result = ((FileInsertExecutor) executor).prepareCompletionResult(statementId, segmentNames, manifest);

    /// Update the ZK manifest to reflect the executor result. Use CAS retry + precheck so a
    /// concurrent user abort is not overwritten with VISIBLE.
    if (result.getState() == InsertStatementState.VISIBLE) {
      /// FILE-insert flow goes ACCEPTED -> VISIBLE in v1; PREPARED/COMMITTED are not entered for
      /// the file path, so the precondition is exactly ACCEPTED to mirror the executor contract.
      CasResult cas = persistWithCasRetry(tableNameWithType, statementId,
          m -> m.getState() == InsertStatementState.ACCEPTED,
          fresh -> {
            fresh.setState(InsertStatementState.VISIBLE);
            fresh.setSegmentNames(segmentNames);
          });
      if (cas == CasResult.PRECHECK_FAILED) {
        LOGGER.warn("completeFileInsert: statement {} was already in a terminal state; ignoring completion",
            statementId);
        return errorResult(statementId, InsertErrorCode.CONCURRENT_STATE_CHANGE,
            "Statement was already in a terminal state; completion ignored");
      }
      if (cas != CasResult.SUCCESS) {
        return errorResult(statementId, InsertErrorCode.STATE_PERSIST_ERROR,
            "Could not persist VISIBLE state: " + cas);
      }
      /// Persist succeeded — safe to drop the in-memory task-name cache so a future stale poll
      /// doesn't try to look up a finished task.
      ((FileInsertExecutor) executor).taskCompleted(statementId);
      bumpActiveStatementCount(-1);  /// ACCEPTED → VISIBLE
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_VISIBLE, 1);
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
    } else if (result.getState() == InsertStatementState.ABORTED) {
      /// Tighten precondition to match the user-abort path: only flip from non-terminal states.
      /// A broader precondition (e.g., "anything not VISIBLE") would let a task-failure abort
      /// overwrite COMMITTED — currently unreachable but keeping the invariant tight prevents
      /// future drift as the state machine evolves.
      CasResult cas = persistWithCasRetry(tableNameWithType, statementId,
          m -> m.getState() != InsertStatementState.VISIBLE
              && m.getState() != InsertStatementState.ABORTED,
          fresh -> {
            fresh.setState(InsertStatementState.ABORTED);
            fresh.setErrorMessage(result.getMessage());
          });
      if (cas == CasResult.SUCCESS) {
        bumpActiveStatementCount(-1);  /// non-terminal → ABORTED
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
        /// SPI contract: every transition into ABORTED must invoke executor.abort() so plugin
        /// implementations (and the in-tree FileInsertExecutor's _taskNames cache) can release
        /// resources. Read the post-CAS manifest so the hook receives the persisted state.
        InsertStatementManifest abortedManifest = _statementStore.getStatement(tableNameWithType, statementId);
        if (abortedManifest != null) {
          delegateAbortToExecutor(abortedManifest);
        }
      } else if (cas == CasResult.PRECHECK_FAILED) {
        /// Concurrent writer already reached a terminal state. Surface that to the caller rather
        /// than masquerading the executor's intended ABORTED state over the actual ZK truth.
        InsertStatementManifest latest = _statementStore.getStatement(tableNameWithType, statementId);
        InsertStatementState actual = latest != null ? latest.getState() : InsertStatementState.ABORTED;
        LOGGER.info("completeFileInsert ABORTED CAS skipped for {}: manifest already in state {}",
            statementId, actual);
        return errorResult(statementId, actual, InsertErrorCode.CONCURRENT_STATE_CHANGE,
            "Statement already reached terminal state " + actual + "; abort skipped");
      } else if (cas == CasResult.NOT_FOUND) {
        LOGGER.warn("completeFileInsert ABORTED CAS could not find manifest for statementId={}", statementId);
        return errorResult(statementId, InsertErrorCode.NOT_FOUND,
            "Manifest disappeared during ABORTED transition: " + statementId);
      } else if (cas == CasResult.VERSION_CONFLICT) {
        LOGGER.error("completeFileInsert ABORTED CAS exhausted retries for statementId={}", statementId);
        /// Defense-in-depth: drop the FILE executor's _taskNames cache so it doesn't accumulate
        /// entries when the cleanup sweep eventually reconciles the manifest. The cache is local
        /// to this controller; reconciliation only updates ZK.
        if (executor instanceof FileInsertExecutor) {
          ((FileInsertExecutor) executor).taskCompleted(statementId);
        }
        return errorResult(statementId, InsertErrorCode.STATE_PERSIST_ERROR,
            "Could not persist ABORTED state after CAS retries; cleanup sweep will reconcile");
      }
    }

    return result;
  }

  /// Resolves a raw table name and optional table type to a fully qualified table name with type.
  ///
  /// @param tableName the raw table name (may or may not have a type suffix)
  /// @param tableType the explicit table type, or null for auto-detection
  /// @return the resolved table name with type suffix
  /// @throws IllegalArgumentException if the table does not exist or is ambiguous
  @VisibleForTesting
  String resolveTableName(String tableName, @Nullable TableType tableType) {
    /// If the table name already has a type suffix, use it directly — but require an explicit
    /// tableType when the underlying raw table is hybrid. Without this guard, "INSERT INTO t_OFFLINE"
    /// against a hybrid t bypasses the hybrid-table check below; hybrid integrity demands the
    /// operator confirm intent rather than relying on a string suffix that may have been typed by
    /// mistake.
    TableType existingType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (existingType != null) {
      String tableNameWithType = tableName;
      if (!_helixResourceManager.hasTable(tableNameWithType)) {
        throw new IllegalArgumentException("Table does not exist: " + tableNameWithType);
      }
      String rawForHybridCheck = TableNameBuilder.extractRawTableName(tableNameWithType);
      boolean hasOffline = _helixResourceManager.hasOfflineTable(rawForHybridCheck);
      boolean hasRealtime = _helixResourceManager.hasRealtimeTable(rawForHybridCheck);
      if (hasOffline && hasRealtime && (tableType == null || tableType != existingType)) {
        throw new IllegalArgumentException(
            "Table '" + rawForHybridCheck + "' is a hybrid table. The "
                + tableNameWithType + " suffix alone is not enough — also specify tableType="
                + existingType + " via SET tableType='" + existingType + "' to confirm intent.");
      }
      return tableNameWithType;
    }

    /// Raw table name without type suffix
    String rawTableName = tableName;
    boolean hasOffline = _helixResourceManager.hasOfflineTable(rawTableName);
    boolean hasRealtime = _helixResourceManager.hasRealtimeTable(rawTableName);

    if (!hasOffline && !hasRealtime) {
      throw new IllegalArgumentException("Table does not exist: " + rawTableName);
    }

    if (hasOffline && hasRealtime) {
      /// Hybrid table: require explicit table type
      if (tableType == null) {
        throw new IllegalArgumentException(
            "Table '" + rawTableName + "' is a hybrid table. Please specify tableType (OFFLINE or REALTIME) "
                + "via SET tableType='OFFLINE' or SET tableType='REALTIME'");
      }
      return TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
    }

    /// Only one type exists
    if (tableType != null) {
      /// Validate the explicit type matches what exists
      String requested = TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
      if (!_helixResourceManager.hasTable(requested)) {
        throw new IllegalArgumentException(
            "Table '" + requested + "' does not exist. The table exists as " + (hasOffline ? "OFFLINE" : "REALTIME"));
      }
      return requested;
    }

    /// Auto-detect: use the one that exists
    return hasOffline ? TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)
        : TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
  }

  /// Handles idempotency when a request with the same requestId is submitted again.
  private InsertResult handleIdempotency(InsertRequest request, InsertStatementManifest existing) {
    /// Same requestId + matching payloadHash (including both-null) = return existing result.
    /// Both-null is a legitimate idempotency case for callers that don't compute a payload hash.
    if (Objects.equals(request.getPayloadHash(), existing.getPayloadHash())) {
      LOGGER.info("Idempotent request detected for requestId={}, returning existing statementId={}",
          request.getRequestId(), existing.getStatementId());
      return new InsertResult.Builder().setStatementId(existing.getStatementId()).setState(existing.getState())
          .setMessage("Idempotent request: returning existing result").setSegmentNames(existing.getSegmentNames())
          .build();
    }

    /// Same requestId + different payloadHash = pre-acceptance reject. No new manifest is created
    /// for this submission — the existing manifest belongs to the prior caller; we surface
    /// state=REJECTED so clients can distinguish "your request was never accepted" from "your
    /// accepted request was aborted." Matches the InsertErrorCode pre-acceptance contract.
    LOGGER.warn("Duplicate requestId={} with different payloadHash. Existing={}, new={}", request.getRequestId(),
        existing.getPayloadHash(), request.getPayloadHash());
    return rejectResult(request.getStatementId(), InsertErrorCode.IDEMPOTENCY_CONFLICT,
        "Request id '" + request.getRequestId() + "' already used with a different payload");
  }

  /// Background task: sweeps all tables for stuck or completed statements.
  ///
  /// Failover-safe: enumerates tables directly from ZK (the property store) rather than relying
  /// solely on the in-memory `_tablesWithStatements` set. After a controller restart, the
  /// first sweep picks up all tables that still have manifests in ZK.
  @VisibleForTesting
  void cleanupAllTables() {
    /// Increment before _started check so stop()'s drain observes any sweep that gets past the
    /// gate, mirroring submitInsert's pattern. Without this, a sweep that read _started=true just
    /// before stop() flipped it would silently mutate ZK after stop() returned.
    _inflightSweepCount.incrementAndGet();
    try {
      if (!_started) {
        /// stop() raced with the scheduler — bail rather than mutate ZK while the coordinator is
        /// officially stopped.
        return;
      }
      LOGGER.debug("Insert statement cleanup sweep started");

      /// Enumerate tables from ZK to pick up tables persisted before this controller started.
      /// This makes cleanup resilient to controller failover.
      List<String> zkTables = _statementStore.listTablesWithStatements();
      Set<String> tables = ConcurrentHashMap.newKeySet();
      tables.addAll(_tablesWithStatements);
      tables.addAll(zkTables);

      /// Update the in-memory set so subsequent sweeps don't need to re-enumerate
      _tablesWithStatements.addAll(zkTables);

      try {
        for (String table : tables) {
          /// Re-check _started between iterations: stop() may have flipped the flag while we were
          /// working on a prior table. Continuing would mutate ZK against a closing property store
          /// and risk landing writes after stop() returns — breaking failover semantics.
          if (!_started) {
            LOGGER.info("Cleanup sweep aborting mid-iteration: coordinator stop() requested");
            break;
          }
          try {
            cleanupStatementsForTable(table);
          } catch (Throwable t) {
            /// Catch Throwable, not just Exception: a ScheduledExecutorService cancels future
            /// executions silently when an Error (e.g., OutOfMemoryError) escapes from a task.
            /// Log + count + continue so a single bad table doesn't stop the sweep for everyone.
            /// We deliberately do NOT rethrow Error: if the JVM is genuinely OOM, the next
            /// allocation will trigger the JVM's own handler. Re-throwing here would skip
            /// remaining tables AND the final gauge publish AND silently cancel future scheduling.
            LOGGER.error("Error during cleanup for table {}", table, t);
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_CLEANUP_SWEEP_FAILURES, 1);
          }
        }
      } finally {
        /// Periodic reseed: reconcile any drift between the incremental counter and ZK truth.
        /// Drift sources are rare (e.g., EXECUTOR_ERROR_BUT_DURABLE speculative decrement when the
        /// manifest re-read races with a concurrent writer), but without periodic reconciliation
        /// they would persist until the next controller restart. We reseed at the END of the sweep
        /// so any in-sweep transitions (cleanup-driven ACCEPTED->ABORTED/VISIBLE->GC) are reflected
        /// in the ZK count we read.
        long sweepNumber = _cleanupSweepCount.incrementAndGet();
        if (sweepNumber % ACTIVE_COUNT_RESEED_EVERY_N_SWEEPS == 0) {
          try {
            long zkCount = computeActiveStatementCountFromZk();
            long inMemory = _activeStatementCount.getAndSet(zkCount);
            if (inMemory != zkCount) {
              LOGGER.info("Reseeded active-statement counter from ZK on sweep #{}: was={}, now={}",
                  sweepNumber, inMemory, zkCount);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to reseed active-statement counter on sweep #{}; keeping in-memory value",
                sweepNumber, e);
          }
        }
        /// Publish the gauge in finally so a bug or Error escaping the for-loop body doesn't leave
        /// the gauge stale across the next 5-minute interval. The counter is incrementally
        /// maintained by hot-path and cleanup transitions; this call only re-emits the latest
        /// value to the metric backend.
        _controllerMetrics.setValueOfGlobalGauge(
            ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
      }
    } catch (Exception e) {
      LOGGER.error("Error during insert statement cleanup sweep", e);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_CLEANUP_SWEEP_FAILURES, 1);
    } finally {
      _inflightSweepCount.decrementAndGet();
    }
  }

  /// Cleans up stuck and completed statements for a specific table.
  ///
  /// Handles:
  /// - ACCEPTED/PREPARED stuck beyond timeout -> ABORTED
  /// - COMMITTED stuck beyond committed timeout -> ABORTED (partial commit safety)
  /// - VISIBLE beyond retention period -> GC (manifest deleted from ZK)
  /// - ABORTED beyond retention period -> GC (manifest deleted from ZK)
  ///
  /// @param tableNameWithType the table to clean up
  /// @return the number of statements whose state was changed
  public int cleanupStatementsForTable(String tableNameWithType) {
    List<InsertStatementManifest> manifests = _statementStore.listStatements(tableNameWithType);
    long now = System.currentTimeMillis();
    int changedCount = 0;
    int deletedCount = 0;

    for (InsertStatementManifest manifest : manifests) {
      InsertStatementState state = manifest.getState();
      long age = now - manifest.getLastUpdatedTimeMs();

      /// Defensive guard against forward-incompatible / corrupt manifests reaching the sweep:
      /// listStatements already rejects forward schemaVersion at deserialization, but a missing
      /// state or insertType slipping through Jackson defaulting must not be silently skipped on
      /// every sweep cycle (would re-iterate the same broken record forever).
      if (state == null || manifest.getInsertType() == null) {
        LOGGER.error("Cleanup sweep: manifest {} for table {} has null state ({}) or insertType ({}); "
                + "treating as corrupt — operator inspection required",
            manifest.getStatementId(), tableNameWithType, state, manifest.getInsertType());
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_CLEANUP_SWEEP_FAILURES, 1);
        continue;
      }

      switch (state) {
        case ACCEPTED:
          /// For FILE inserts, check whether the Minion task has completed before applying the
          /// timeout. Without this check a successful task has no mechanism to transition the
          /// statement from ACCEPTED to VISIBLE — it would be incorrectly aborted after timeout.
          if (manifest.getInsertType() == InsertType.FILE) {
            InsertStatementState taskResolution = autoCompleteFileInsertIfTaskDone(manifest);
            if (taskResolution == InsertStatementState.VISIBLE) {
              changedCount++;
              break;
            } else if (taskResolution == InsertStatementState.ABORTED) {
              /// Task failed — abort the manifest. CAS-first ordering: a concurrent user
              /// markVisible (impossible from ACCEPTED but defended for safety) or another
              /// controller's cleanup must not lose the executor cleanup.
              LOGGER.warn("Aborting file insert {} because its Minion task failed", manifest.getStatementId());
              CasResult cas = persistWithCasRetry(tableNameWithType, manifest.getStatementId(),
                  m -> m.getState() == InsertStatementState.ACCEPTED,
                  fresh -> {
                    fresh.setState(InsertStatementState.ABORTED);
                    fresh.setErrorMessage("Aborted because Minion task ended in a failure state");
                  });
              if (cas == CasResult.SUCCESS) {
                /// Re-read the manifest after the CAS so the executor's abort hook receives the
                /// post-transition state (ABORTED) rather than the stale local copy (ACCEPTED).
                /// If the re-read returns null (concurrent GC raced), fall back to mutating the
                /// local copy's state to ABORTED so the SPI contract is honored even though the
                /// object isn't a fresh ZK read.
                InsertStatementManifest abortedManifest = _statementStore.getStatement(tableNameWithType,
                    manifest.getStatementId());
                if (abortedManifest == null) {
                  manifest.setState(InsertStatementState.ABORTED);
                  abortedManifest = manifest;
                }
                delegateAbortToExecutor(abortedManifest);
                bumpActiveStatementCount(-1);  /// ACCEPTED → ABORTED via task-failed sweep
                _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
                changedCount++;
              }
              break;
            }
            /// else: task still running — fall through to normal timeout check
          }
          /// Skip the timeout-abort for ROW inserts. The ROW executor's success path uploads and
          /// registers segments BEFORE the coordinator's CAS persists VISIBLE; if that CAS fails
          /// after retries, the manifest stays ACCEPTED while the data is live. Aborting it from
          /// the sweep would falsely report ABORTED to operators reading /insert/status. The ROW
          /// executor's abort() is a documented no-op, so leaving the manifest in ACCEPTED is the
          /// correct safety stance until v2 introduces a true two-phase commit. See
          /// ControllerRowInsertExecutor's "single-phase visibility" Javadoc.
          ///
          /// To prevent /insert/list from accumulating stuck rows indefinitely, GC the manifest
          /// after a long retention window. The data remains queryable; only the manifest record
          /// is deleted. Operators have ACCEPTED_ROW_RETENTION_MS (7 days) to investigate.
          if (manifest.getInsertType() == InsertType.ROW) {
            if (age > ACCEPTED_ROW_RETENTION_MS) {
              LOGGER.info("GC'ing stuck-ACCEPTED ROW statement {} for table {} (age={}ms exceeds {}ms)",
                  manifest.getStatementId(), tableNameWithType, age, ACCEPTED_ROW_RETENTION_MS);
              /// CAS precondition: a concurrent user-abort or executor-success path may have
              /// transitioned the manifest to ABORTED/VISIBLE just as we observe it. Re-read with
              /// a CAS-set-self-noop pattern so deleteStatement only proceeds when the state is
              /// still ACCEPTED — otherwise the concurrent terminal-state writer's manifest gets
              /// silently wiped, the user's getStatus returns NOT_FOUND, and the GC counter
              /// double-counts against INSERT_STATEMENTS_ABORTED.
              InsertStatementManifest current = _statementStore.getStatement(tableNameWithType,
                  manifest.getStatementId());
              if (current == null || current.getState() != InsertStatementState.ACCEPTED) {
                LOGGER.info("Stuck-ACCEPTED ROW GC skipped for {}: state changed to {} concurrently",
                    manifest.getStatementId(), current == null ? InsertErrorCode.NOT_FOUND : current.getState());
                break;
              }
              /// KEEP the requestId reservation (do NOT release). Same rationale as the VISIBLE
              /// retention block below: the data uploaded by the executor's success path may still
              /// be live in IdealState even though the manifest's CAS to VISIBLE failed. Releasing
              /// the reservation would let a stale client retry (>retentionMs after the original)
              /// acquire a fresh reservation and re-execute, producing duplicate rows. The
              /// tombstone-prune path will reap the reservation on its own retention schedule.
              if (_statementStore.deleteStatement(tableNameWithType, manifest.getStatementId())) {
                bumpActiveStatementCount(-1);  /// ACCEPTED → GC'd
                _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_GC, 1);
                deletedCount++;
              } else {
                _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_GC_FAILED, 1);
              }
            }
            break;
          }
          if (age > _statementTimeoutMs) {
            /// FILE-typed manifests with no registered executor (controller running with Minion
            /// task manager disabled) cannot be safely timeout-aborted — the Minion task may still
            /// be running externally, and aborting the manifest would falsely report failure to
            /// the user while orphan segments potentially go live in IdealState. Skip the abort and
            /// log a WARN; operator must either re-enable Minion or manually reconcile.
            if (manifest.getInsertType() == InsertType.FILE
                && lookupExecutor(InsertType.FILE.name()) == null) {
              LOGGER.warn("Skipping timeout-abort for FILE statement {} in table {} (age={}ms): no "
                  + "FILE executor registered. Re-enable Minion task management or manually clean "
                  + "up orphan ZK manifest.", manifest.getStatementId(), tableNameWithType, age);
              break;
            }
            LOGGER.warn("Aborting stuck statement {} in state {} for table {} (age={}ms)",
                manifest.getStatementId(), state, tableNameWithType, age);

            /// CAS-flip first (matching the user-abort ordering). Only call the executor cleanup
            /// hook on CAS success — a concurrent user-abort or markVisible may have transitioned
            /// the manifest, in which case we must not run side-effecting cleanup against state we
            /// didn't durably claim.
            InsertStatementState observedState = state;
            CasResult cas = persistWithCasRetry(tableNameWithType, manifest.getStatementId(),
                m -> m.getState() == observedState,
                fresh -> {
                  fresh.setState(InsertStatementState.ABORTED);
                  fresh.setErrorMessage("Aborted due to timeout (stuck in " + observedState + ")");
                });
            if (cas == CasResult.SUCCESS) {
              /// Re-read post-CAS manifest so the abort hook receives ABORTED state, not the stale
              /// local ACCEPTED copy. If the re-read returns null (concurrent GC raced), update the
              /// local copy's state to ABORTED so the SPI contract is honored.
              InsertStatementManifest abortedManifest = _statementStore.getStatement(tableNameWithType,
                  manifest.getStatementId());
              if (abortedManifest == null) {
                manifest.setState(InsertStatementState.ABORTED);
                abortedManifest = manifest;
              }
              delegateAbortToExecutor(abortedManifest);
              bumpActiveStatementCount(-1);  /// ACCEPTED/PREPARED → ABORTED via timeout sweep
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
              changedCount++;
            } else if (cas == CasResult.PRECHECK_FAILED) {
              LOGGER.info("Cleanup sweep abort skipped for {}; concurrent writer changed state",
                  manifest.getStatementId());
            }
          }
          break;

        case VISIBLE:
          if (age > _visibleRetentionMs) {
            LOGGER.info("GC'ing completed statement {} for table {} (visible for {}ms)",
                manifest.getStatementId(), tableNameWithType, age);
            /// Delete the manifest but KEEP the requestId reservation. Releasing the reservation
            /// here would let a stale client retry (>retentionMs after the original) acquire a fresh
            /// reservation and re-execute, producing duplicate data. Keeping the reservation makes
            /// the retry hit the "stale-rebind" branch in submitInsert, which detects the GC'd
            /// manifest, finds no winner, and returns an error. Tombstone-equivalent for v1.
            if (_statementStore.deleteStatement(tableNameWithType, manifest.getStatementId())) {
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_GC, 1);
              changedCount++;
              deletedCount++;
            } else {
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_GC_FAILED, 1);
            }
          }
          break;

        case ABORTED:
          if (age > _visibleRetentionMs) {
            LOGGER.info("GC'ing aborted statement {} for table {} (aborted for {}ms)",
                manifest.getStatementId(), tableNameWithType, age);
            /// Content-checked release: only free the reservation if it still points at THIS
            /// aborted manifest. If a concurrent retry rebound the reservation to a fresh statement
            /// (the rebind window in submitInsert), unconditionally releasing here would delete
            /// the new statement's idempotency reservation and enable duplicate execution.
            ///
            /// ORDERING: defer the manifest delete only when release THROWS (ZK transient — leave
            /// both intact for next sweep). If release returns false because the reservation was
            /// legitimately rebound, the reservation no longer references this manifest and we must
            /// still delete the manifest — otherwise the ABORTED manifest leaks forever (every
            /// sweep observes the same rebound reservation and refuses to delete).
            boolean released;
            try {
              released = _statementStore.releaseRequestIdIfEquals(tableNameWithType,
                  manifest.getRequestId(), manifest.getStatementId());
            } catch (RuntimeException e) {
              LOGGER.warn("Skipping releaseRequestId for {}; will retry both release and manifest "
                  + "delete on next sweep", manifest.getStatementId(), e);
              break;
            }
            if (!released) {
              LOGGER.info("Reservation for statement {} was rebound to a fresh requestor; deleting "
                  + "the orphan ABORTED manifest anyway", manifest.getStatementId());
            }
            if (_statementStore.deleteStatement(tableNameWithType, manifest.getStatementId())) {
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_GC, 1);
              changedCount++;
              deletedCount++;
            } else {
              _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_GC_FAILED, 1);
            }
          }
          break;

        default:
          break;
      }
    }

    /// Reclaim reservation tombstones older than retention. releaseRequestIdIfEquals deliberately
    /// writes a tombstone instead of doing an unconditional remove (to avoid wiping a concurrent
    /// rebind), so without this sweep the /INSERT_REQUEST_IDS/{table}/ tree grows forever.
    _statementStore.pruneStaleReservationTombstones(tableNameWithType, _visibleRetentionMs);

    /// Do NOT prune _tablesWithStatements here: a concurrent submit could add a fresh manifest
    /// between our listStatements and the remove, and we'd drop the table from the optimization
    /// set despite live data. The ZK tree is the source of truth — every cleanup sweep enumerates
    /// tables from ZK (cleanupAllTables) and re-adds them, so a stale entry self-heals on the next
    /// pass without risking a window where the sweep skips a table that has live manifests.

    if (changedCount > 0) {
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
    }

    return changedCount;
  }

  /// Registers a table as having insert statements. Called externally (e.g., from REST endpoints)
  /// to ensure the cleanup sweep covers this table.
  public void registerTableForCleanup(String tableNameWithType) {
    _tablesWithStatements.add(tableNameWithType);
  }

  /// Returns the active-statement count. Constant time — backed by {@link #_activeStatementCount}.
  private long getActiveStatementCount() {
    return _activeStatementCount.get();
  }

  /// Adjusts the active-statement count after a state transition. `delta` can be negative.
  private void bumpActiveStatementCount(int delta) {
    _activeStatementCount.addAndGet(delta);
  }

  /// Computes the active count by scanning ZK. Called from the cleanup sweep so any drift between
  /// the in-memory counter and ZK truth is reconciled within one sweep cycle. Also called once at
  /// {@link #start()} so the running counter is seeded from any manifests left behind by a prior
  /// controller leader. Not on the hot path.
  private long computeActiveStatementCountFromZk() {
    /// Lists tables directly from the store (not _tablesWithStatements) because the in-memory set
    /// is empty at start() time — failover recovery requires reading ZK for the canonical table set.
    List<String> tables;
    try {
      tables = _statementStore.listTablesWithStatements();
    } catch (Exception e) {
      LOGGER.warn("Failed to enumerate tables for active-statement count; counter remains at last value", e);
      return _activeStatementCount.get();
    }
    long count = 0;
    for (String table : tables) {
      try {
        List<InsertStatementManifest> manifests = _statementStore.listStatements(table);
        for (InsertStatementManifest m : manifests) {
          InsertStatementState s = m.getState();
          if (s == InsertStatementState.ACCEPTED) {
            count++;
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error counting active statements for table {}", table, e);
      }
    }
    return count;
  }

  /// Checks whether the Minion task for an ACCEPTED FILE insert has completed, and if so
  /// auto-transitions the statement to VISIBLE (or ABORTED on task failure). Called from
  /// the cleanup sweep before applying the normal ACCEPTED-state timeout.
  ///
  /// @param manifest the ACCEPTED FILE insert manifest
  /// @return {@link InsertStatementState#VISIBLE} if auto-completed, {@link InsertStatementState#ABORTED}
  ///     if the task failed, or `null` if the task is still running / status unavailable
  @Nullable
  private InsertStatementState autoCompleteFileInsertIfTaskDone(InsertStatementManifest manifest) {
    InsertExecutor executor = lookupExecutor(InsertType.FILE.name());
    if (!(executor instanceof FileInsertExecutor)) {
      return null;
    }
    FileInsertExecutor fileExecutor = (FileInsertExecutor) executor;
    InsertStatementState resolution =
        fileExecutor.resolveAcceptedStatementIfTaskDone(manifest.getStatementId(), manifest);
    if (resolution == InsertStatementState.VISIBLE) {
      /// Persist VISIBLE state to ZK with CAS+precondition: a concurrent user-abort or another
      /// controller's cleanup sweep must not be silently overwritten.
      CasResult cas = persistWithCasRetry(manifest.getTableNameWithType(), manifest.getStatementId(),
          m -> m.getState() == InsertStatementState.ACCEPTED,
          fresh -> {
            fresh.setState(InsertStatementState.VISIBLE);
            /// v1 limitation: the Minion task's produced segment names are not surfaced into this
            /// sweep path (no callback wiring into SegmentGenerationAndPushTask). When the manifest
            /// ended up with empty segmentNames, stamp an operator-visible advisory on
            /// informationalMessage so /insert/status / /insert/list show the gap rather than
            /// reporting VISIBLE+empty silently.
            if (fresh.getSegmentNames().isEmpty()) {
              fresh.setInformationalMessage("Auto-completed via cleanup sweep without explicit "
                  + "/insert/complete callback; segmentNames not surfaced. Consult IdealState for "
                  + "actual segments produced by the Minion task. v2 will plumb this through.");
            }
            /// Always clear stale errorMessage on a successful auto-complete (regardless of whether
            /// segmentNames were surfaced). Prior transient failures on the ACCEPTED state may have
            /// left an errorMessage; leaving it in place would make a now-VISIBLE statement look
            /// failed to clients reading /insert/status.
            fresh.setErrorMessage(null);
          });
      if (cas == CasResult.SUCCESS) {
        /// Drop the in-memory task-name cache now that the manifest is durably VISIBLE. Without
        /// this, long-lived controllers accumulate _taskNames entries for every file insert that
        /// was auto-completed via the sweep (the dominant code path).
        fileExecutor.taskCompleted(manifest.getStatementId());
        bumpActiveStatementCount(-1);  /// ACCEPTED → VISIBLE via auto-complete sweep
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_VISIBLE, 1);
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
        LOGGER.info("Auto-completed file insert {} to VISIBLE via cleanup sweep", manifest.getStatementId());
      } else {
        LOGGER.info("Auto-complete to VISIBLE skipped for {}: concurrent writer changed state ({})",
            manifest.getStatementId(), cas);
        /// Differentiate the CAS outcomes:
        /// - PRECHECK_FAILED: a concurrent writer (user-abort, success path) reached a terminal
        ///   state. That writer is responsible for dropping the cache. Drop here as defense-in-depth
        ///   only since this branch is rare and the cache entry is otherwise leaked.
        /// - VERSION_CONFLICT: CAS retries exhausted but the manifest may still be ACCEPTED (the
        ///   racing writer could be a metadata-only update that bumped the version). The next sweep
        ///   needs the cache entry to re-poll; do NOT drop here.
        /// - NOT_FOUND: manifest already GC'd; cache entry is dead, drop it.
        if (cas == CasResult.PRECHECK_FAILED || cas == CasResult.NOT_FOUND) {
          fileExecutor.taskCompleted(manifest.getStatementId());
        }
        /// Don't claim VISIBLE if the persist didn't happen.
        return null;
      }
    }
    return resolution;
  }

  /// Best-effort delegation of abort to the executor so it can clean up side effects such as
  /// reverting segment lineage entries or purging the prepared store.
  ///
  /// Invoked on four coordinator-driven terminal-ABORTED paths where the cleanup did not
  /// originate inside the executor itself: task-name-persist failure abort (after successful Minion
  /// schedule, before the manifest is committed), `/insert/complete` REST endpoint reporting
  /// ABORTED, cleanup-sweep task-failed abort, and cleanup-sweep timeout-abort. The user-abort
  /// REST path in `abortStatement` is the exception — it calls
  /// `executor.abort(statementId)` directly inline after the CAS-first state flip, so this
  /// helper is NOT invoked there. Notably NOT invoked when the executor's own `execute()`
  /// returns an ABORTED result either: the executor reported its own terminal state and is
  /// responsible for any cleanup before returning, so a redundant `abort(statementId)` would
  /// re-enter the executor's own cleanup path unnecessarily.
  private void delegateAbortToExecutor(InsertStatementManifest manifest) {
    String executorType = manifest.getInsertType().name();
    InsertExecutor executor = lookupExecutor(executorType);
    if (executor != null) {
      try {
        executor.abort(manifest.getStatementId());
      } catch (Exception e) {
        LOGGER.warn("Executor abort failed during cleanup for statementId={}", manifest.getStatementId(), e);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_ABORT_HOOK_FAILED, 1);
      }
    }
  }

  /// Number of CAS attempts for ZK manifest writes. 3 balances recovery from optimistic-locking
  /// conflicts (common under split-brain failover) against bounded latency.
  private static final int CAS_RETRY_ATTEMPTS = 3;

  /// Result of a {@link #persistWithCasRetry} call.
  enum CasResult {
    /// Write succeeded within the retry budget.
    SUCCESS,
    /// Manifest was not found in ZK (deleted/GC'd before our read).
    NOT_FOUND,
    /// Precondition check returned false — a concurrent writer reached a terminal state first.
    PRECHECK_FAILED,
    /// All {@value #CAS_RETRY_ATTEMPTS} version-checked writes failed.
    VERSION_CONFLICT
  }

  /// Persists a manifest change with a bounded CAS retry loop. Each attempt re-reads the current
  /// ZK state, runs the `preCheck` predicate, applies the mutator if the precondition holds,
  /// and attempts a version-checked write. This guards against optimistic-locking conflicts
  /// (two-write races on the same manifest, e.g., minionTaskName write plus terminal-state write,
  /// or a split-brain failover leader writing concurrently) AND against resurrecting a statement
  /// that a concurrent abort already transitioned to ABORTED.
  ///
  /// @param preCheck invoked on the fresh manifest before the mutator. Returning `false`
  ///   short-circuits with {@link CasResult#PRECHECK_FAILED} — used by callers to refuse invalid
  ///   transitions (e.g., "don't overwrite ABORTED with VISIBLE").
  @VisibleForTesting
  CasResult persistWithCasRetry(String tableNameWithType, String statementId,
      Predicate<InsertStatementManifest> preCheck,
      Consumer<InsertStatementManifest> mutator) {
    for (int attempt = 1; attempt <= CAS_RETRY_ATTEMPTS; attempt++) {
      InsertStatementManifest current = _statementStore.getStatement(tableNameWithType, statementId);
      if (current == null) {
        LOGGER.warn("persistWithCasRetry: manifest not found for statementId={} table={} (attempt {})",
            statementId, tableNameWithType, attempt);
        return CasResult.NOT_FOUND;
      }
      if (!preCheck.test(current)) {
        LOGGER.info("persistWithCasRetry: precheck rejected mutation for statementId={} state={} "
                + "(concurrent writer won)", statementId, current.getState());
        return CasResult.PRECHECK_FAILED;
      }
      mutator.accept(current);
      if (_statementStore.updateStatement(current)) {
        if (attempt > 1) {
          LOGGER.info("persistWithCasRetry succeeded on attempt {} for statementId={}", attempt, statementId);
        }
        return CasResult.SUCCESS;
      }
      LOGGER.warn("persistWithCasRetry attempt {}/{} failed for statementId={} — version conflict; retrying",
          attempt, CAS_RETRY_ATTEMPTS, statementId);
    }
    return CasResult.VERSION_CONFLICT;
  }

  /// Convenience overload for callers that always accept any prior state.
  @VisibleForTesting
  CasResult persistWithCasRetry(String tableNameWithType, String statementId,
      Consumer<InsertStatementManifest> mutator) {
    return persistWithCasRetry(tableNameWithType, statementId, m -> true, mutator);
  }

  /// Releases a requestId reservation when the submission fails, so the client can retry. Uses the
  /// content-checked variant so a concurrent rebind that re-pointed the reservation at a different
  /// statement is preserved — the unconditional release would wipe the rebound reservation,
  /// letting a third caller create a duplicate statement.
  ///
  /// No-op if requestId or tableNameWithType is null.
  ///
  /// @param expectedStatementId the statementId this submission was operating on; the release only
  ///   succeeds if the reservation still points at this id
  private void releaseRequestIdOnFailure(@Nullable String tableNameWithType, @Nullable String requestId,
      @Nullable String expectedStatementId) {
    if (requestId != null && tableNameWithType != null && expectedStatementId != null) {
      try {
        _statementStore.releaseRequestIdIfEquals(tableNameWithType, requestId, expectedStatementId);
      } catch (RuntimeException e) {
        /// Treat infra failures (ZK transient) as best-effort: log and continue. The reservation
        /// will be picked up by the next cleanup sweep when ZK recovers.
        LOGGER.warn("Best-effort releaseRequestId failed for table={} requestId={}; cleanup sweep "
            + "will reconcile", tableNameWithType, requestId, e);
      }
    }
  }


  /// After a lost rebind race, polls for the winner's manifest to become findable. The winner must
  /// complete its `createStatement` call before the loser can surface idempotency, so a
  /// bounded wait is required. Total cap stays at ~100ms — surfacing the winner's terminal state
  /// to the loser avoids a client-side retry cycle and is a worthwhile trade-off for the rare
  /// rebind-race case. Jitter (per the reviewer's recommendation) avoids thundering-herd behavior
  /// across rebind contenders. If the winner has not published in this window, the loser surfaces
  /// `REBIND_RACE_LOST` and the client retries.
  ///
  /// Future v2 should replace this poll with a ZK data-watch on the reservation/manifest path
  /// to eliminate the Jersey thread block entirely.
  @Nullable
  private InsertStatementManifest waitForWinnerManifest(String tableNameWithType, String requestId,
      String ourStatementId) {
    for (int attempt = 0; attempt < WAIT_FOR_WINNER_ATTEMPTS; attempt++) {
      if (!_started) {
        /// stop() was called mid-wait. Don't keep polling against a closing property store.
        return null;
      }
      InsertStatementManifest manifest = _statementStore.findByRequestId(tableNameWithType, requestId);
      if (manifest != null && !manifest.getStatementId().equals(ourStatementId)) {
        return manifest;
      }
      if (attempt == WAIT_FOR_WINNER_ATTEMPTS - 1) {
        break;  /// last attempt, no more sleep
      }
      try {
        Thread.sleep(WAIT_FOR_WINNER_BACKOFF_BASE_MS
            + ThreadLocalRandom.current().nextInt(WAIT_FOR_WINNER_BACKOFF_JITTER_MS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  /// 20 × (10–20ms) = 200–400ms total wait. Tuned to outlast a typical GC pause / kernel scheduling
  /// hiccup on a busy controller so the rebind loser does not surface REBIND_RACE_LOST when the
  /// winner is just slightly delayed between the rebind CAS and the createStatement persist.
  private static final int WAIT_FOR_WINNER_ATTEMPTS = 20;
  private static final int WAIT_FOR_WINNER_BACKOFF_BASE_MS = 10;
  private static final int WAIT_FOR_WINNER_BACKOFF_JITTER_MS = 10;

  /// Pre-acceptance rejection — no manifest exists in ZK. Use for validation failures and rejections
  /// before the manifest is persisted. Callers cannot `getStatus` a REJECTED statementId.
  private static InsertResult rejectResult(String statementId, String errorCode, String message) {
    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.REJECTED)
        .setErrorCode(errorCode).setMessage(message).build();
  }

  /// Post-acceptance error result — used after the manifest was created and reached a terminal state.
  /// Default state is ABORTED; callers in the success-then-persist-failed paths should use the
  /// actualState overload below to surface VISIBLE.
  private static InsertResult errorResult(String statementId, String errorCode, String message) {
    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
        .setErrorCode(errorCode).setMessage(message).build();
  }

  /// Variant for the case where execution actually reached a non-ABORTED state but a downstream
  /// operation (typically the ZK persist) failed. The caller should not see `state=ABORTED`
  /// for data that may already be queryable.
  private static InsertResult errorResult(String statementId, InsertStatementState actualState,
      String errorCode, String message) {
    return new InsertResult.Builder().setStatementId(statementId).setState(actualState)
        .setErrorCode(errorCode).setMessage(message).build();
  }

  @VisibleForTesting
  InsertStatementStore getStatementStore() {
    return _statementStore;
  }
}
