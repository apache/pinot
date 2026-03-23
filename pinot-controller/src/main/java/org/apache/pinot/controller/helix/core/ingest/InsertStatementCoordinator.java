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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central coordinator for the push-based INSERT INTO statement lifecycle.
 *
 * <p>Manages the full state machine (NEW -> ACCEPTED -> PREPARED -> COMMITTED -> VISIBLE -> GC),
 * idempotency checking, hybrid table validation, delegation to {@link InsertExecutor} backends,
 * and background cleanup of stuck statements.
 *
 * <p>The coordinator is resilient to controller failover: on startup, it resumes cleanup from
 * the ZK-persisted state. No in-memory state is required for correctness; the ZK manifests are
 * the source of truth.
 *
 * <p>This class is thread-safe. It is instantiated once during controller startup and shared
 * across REST API handlers.
 */
public class InsertStatementCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementCoordinator.class);

  /** Default timeout for statements stuck in ACCEPTED or PREPARED state (30 minutes). */
  public static final long DEFAULT_STATEMENT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(30);

  /** Default timeout for statements stuck in COMMITTED state (60 minutes). */
  public static final long DEFAULT_COMMITTED_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(60);

  /** Default retention period before VISIBLE statements are moved to GC (24 hours). */
  public static final long DEFAULT_VISIBLE_RETENTION_MS = TimeUnit.HOURS.toMillis(24);

  /** Interval between cleanup sweeps (5 minutes). */
  private static final long CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

  private final PinotHelixResourceManager _helixResourceManager;
  private final InsertStatementStore _statementStore;
  private final ControllerMetrics _controllerMetrics;
  private final Map<String, InsertExecutor> _executors;
  private final long _statementTimeoutMs;
  private final long _committedTimeoutMs;
  private final long _visibleRetentionMs;
  private final ScheduledExecutorService _cleanupScheduler;

  /**
   * Tables that are known to have insert statements. Populated as statements are submitted
   * and during cleanup sweeps. This set is an optimization to avoid scanning all tables; the
   * ZK state is always the source of truth.
   */
  private final Set<String> _tablesWithStatements = ConcurrentHashMap.newKeySet();

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics) {
    this(helixResourceManager, statementStore, controllerMetrics,
        DEFAULT_STATEMENT_TIMEOUT_MS, DEFAULT_COMMITTED_TIMEOUT_MS, DEFAULT_VISIBLE_RETENTION_MS);
  }

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics,
      long statementTimeoutMs, long committedTimeoutMs, long visibleRetentionMs) {
    _helixResourceManager = helixResourceManager;
    _statementStore = statementStore;
    _controllerMetrics = controllerMetrics;
    _executors = new ConcurrentHashMap<>();
    _statementTimeoutMs = statementTimeoutMs;
    _committedTimeoutMs = committedTimeoutMs;
    _visibleRetentionMs = visibleRetentionMs;
    _cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "insert-statement-cleanup");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Starts the background cleanup task for stuck statements.
   * On startup (including after controller failover), the first sweep picks up any
   * stuck statements from ZK.
   */
  public void start() {
    _cleanupScheduler.scheduleWithFixedDelay(this::cleanupAllTables, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
    LOGGER.info("InsertStatementCoordinator started with statementTimeout={}ms, committedTimeout={}ms, "
        + "visibleRetention={}ms", _statementTimeoutMs, _committedTimeoutMs, _visibleRetentionMs);
  }

  /**
   * Stops the coordinator and shuts down the cleanup scheduler.
   */
  public void stop() {
    _cleanupScheduler.shutdownNow();
    LOGGER.info("InsertStatementCoordinator stopped");
  }

  /**
   * Registers an {@link InsertExecutor} for a specific insert type.
   */
  public void registerExecutor(String executorType, InsertExecutor executor) {
    _executors.put(executorType, executor);
    LOGGER.info("Registered InsertExecutor for type={}", executorType);
  }

  /**
   * Submits an INSERT INTO request for execution.
   *
   * <p>Performs idempotency checks, hybrid table validation, creates a manifest,
   * and delegates to the appropriate executor.
   *
   * @param request the insert request
   * @return the result reflecting the initial state of the statement
   */
  public InsertResult submitInsert(InsertRequest request) {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_SUBMITTED, 1);

    // 1. Resolve table name to physical table with type
    String tableNameWithType;
    try {
      tableNameWithType = resolveTableName(request.getTableName(), request.getTableType());
    } catch (IllegalArgumentException e) {
      return errorResult(request.getStatementId(), "TABLE_RESOLUTION_ERROR", e.getMessage());
    }

    // 2. Idempotency check
    if (request.getRequestId() != null) {
      InsertStatementManifest existing = _statementStore.findByRequestId(tableNameWithType, request.getRequestId());
      if (existing != null) {
        return handleIdempotency(request, existing);
      }
    }

    // 3. Check executor availability
    String executorType = request.getInsertType().name();
    InsertExecutor executor = _executors.get(executorType);
    if (executor == null) {
      return errorResult(request.getStatementId(), "NO_EXECUTOR",
          "No InsertExecutor registered for type: " + executorType);
    }

    // 4. Create manifest
    long now = System.currentTimeMillis();
    InsertStatementManifest manifest =
        new InsertStatementManifest(request.getStatementId(), request.getRequestId(),
            request.getPayloadHash(), tableNameWithType, request.getInsertType(),
            InsertStatementState.ACCEPTED, now, now, Collections.emptyList(), null);

    if (!_statementStore.createStatement(manifest)) {
      return errorResult(request.getStatementId(), "STORE_ERROR", "Failed to persist statement manifest in ZooKeeper");
    }

    _tablesWithStatements.add(tableNameWithType);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());

    // 5. Delegate to executor
    try {
      InsertResult executorResult = executor.execute(request);
      // Update manifest based on executor result
      if (executorResult.getState() == InsertStatementState.ABORTED) {
        manifest.setState(InsertStatementState.ABORTED);
        manifest.setErrorMessage(executorResult.getMessage());
        _statementStore.updateStatement(manifest);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
      }
      return executorResult;
    } catch (Exception e) {
      LOGGER.error("Executor failed for statementId={}", request.getStatementId(), e);
      manifest.setState(InsertStatementState.ABORTED);
      manifest.setErrorMessage("Executor error: " + e.getMessage());
      _statementStore.updateStatement(manifest);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
      return errorResult(request.getStatementId(), "EXECUTOR_ERROR", "Executor failed: " + e.getMessage());
    }
  }

  /**
   * Called by an executor when data has been staged and is ready to commit.
   *
   * @param statementId the statement identifier
   * @param tableNameWithType the table name with type
   * @param segmentNames the list of segment names that were prepared
   */
  public void prepareComplete(String statementId, String tableNameWithType, List<String> segmentNames) {
    InsertStatementManifest manifest = _statementStore.getStatement(tableNameWithType, statementId);
    if (manifest == null) {
      LOGGER.warn("prepareComplete called for unknown statementId={}", statementId);
      return;
    }
    if (manifest.getState() != InsertStatementState.ACCEPTED) {
      LOGGER.warn("prepareComplete called for statementId={} in unexpected state {}",
          statementId, manifest.getState());
      return;
    }
    manifest.setState(InsertStatementState.PREPARED);
    manifest.setSegmentNames(segmentNames);
    _statementStore.updateStatement(manifest);
    LOGGER.info("Statement {} moved to PREPARED with segments={}", statementId, segmentNames);
  }

  /**
   * Commits a prepared statement, making its segments visible.
   *
   * @param statementId the statement identifier
   * @param tableNameWithType the table name with type
   * @return the result reflecting the committed state
   */
  public InsertResult commitStatement(String statementId, String tableNameWithType) {
    InsertStatementManifest manifest = _statementStore.getStatement(tableNameWithType, statementId);
    if (manifest == null) {
      return errorResult(statementId, "NOT_FOUND", "Statement not found: " + statementId);
    }

    if (manifest.getState() != InsertStatementState.PREPARED) {
      return errorResult(statementId, "INVALID_STATE",
          "Cannot commit statement in state " + manifest.getState() + ", expected PREPARED");
    }

    manifest.setState(InsertStatementState.COMMITTED);
    _statementStore.updateStatement(manifest);
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_COMMITTED, 1);

    LOGGER.info("Statement {} committed with segments={}", statementId, manifest.getSegmentNames());

    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.COMMITTED)
        .setMessage("Statement committed successfully").setSegmentNames(manifest.getSegmentNames()).build();
  }

  /**
   * Marks a committed statement as visible (segments are now queryable).
   * This should be called after confirming that all replicas have applied the data.
   *
   * @param statementId the statement identifier
   * @param tableNameWithType the table name with type
   */
  public void markVisible(String statementId, String tableNameWithType) {
    InsertStatementManifest manifest = _statementStore.getStatement(tableNameWithType, statementId);
    if (manifest == null) {
      LOGGER.warn("markVisible called for unknown statementId={}", statementId);
      return;
    }
    if (manifest.getState() != InsertStatementState.COMMITTED) {
      LOGGER.warn("markVisible called for statementId={} in unexpected state {} (expected COMMITTED)",
          statementId, manifest.getState());
      // Allow idempotent transition if already VISIBLE
      if (manifest.getState() == InsertStatementState.VISIBLE) {
        return;
      }
      return;
    }
    manifest.setState(InsertStatementState.VISIBLE);
    _statementStore.updateStatement(manifest);

    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_VISIBLE, 1);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
    LOGGER.info("Statement {} is now VISIBLE", statementId);
  }

  /**
   * Aborts a statement, releasing any resources it holds. Double-abort is idempotent.
   *
   * @param statementId the statement identifier
   * @param tableNameWithType the table name with type
   * @return the result reflecting the aborted state
   */
  public InsertResult abortStatement(String statementId, @Nullable String tableNameWithType) {
    // If tableNameWithType is not provided, we need to search for it
    InsertStatementManifest manifest = null;
    if (tableNameWithType != null) {
      manifest = _statementStore.getStatement(tableNameWithType, statementId);
    }

    if (manifest == null) {
      return errorResult(statementId, "NOT_FOUND", "Statement not found: " + statementId);
    }

    InsertStatementState currentState = manifest.getState();

    // Double-abort idempotency: already in terminal state
    if (currentState == InsertStatementState.ABORTED || currentState == InsertStatementState.GC) {
      return new InsertResult.Builder().setStatementId(statementId).setState(currentState)
          .setMessage("Statement already in terminal state: " + currentState).build();
    }

    // Cannot abort a VISIBLE statement (data is already queryable)
    if (currentState == InsertStatementState.VISIBLE) {
      return errorResult(statementId, "INVALID_STATE",
          "Cannot abort a VISIBLE statement. Data is already queryable.");
    }

    // Delegate abort to executor if applicable
    String executorType = manifest.getInsertType().name();
    InsertExecutor executor = _executors.get(executorType);
    if (executor != null) {
      try {
        executor.abort(statementId);
      } catch (Exception e) {
        LOGGER.warn("Executor abort failed for statementId={}", statementId, e);
      }
    }

    manifest.setState(InsertStatementState.ABORTED);
    manifest.setErrorMessage("Aborted by user request");
    _statementStore.updateStatement(manifest);

    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());

    LOGGER.info("Statement {} aborted (was in state {})", statementId, currentState);

    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
        .setMessage("Statement aborted").build();
  }

  /**
   * Returns the current status of a statement.
   *
   * @param statementId the statement identifier
   * @param tableNameWithType the table name with type
   * @return the result reflecting the current state
   */
  public InsertResult getStatus(String statementId, String tableNameWithType) {
    InsertStatementManifest manifest = _statementStore.getStatement(tableNameWithType, statementId);
    if (manifest == null) {
      return errorResult(statementId, "NOT_FOUND", "Statement not found: " + statementId);
    }

    return new InsertResult.Builder().setStatementId(statementId).setState(manifest.getState())
        .setMessage(manifest.getErrorMessage()).setSegmentNames(manifest.getSegmentNames()).build();
  }

  /**
   * Lists all statements for a given table.
   *
   * @param tableNameWithType the table name with type
   * @return list of results for all statements
   */
  public List<InsertResult> listStatements(String tableNameWithType) {
    List<InsertStatementManifest> manifests = _statementStore.listStatements(tableNameWithType);
    List<InsertResult> results = new ArrayList<>();
    for (InsertStatementManifest manifest : manifests) {
      results.add(new InsertResult.Builder().setStatementId(manifest.getStatementId()).setState(manifest.getState())
          .setMessage(manifest.getErrorMessage()).setSegmentNames(manifest.getSegmentNames()).build());
    }
    return results;
  }

  /**
   * Resolves a raw table name and optional table type to a fully qualified table name with type.
   *
   * @param tableName the raw table name (may or may not have a type suffix)
   * @param tableType the explicit table type, or null for auto-detection
   * @return the resolved table name with type suffix
   * @throws IllegalArgumentException if the table does not exist or is ambiguous
   */
  @VisibleForTesting
  String resolveTableName(String tableName, @Nullable TableType tableType) {
    // If the table name already has a type suffix, use it directly
    TableType existingType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (existingType != null) {
      String tableNameWithType = tableName;
      if (!_helixResourceManager.hasTable(tableNameWithType)) {
        throw new IllegalArgumentException("Table does not exist: " + tableNameWithType);
      }
      return tableNameWithType;
    }

    // Raw table name without type suffix
    String rawTableName = tableName;
    boolean hasOffline = _helixResourceManager.hasOfflineTable(rawTableName);
    boolean hasRealtime = _helixResourceManager.hasRealtimeTable(rawTableName);

    if (!hasOffline && !hasRealtime) {
      throw new IllegalArgumentException("Table does not exist: " + rawTableName);
    }

    if (hasOffline && hasRealtime) {
      // Hybrid table: require explicit table type
      if (tableType == null) {
        throw new IllegalArgumentException(
            "Table '" + rawTableName + "' is a hybrid table. Please specify tableType (OFFLINE or REALTIME) "
                + "via SET tableType='OFFLINE' or SET tableType='REALTIME'");
      }
      return TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
    }

    // Only one type exists
    if (tableType != null) {
      // Validate the explicit type matches what exists
      String requested = TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
      if (!_helixResourceManager.hasTable(requested)) {
        throw new IllegalArgumentException(
            "Table '" + requested + "' does not exist. The table exists as " + (hasOffline ? "OFFLINE" : "REALTIME"));
      }
      return requested;
    }

    // Auto-detect: use the one that exists
    return hasOffline ? TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)
        : TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
  }

  /**
   * Handles idempotency when a request with the same requestId is submitted again.
   */
  private InsertResult handleIdempotency(InsertRequest request, InsertStatementManifest existing) {
    // Same requestId + same payloadHash = return existing result
    if (request.getPayloadHash() != null && request.getPayloadHash().equals(existing.getPayloadHash())) {
      LOGGER.info("Idempotent request detected for requestId={}, returning existing statementId={}",
          request.getRequestId(), existing.getStatementId());
      return new InsertResult.Builder().setStatementId(existing.getStatementId()).setState(existing.getState())
          .setMessage("Idempotent request: returning existing result").setSegmentNames(existing.getSegmentNames())
          .build();
    }

    // Same requestId + different payloadHash = error
    LOGGER.warn("Duplicate requestId={} with different payloadHash. Existing={}, new={}", request.getRequestId(),
        existing.getPayloadHash(), request.getPayloadHash());
    return errorResult(request.getStatementId(), "IDEMPOTENCY_CONFLICT",
        "Request id '" + request.getRequestId() + "' already used with a different payload");
  }

  /**
   * Background task: sweeps all known tables for stuck or completed statements.
   * This method is also effective after controller failover because it reads all state from ZK.
   */
  @VisibleForTesting
  void cleanupAllTables() {
    try {
      LOGGER.debug("Insert statement cleanup sweep started");
      // Sweep all tables that are known to have statements.
      // Copy the set to avoid ConcurrentModificationException.
      Set<String> tables = ConcurrentHashMap.newKeySet();
      tables.addAll(_tablesWithStatements);
      for (String table : tables) {
        try {
          cleanupStatementsForTable(table);
        } catch (Exception e) {
          LOGGER.error("Error during cleanup for table {}", table, e);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error during insert statement cleanup sweep", e);
    }
  }

  /**
   * Cleans up stuck and completed statements for a specific table.
   *
   * <p>Handles:
   * <ul>
   *   <li>ACCEPTED/PREPARED stuck beyond timeout -> ABORTED</li>
   *   <li>COMMITTED stuck beyond committed timeout -> ABORTED (partial commit safety)</li>
   *   <li>VISIBLE beyond retention period -> GC (manifest deleted from ZK)</li>
   *   <li>ABORTED beyond retention period -> GC (manifest deleted from ZK)</li>
   * </ul>
   *
   * @param tableNameWithType the table to clean up
   * @return the number of statements whose state was changed
   */
  public int cleanupStatementsForTable(String tableNameWithType) {
    List<InsertStatementManifest> manifests = _statementStore.listStatements(tableNameWithType);
    long now = System.currentTimeMillis();
    int changedCount = 0;

    for (InsertStatementManifest manifest : manifests) {
      InsertStatementState state = manifest.getState();
      long age = now - manifest.getLastUpdatedTimeMs();

      switch (state) {
        case ACCEPTED:
        case PREPARED:
          if (age > _statementTimeoutMs) {
            LOGGER.warn("Aborting stuck statement {} in state {} for table {} (age={}ms)",
                manifest.getStatementId(), state, tableNameWithType, age);
            manifest.setState(InsertStatementState.ABORTED);
            manifest.setErrorMessage("Aborted due to timeout (stuck in " + state + ")");
            _statementStore.updateStatement(manifest);
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
            changedCount++;
          }
          break;

        case COMMITTED:
          if (age > _committedTimeoutMs) {
            LOGGER.warn("Aborting stuck COMMITTED statement {} for table {} (age={}ms). "
                    + "Servers may have failed to apply.",
                manifest.getStatementId(), tableNameWithType, age);
            manifest.setState(InsertStatementState.ABORTED);
            manifest.setErrorMessage("Aborted due to timeout (stuck in COMMITTED, servers may have failed to apply)");
            _statementStore.updateStatement(manifest);
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
            changedCount++;
          }
          break;

        case VISIBLE:
          if (age > _visibleRetentionMs) {
            LOGGER.info("GC'ing completed statement {} for table {} (visible for {}ms)",
                manifest.getStatementId(), tableNameWithType, age);
            _statementStore.deleteStatement(tableNameWithType, manifest.getStatementId());
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_GC, 1);
            changedCount++;
          }
          break;

        case ABORTED:
          if (age > _visibleRetentionMs) {
            LOGGER.info("GC'ing aborted statement {} for table {} (aborted for {}ms)",
                manifest.getStatementId(), tableNameWithType, age);
            _statementStore.deleteStatement(tableNameWithType, manifest.getStatementId());
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_GC, 1);
            changedCount++;
          }
          break;

        case GC:
          // GC state should have been deleted already; delete the ZK node
          LOGGER.info("Deleting leftover GC manifest for statement {} on table {}",
              manifest.getStatementId(), tableNameWithType);
          _statementStore.deleteStatement(tableNameWithType, manifest.getStatementId());
          changedCount++;
          break;

        default:
          break;
      }
    }

    if (changedCount > 0) {
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE, getActiveStatementCount());
    }

    return changedCount;
  }

  /**
   * Registers a table as having insert statements. Called externally (e.g., from REST endpoints)
   * to ensure the cleanup sweep covers this table.
   */
  public void registerTableForCleanup(String tableNameWithType) {
    _tablesWithStatements.add(tableNameWithType);
  }

  private long getActiveStatementCount() {
    long count = 0;
    for (String table : _tablesWithStatements) {
      try {
        List<InsertStatementManifest> manifests = _statementStore.listStatements(table);
        for (InsertStatementManifest m : manifests) {
          InsertStatementState s = m.getState();
          if (s == InsertStatementState.ACCEPTED || s == InsertStatementState.PREPARED
              || s == InsertStatementState.COMMITTED) {
            count++;
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error counting active statements for table {}", table, e);
      }
    }
    return count;
  }

  private static InsertResult errorResult(String statementId, String errorCode, String message) {
    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
        .setErrorCode(errorCode).setMessage(message).build();
  }

  @VisibleForTesting
  InsertStatementStore getStatementStore() {
    return _statementStore;
  }
}
