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
package org.apache.pinot.core.data.manager.ingest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages recovery of INSERT INTO operations on server startup.
 *
 * <p>On startup, scans the prepared store for any in-progress statements and reconciles their
 * state with the coordinator. For committed statements, replays and applies the prepared data.
 * For aborted or unknown statements, cleans up the prepared data. For prepared (still in progress)
 * statements, leaves them for coordinator timeout decision.
 *
 * <p>If the coordinator is unreachable during recovery, the manager retries with exponential
 * backoff up to a configurable maximum number of attempts. Recovery runs asynchronously and does
 * not block server startup.
 *
 * <p>This class is not thread-safe and should only be called during single-threaded startup or
 * from a dedicated recovery thread.
 */
public class InsertRecoveryManager {

  /**
   * Callback interface for querying statement state from the coordinator.
   * Implementations should throw {@link CoordinatorUnreachableException} when the coordinator
   * cannot be contacted, to distinguish transient failures from a definitive "unknown" response.
   */
  public interface StatementStateResolver {
    /**
     * Resolves the current state of a statement from the coordinator.
     *
     * @param statementId the statement identifier
     * @return the current state, or {@code null} if the statement is definitively unknown
     * @throws CoordinatorUnreachableException if the coordinator cannot be reached
     */
    InsertStatementState resolve(String statementId)
        throws CoordinatorUnreachableException;
  }

  /**
   * Thrown when the coordinator is unreachable during state resolution.
   */
  public static class CoordinatorUnreachableException extends Exception {
    public CoordinatorUnreachableException(String message) {
      super(message);
    }

    public CoordinatorUnreachableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Callback interface for applying recovered rows to mutable segments.
   */
  public interface RowApplyCallback {
    /**
     * Applies the given rows for the specified statement and partition.
     *
     * @param statementId the statement identifier
     * @param partitionId the partition identifier
     * @param rows the rows to apply
     */
    void apply(String statementId, int partitionId, List<GenericRow> rows);
  }

  /**
   * Callback to notify the coordinator that a statement has been fully applied on this server.
   */
  public interface ApplyCompleteCallback {
    /**
     * Notifies that the given statement has been fully applied on this server.
     *
     * @param statementId the statement identifier
     */
    void onApplyComplete(String statementId);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertRecoveryManager.class);

  /** Default maximum number of retry attempts when the coordinator is unreachable. */
  static final int DEFAULT_MAX_RETRIES = 5;

  /** Initial backoff delay in milliseconds for coordinator retry. */
  static final long INITIAL_BACKOFF_MS = 1000;

  /** Maximum backoff delay in milliseconds. */
  static final long MAX_BACKOFF_MS = 30_000;

  private final PreparedStore _preparedStore;
  private final InsertRowApplier _rowApplier;
  private final StatementStateResolver _stateResolver;
  private final ApplyCompleteCallback _applyCompleteCallback;
  private final int _maxRetries;

  private int _recoveryReplayedCount;
  private int _recoveryCleanedCount;
  private int _recoveryFailedCount;

  /**
   * Creates a new recovery manager.
   *
   * @param preparedStore the prepared store to scan for in-progress statements
   * @param rowApplier the row applier for replaying committed data
   * @param stateResolver callback to resolve statement state from the coordinator
   * @param applyCompleteCallback callback to notify coordinator after apply, or {@code null}
   */
  public InsertRecoveryManager(PreparedStore preparedStore, InsertRowApplier rowApplier,
      StatementStateResolver stateResolver, ApplyCompleteCallback applyCompleteCallback) {
    this(preparedStore, rowApplier, stateResolver, applyCompleteCallback, DEFAULT_MAX_RETRIES);
  }

  /**
   * Creates a new recovery manager with configurable retry count.
   *
   * @param preparedStore the prepared store to scan for in-progress statements
   * @param rowApplier the row applier for replaying committed data
   * @param stateResolver callback to resolve statement state from the coordinator
   * @param applyCompleteCallback callback to notify coordinator after apply, or {@code null}
   * @param maxRetries maximum number of retry attempts for coordinator unreachability
   */
  public InsertRecoveryManager(PreparedStore preparedStore, InsertRowApplier rowApplier,
      StatementStateResolver stateResolver, ApplyCompleteCallback applyCompleteCallback, int maxRetries) {
    _preparedStore = preparedStore;
    _rowApplier = rowApplier;
    _stateResolver = stateResolver;
    _applyCompleteCallback = applyCompleteCallback;
    _maxRetries = maxRetries;
  }

  /**
   * Runs recovery for all prepared statements found in the prepared store.
   *
   * @param rowApplyCallback callback to apply recovered rows to mutable segments
   * @return the number of statements recovered (replayed)
   */
  public int recover(RowApplyCallback rowApplyCallback) {
    List<String> preparedStatements = _preparedStore.listPreparedStatements();
    if (preparedStatements.isEmpty()) {
      LOGGER.info("No prepared statements found during recovery");
      return 0;
    }

    LOGGER.info("Found {} prepared statement(s) during recovery", preparedStatements.size());

    _recoveryReplayedCount = 0;
    _recoveryCleanedCount = 0;
    _recoveryFailedCount = 0;

    for (String statementId : preparedStatements) {
      try {
        recoverStatement(statementId, rowApplyCallback);
      } catch (Exception e) {
        LOGGER.error("Failed to recover statement: {}", statementId, e);
        _recoveryFailedCount++;
      }
    }

    LOGGER.info("Recovery completed: replayed={}, cleaned={}, failed={}, total={}",
        _recoveryReplayedCount, _recoveryCleanedCount, _recoveryFailedCount, preparedStatements.size());
    return _recoveryReplayedCount;
  }

  private void recoverStatement(String statementId, RowApplyCallback rowApplyCallback) {
    InsertStatementState state = resolveWithRetry(statementId);

    if (state == null) {
      LOGGER.warn("Unknown statement {} during recovery (coordinator returned null), cleaning up", statementId);
      _preparedStore.cleanup(statementId);
      _recoveryCleanedCount++;
      return;
    }

    switch (state) {
      case COMMITTED:
        LOGGER.info("Replaying committed statement: {}", statementId);
        if (replayCommittedStatement(statementId, rowApplyCallback)) {
          _recoveryReplayedCount++;
          // Notify the coordinator that this server has finished applying
          notifyApplyComplete(statementId);
        } else {
          _recoveryFailedCount++;
        }
        break;

      case VISIBLE:
        // Already visible: replay idempotently (the applier deduplicates) and clean up
        LOGGER.info("Replaying visible statement (idempotent): {}", statementId);
        if (replayCommittedStatement(statementId, rowApplyCallback)) {
          _recoveryReplayedCount++;
          // Only clean up prepared data after successful replay. If replay failed (e.g.,
          // partition dir missing or I/O error), keep the data so a subsequent recovery
          // attempt can retry rather than permanently losing the rows.
          _preparedStore.cleanup(statementId);
        } else {
          LOGGER.warn("VISIBLE statement {} replay incomplete — keeping prepared data for next recovery",
              statementId);
          _recoveryFailedCount++;
        }
        break;

      case ABORTED:
      case GC:
        LOGGER.info("Cleaning up aborted/GC'd statement: {}", statementId);
        _preparedStore.cleanup(statementId);
        _recoveryCleanedCount++;
        break;

      case PREPARED:
      case ACCEPTED:
      case NEW:
        LOGGER.info("Statement {} is still in state {}, leaving for coordinator timeout decision",
            statementId, state);
        // Do not clean up; the coordinator will either commit or abort this statement.
        break;

      default:
        LOGGER.warn("Unexpected state {} for statement {}, cleaning up", state, statementId);
        _preparedStore.cleanup(statementId);
        _recoveryCleanedCount++;
        break;
    }
  }

  /**
   * Resolves statement state with exponential backoff retry when the coordinator is unreachable.
   * Returns {@code null} if the statement is definitively unknown or if all retries are exhausted
   * (in which case the statement will be left for coordinator decision rather than incorrectly
   * cleaned up).
   */
  private InsertStatementState resolveWithRetry(String statementId) {
    long backoffMs = INITIAL_BACKOFF_MS;
    for (int attempt = 0; attempt <= _maxRetries; attempt++) {
      try {
        return _stateResolver.resolve(statementId);
      } catch (CoordinatorUnreachableException e) {
        if (attempt < _maxRetries) {
          LOGGER.warn("Coordinator unreachable for statement {} (attempt {}/{}), retrying in {}ms",
              statementId, attempt + 1, _maxRetries, backoffMs, e);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Recovery interrupted while waiting for coordinator retry");
            return InsertStatementState.PREPARED; // Treat as "leave for coordinator"
          }
          backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        } else {
          LOGGER.error("Coordinator unreachable for statement {} after {} retries; "
              + "leaving statement for coordinator decision", statementId, _maxRetries, e);
          // Return PREPARED so the statement is left alone rather than incorrectly cleaned up.
          // The coordinator's cleanup sweep will handle it once it becomes reachable.
          return InsertStatementState.PREPARED;
        }
      }
    }
    return InsertStatementState.PREPARED;
  }

  private boolean replayCommittedStatement(String statementId, RowApplyCallback rowApplyCallback) {
    File stmtDir = getStatementDir(statementId);
    if (stmtDir == null || !stmtDir.exists()) {
      LOGGER.warn("No prepared directory found for committed statement: {}", statementId);
      return false;
    }

    File[] partitionDirs = stmtDir.listFiles(File::isDirectory);
    if (partitionDirs == null || partitionDirs.length == 0) {
      LOGGER.warn("No partition directories found for committed statement: {}", statementId);
      return false;
    }

    boolean applied = false;
    boolean allPartitionsHandled = true;
    for (File partitionDir : partitionDirs) {
      try {
        int partitionId = Integer.parseInt(partitionDir.getName());
        List<GenericRow> rows = _rowApplier.apply(statementId, partitionId, 0);
        if (rows != null && !rows.isEmpty()) {
          rowApplyCallback.apply(statementId, partitionId, rows);
          // Only mark as applied AFTER successful indexing to avoid data loss
          _rowApplier.confirmApplied(statementId, partitionId, 0);
          applied = true;
        } else if (rows == null) {
          // null means already applied in a previous recovery — treat as success
          applied = true;
        }
        // empty list means no data — still considered handled
      } catch (NumberFormatException e) {
        LOGGER.warn("Skipping non-numeric partition directory: {}", partitionDir.getName());
        allPartitionsHandled = false;
      } catch (IOException e) {
        LOGGER.error("Failed to replay partition {} for statement {}", partitionDir.getName(), statementId, e);
        allPartitionsHandled = false;
      }
    }

    if (applied && allPartitionsHandled) {
      _preparedStore.cleanup(statementId);
    }
    return applied;
  }

  private void notifyApplyComplete(String statementId) {
    if (_applyCompleteCallback != null) {
      try {
        _applyCompleteCallback.onApplyComplete(statementId);
      } catch (Exception e) {
        LOGGER.warn("Failed to notify coordinator of apply completion for statement {}", statementId, e);
      }
    }
  }

  private File getStatementDir(String statementId) {
    if (_preparedStore instanceof LocalPreparedStore) {
      return new File(((LocalPreparedStore) _preparedStore).getPreparedBaseDir(), statementId);
    }
    return null;
  }

  /**
   * Returns the count of statements replayed during the last recovery run.
   */
  public int getRecoveryReplayedCount() {
    return _recoveryReplayedCount;
  }

  /**
   * Returns the count of statements cleaned up during the last recovery run.
   */
  public int getRecoveryCleanedCount() {
    return _recoveryCleanedCount;
  }

  /**
   * Returns the count of statements that failed recovery during the last recovery run.
   */
  public int getRecoveryFailedCount() {
    return _recoveryFailedCount;
  }
}
