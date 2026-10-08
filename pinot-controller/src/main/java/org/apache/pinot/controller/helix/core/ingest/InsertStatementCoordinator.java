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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
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


/// Coordinates synchronous ROW inserts and stores their results in ZooKeeper.
/// A requestId is reserved with create-only semantics and is never rebound or released while its
/// table exists. A retry returns the original manifest, including failed or uncertain executions.
/// Manifests are retained until table deletion; recovery and bounded retention require a separate
/// protocol. This class is thread-safe; register the executor before starting the coordinator.
public class InsertStatementCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementCoordinator.class);

  private final PinotHelixResourceManager _helixResourceManager;
  private final InsertStatementStore _statementStore;
  private final ControllerMetrics _controllerMetrics;
  private final int _maxRowsPerRowInsert;
  private final long _maxBytesPerRowInsert;
  private final AtomicLong _inflightSubmitCount = new AtomicLong();
  private volatile boolean _started;
  private InsertExecutor _rowExecutor;

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics) {
    this(helixResourceManager, statementStore, controllerMetrics,
        ControllerConf.DEFAULT_INSERT_ROW_MAX_ROWS_PER_STATEMENT,
        ControllerConf.DEFAULT_INSERT_ROW_MAX_BYTES_PER_STATEMENT);
  }

  public InsertStatementCoordinator(PinotHelixResourceManager helixResourceManager,
      InsertStatementStore statementStore, ControllerMetrics controllerMetrics,
      int maxRowsPerRowInsert, long maxBytesPerRowInsert) {
    _helixResourceManager = helixResourceManager;
    _statementStore = statementStore;
    _controllerMetrics = controllerMetrics;
    _maxRowsPerRowInsert = maxRowsPerRowInsert;
    _maxBytesPerRowInsert = maxBytesPerRowInsert;
  }

  public synchronized void registerExecutor(String executorType, InsertExecutor executor) {
    if (_started) {
      throw new IllegalStateException("Register the ROW executor before starting the coordinator");
    }
    if (!InsertType.ROW.name().equals(executorType)) {
      throw new IllegalArgumentException("Only synchronous ROW inserts are supported");
    }
    _rowExecutor = Objects.requireNonNull(executor);
  }

  public synchronized void start() {
    _started = true;
  }

  public boolean isStarted() {
    return _started;
  }

  public synchronized void stop() {
    _started = false;
  }

  public InsertResult submitInsert(InsertRequest request) {
    _inflightSubmitCount.incrementAndGet();
    try {
      if (!_started) {
        return rejectResult(request.getStatementId(), InsertErrorCode.COORDINATOR_NOT_READY,
            "Push-based INSERT INTO is disabled or the controller is stopping");
      }
      return submitInsertInternal(request);
    } finally {
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.INSERT_STATEMENTS_ACTIVE,
          _inflightSubmitCount.decrementAndGet());
    }
  }

  private InsertResult submitInsertInternal(InsertRequest request) {
    String statementId = request.getStatementId();
    if (request.getInsertType() != InsertType.ROW || _rowExecutor == null) {
      return rejectResult(statementId, InsertErrorCode.NO_EXECUTOR,
          "This endpoint supports only synchronous ROW inserts");
    }
    if (request.getConsistencyMode() != InsertConsistencyMode.WAIT_FOR_ACCEPT) {
      return rejectResult(statementId, InsertErrorCode.UNSUPPORTED_CONSISTENCY_MODE,
          "Only WAIT_FOR_ACCEPT consistency mode is supported");
    }
    List<GenericRow> rows = request.getRows();
    if (rows == null || rows.isEmpty()) {
      return rejectResult(statementId, InsertErrorCode.EMPTY_ROWS, "INSERT requires at least one row");
    }
    if (rows.size() > _maxRowsPerRowInsert) {
      return rejectResult(statementId, InsertErrorCode.ROW_LIMIT_EXCEEDED,
          "INSERT exceeds the per-statement limit of " + _maxRowsPerRowInsert + " rows");
    }
    if (estimateRowPayloadBytes(rows, _maxBytesPerRowInsert) > _maxBytesPerRowInsert) {
      return rejectResult(statementId, InsertErrorCode.PAYLOAD_TOO_LARGE,
          "INSERT exceeds the per-statement payload limit of " + _maxBytesPerRowInsert + " bytes");
    }
    String tableNameWithType;
    try {
      tableNameWithType = resolveTableName(request.getTableName(), request.getTableType());
    } catch (IllegalArgumentException e) {
      return rejectResult(statementId, InsertErrorCode.TABLE_RESOLUTION_ERROR, e.getMessage());
    }
    String requestId = request.getRequestId();
    String payloadHash = requestId != null ? request.computePayloadHash() : null;
    if (requestId != null) {
      try {
        String reservedStatementId = _statementStore.reserveRequestId(tableNameWithType, requestId, statementId);
        if (reservedStatementId != null) {
          InsertStatementManifest existing = _statementStore.getStatement(tableNameWithType, reservedStatementId);
          if (existing == null) {
            // The owner may still be creating its manifest, or may have crashed before creating it.
            // Never execute another insert for this reservation, even after a controller restart.
            return rejectResult(statementId, InsertErrorCode.IDEMPOTENCY_ERROR,
                "The requestId is reserved but its statement is unavailable; retry to read the original result");
          }
          if (!Objects.equals(payloadHash, existing.getPayloadHash())) {
            return rejectResult(statementId, InsertErrorCode.IDEMPOTENCY_CONFLICT,
                "The requestId was already used with a different payload");
          }
          return toResult(existing);
        }
      } catch (RuntimeException e) {
        LOGGER.error("Cannot verify idempotency for requestId={}", requestId, e);
        return rejectResult(statementId, InsertErrorCode.IDEMPOTENCY_ERROR,
            "Cannot verify idempotency: " + e.getMessage());
      }
    }

    long now = System.currentTimeMillis();
    InsertStatementManifest manifest = new InsertStatementManifest(statementId, requestId, payloadHash,
        tableNameWithType, InsertType.ROW, InsertStatementState.ACCEPTED, now, now, List.of(), null, null);
    if (!_statementStore.createStatement(manifest)) {
      return rejectResult(statementId, InsertErrorCode.STORE_ERROR,
          "Failed to persist the statement; requestId remains reserved to prevent duplicate execution");
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.INSERT_STATEMENTS_SUBMITTED, 1);

    InsertResult result;
    try {
      result = _rowExecutor.execute(request.withResolvedTable(tableNameWithType));
    } catch (Exception e) {
      LOGGER.error("ROW insert failed for statementId={}", statementId, e);
      result = new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
          .setErrorCode(InsertErrorCode.EXECUTOR_ERROR).setMessage("Executor failed: " + e.getMessage()).build();
    }
    if (result.getState() != InsertStatementState.VISIBLE && result.getState() != InsertStatementState.ABORTED) {
      result = new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.ABORTED)
          .setErrorCode(InsertErrorCode.EXECUTOR_ERROR)
          .setMessage("A synchronous ROW executor must return VISIBLE or ABORTED").build();
    }
    manifest.setState(result.getState());
    manifest.setErrorMessage(result.getMessage());
    manifest.setErrorCode(result.getErrorCode());
    if (result.getSegmentNames() != null) {
      manifest.setSegmentNames(result.getSegmentNames());
    }
    // This submission is the only manifest writer. Retry a failed store update without re-running
    // the executor. On permanent failure ACCEPTED remains an uncertain result that requires inspection.
    boolean persisted = false;
    for (int attempt = 0; attempt < 3 && !persisted; attempt++) {
      persisted = _statementStore.updateStatement(manifest);
    }
    if (!persisted) {
      return new InsertResult.Builder().setStatementId(statementId).setState(result.getState())
          .setSegmentNames(result.getSegmentNames()).setErrorCode(InsertErrorCode.STATE_PERSIST_ERROR)
          .setMessage("Execution finished but its result could not be persisted; requestId remains reserved")
          .build();
    }
    _controllerMetrics.addMeteredGlobalValue(result.getState() == InsertStatementState.VISIBLE
        ? ControllerMeter.INSERT_STATEMENTS_VISIBLE : ControllerMeter.INSERT_STATEMENTS_ABORTED, 1);
    return result;
  }

  public InsertResult getStatus(String statementId, String tableNameWithType) {
    InsertStatementManifest manifest = _statementStore.getStatement(tableNameWithType, statementId);
    return manifest != null ? toResult(manifest)
        : rejectResult(statementId, InsertErrorCode.NOT_FOUND, "Statement not found: " + statementId);
  }

  public List<InsertResult> listStatements(String tableNameWithType) {
    List<InsertResult> results = new ArrayList<>();
    for (InsertStatementManifest manifest : _statementStore.listStatements(tableNameWithType)) {
      results.add(toResult(manifest));
    }
    return results;
  }

  private static InsertResult toResult(InsertStatementManifest manifest) {
    return new InsertResult.Builder().setStatementId(manifest.getStatementId()).setState(manifest.getState())
        .setSegmentNames(manifest.getSegmentNames()).setErrorCode(manifest.getErrorCode())
        .setMessage(manifest.getErrorMessage()).build();
  }

  private static InsertResult rejectResult(String statementId, String errorCode, String message) {
    return new InsertResult.Builder().setStatementId(statementId).setState(InsertStatementState.REJECTED)
        .setErrorCode(errorCode).setMessage(message).build();
  }

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

  private static long estimateRowPayloadBytes(List<GenericRow> rows, long limit) {
    long total = 0;
    for (GenericRow row : rows) {
      total += estimateValuePayloadBytes(row.getFieldToValueMap(), limit - total);
      for (String nullField : row.getNullValueFields()) {
        total += 16 + 2L * nullField.length();
        if (total > limit) {
          break;
        }
      }
      if (total > limit) {
        break;
      }
    }
    return total;
  }

  private static long estimateValuePayloadBytes(@Nullable Object value, long limit) {
    if (value instanceof CharSequence || value instanceof Character) {
      return 2L * value.toString().length();
    }
    if (value instanceof byte[]) {
      return ((byte[]) value).length;
    }
    if (value instanceof Number) {
      return Math.max(16, 2L * value.toString().length());
    }
    long total = 16;
    if (value instanceof Map) {
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        total += estimateValuePayloadBytes(entry.getKey(), limit - total);
        total += estimateValuePayloadBytes(entry.getValue(), limit - total);
        if (total > limit) {
          break;
        }
      }
    } else if (value instanceof List) {
      for (Object element : (List<?>) value) {
        total += estimateValuePayloadBytes(element, limit - total);
        if (total > limit) {
          break;
        }
      }
    } else if (value != null && value.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(value); i++) {
        total += estimateValuePayloadBytes(Array.get(value, i), limit - total);
        if (total > limit) {
          break;
        }
      }
    }
    return total;
  }
}
