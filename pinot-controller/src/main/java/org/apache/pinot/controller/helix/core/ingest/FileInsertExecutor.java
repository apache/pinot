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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes INSERT INTO ... FROM FILE statements by scheduling Minion
 * {@code SegmentGenerationAndPushTask} jobs and coordinating segment visibility via the segment
 * replacement protocol.
 *
 * <p>This executor validates table mode safety rules, creates a segment lineage entry through
 * {@code startReplaceSegments}, schedules the Minion task with the statement and lineage context,
 * and tracks statement lifecycle through {@link InsertStatementManifest}.
 *
 * <p>Instances are thread-safe; internal state is managed via a {@link ConcurrentHashMap}.
 */
public class FileInsertExecutor implements InsertExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileInsertExecutor.class);

  static final String TASK_TYPE = "SegmentGenerationAndPushTask";
  static final String INPUT_DIR_URI = "inputDirURI";
  static final String STATEMENT_ID_KEY = "insert.statementId";
  static final String LINEAGE_ENTRY_ID_KEY = "insert.lineageEntryId";
  static final String SKIP_PUSH_KEY = "insert.skipPush";

  private final PinotHelixResourceManager _resourceManager;
  private final PinotTaskManager _taskManager;

  /**
   * In-memory map of statement manifests keyed by statementId. In a production deployment this
   * would be persisted to ZooKeeper; the in-memory map serves as a fast lookup cache.
   */
  private final ConcurrentHashMap<String, InsertStatementManifest> _manifests = new ConcurrentHashMap<>();

  /**
   * Maps statementId to the lineage entry id returned by startReplaceSegments so that we can
   * call endReplaceSegments or revertReplaceSegments later.
   */
  private final ConcurrentHashMap<String, String> _lineageEntries = new ConcurrentHashMap<>();

  /**
   * Creates a new executor with the given controller dependencies.
   *
   * @param resourceManager the Helix resource manager for table config and segment operations
   * @param taskManager     the task manager for scheduling Minion tasks
   */
  public FileInsertExecutor(PinotHelixResourceManager resourceManager, PinotTaskManager taskManager) {
    _resourceManager = resourceManager;
    _taskManager = taskManager;
  }

  @Override
  public InsertResult execute(InsertRequest request) {
    String statementId = request.getStatementId();
    String fileUri = request.getFileUri();
    String tableName = request.getTableName();
    TableType tableType = request.getTableType();

    LOGGER.info("Executing file insert statement {} for table {} from URI {}", statementId, tableName, fileUri);

    // 1. Resolve the fully qualified table name
    String tableNameWithType = resolveTableNameWithType(tableName, tableType);

    // 2. Look up the table config
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Table not found: " + tableNameWithType, "TABLE_NOT_FOUND");
    }

    // 3. Validate table mode safety rules
    String safetyError = validateTableModeSafety(tableConfig);
    if (safetyError != null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED, safetyError, "TABLE_MODE_REJECTED");
    }

    // 4. Validate file URI
    String uriError = validateFileUri(fileUri);
    if (uriError != null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED, uriError, "INVALID_FILE_URI");
    }

    // 5. Create the statement manifest
    InsertStatementManifest manifest = new InsertStatementManifest(
        statementId,
        request.getRequestId(),
        request.getPayloadHash(),
        tableNameWithType,
        InsertType.FILE,
        InsertStatementState.ACCEPTED,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        null,
        null,
        null
    );
    _manifests.put(statementId, manifest);

    // 6. Schedule Minion SegmentGenerationAndPushTask.
    // Note: segment replacement protocol (startReplaceSegments/endReplaceSegments) is deferred
    // to completeFileInsert() when the final segment names are known. Starting replacement with
    // an empty segmentsTo list would cause endReplaceSegments to reject the actual segments.
    try {
      Map<String, String> taskConfigs = buildTaskConfigs(fileUri, statementId, null, request.getOptions());
      Map<String, String> taskResult = _taskManager.createTask(
          TASK_TYPE,
          tableNameWithType,
          statementId,
          taskConfigs
      );
      LOGGER.info("Scheduled Minion task for statement {}: {}", statementId, taskResult);
    } catch (Exception e) {
      LOGGER.error("Failed to schedule Minion task for statement {}", statementId, e);
      manifest.setState(InsertStatementState.ABORTED);
      manifest.setErrorMessage("Failed to schedule Minion task: " + e.getMessage());

      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Failed to schedule Minion task: " + e.getMessage(), "TASK_SCHEDULE_FAILED");
    }

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ACCEPTED)
        .setMessage("File insert task scheduled successfully")
        .build();
  }

  /**
   * Completes a file insert statement after its Minion task has finished segment generation and
   * push. This should be called by the controller's task-completion observer or periodic poller.
   *
   * <p>Calls {@code endReplaceSegments} to finalize the lineage entry, making the new segments
   * visible to queries, and transitions the manifest to VISIBLE.
   *
   * @param statementId   the statement to complete
   * @param segmentNames  the segment names produced by the Minion task
   * @return the result reflecting the new state
   */
  /**
   * Completes a file insert using in-memory state only. Package-private because callers
   * should use the three-arg overload with a ZK manifest for failover safety.
   */
  InsertResult completeFileInsert(String statementId, List<String> segmentNames) {
    return completeFileInsert(statementId, segmentNames, null);
  }

  /**
   * Completes a file insert with ZK manifest fallback for failover recovery.
   * This is the primary entry point — always pass the ZK manifest when available.
   */
  public InsertResult completeFileInsert(String statementId, List<String> segmentNames,
      @Nullable InsertStatementManifest zkManifest) {
    InsertStatementManifest manifest = _manifests.get(statementId);
    if (manifest == null) {
      manifest = zkManifest;
    }
    if (manifest == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Unknown statement: " + statementId, "STATEMENT_NOT_FOUND");
    }

    String tableNameWithType = manifest.getTableNameWithType();
    LOGGER.info("Completing file insert statement {} for table {} with segments {}",
        statementId, tableNameWithType, segmentNames);

    // Start the segment replacement protocol now that the exact segment names are known.
    // This avoids the problem of starting with an empty segmentsTo list, which would cause
    // endReplaceSegments to reject the actual segments during validation.
    String lineageEntryId;
    try {
      lineageEntryId = _resourceManager.startReplaceSegments(
          tableNameWithType,
          Collections.emptyList(),  // segmentsFrom: empty for append
          segmentNames,             // segmentsTo: the exact segments produced by the Minion task
          false,
          buildCustomMap(statementId)
      );
      _lineageEntries.put(statementId, lineageEntryId);
      manifest.setLineageEntryId(lineageEntryId);
      LOGGER.info("Started segment replacement for statement {} with lineage entry {} and segments {}",
          statementId, lineageEntryId, segmentNames);
    } catch (Exception e) {
      LOGGER.error("Failed to start segment replacement for statement {}", statementId, e);
      manifest.setState(InsertStatementState.ABORTED);
      manifest.setErrorMessage("Failed to start segment replacement: " + e.getMessage());
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Failed to start segment replacement: " + e.getMessage(), "SEGMENT_REPLACE_FAILED");
    }

    // Finalize the replacement to make segments visible
    try {
      EndReplaceSegmentsRequest endRequest = new EndReplaceSegmentsRequest(segmentNames);
      _resourceManager.endReplaceSegments(tableNameWithType, lineageEntryId, endRequest);
    } catch (Exception e) {
      LOGGER.error("Failed to end segment replacement for statement {}", statementId, e);
      manifest.setState(InsertStatementState.ABORTED);
      manifest.setErrorMessage("Failed to finalize segment replacement: " + e.getMessage());

      // Best-effort revert of the lineage entry we just created
      revertLineageEntry(tableNameWithType, lineageEntryId, statementId);

      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Failed to finalize segment replacement: " + e.getMessage(), "SEGMENT_REPLACE_FAILED");
    }

    manifest.setState(InsertStatementState.VISIBLE);
    manifest.setSegmentNames(segmentNames);
    _lineageEntries.remove(statementId);

    LOGGER.info("File insert statement {} is now VISIBLE with segments {}", statementId, segmentNames);

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.VISIBLE)
        .setMessage("Segments are now visible")
        .setSegmentNames(segmentNames)
        .build();
  }

  @Override
  public InsertResult getStatus(String statementId) {
    InsertStatementManifest manifest = _manifests.get(statementId);
    if (manifest == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Unknown statement: " + statementId, "STATEMENT_NOT_FOUND");
    }

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(manifest.getState())
        .setMessage(manifest.getErrorMessage())
        .setSegmentNames(manifest.getSegmentNames())
        .build();
  }

  @Override
  public InsertResult abort(String statementId) {
    InsertStatementManifest manifest = _manifests.get(statementId);
    if (manifest == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Unknown statement: " + statementId, "STATEMENT_NOT_FOUND");
    }

    LOGGER.info("Aborting file insert statement {}", statementId);
    manifest.setState(InsertStatementState.ABORTED);
    manifest.setErrorMessage("Aborted by user");

    // Revert segment replacement if lineage entry exists (try in-memory, then ZK manifest)
    String lineageEntryId = _lineageEntries.remove(statementId);
    if (lineageEntryId == null) {
      lineageEntryId = manifest.getLineageEntryId();
    }
    if (lineageEntryId != null) {
      revertLineageEntry(manifest.getTableNameWithType(), lineageEntryId, statementId);
    }

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage("Statement aborted successfully")
        .build();
  }

  /**
   * Validates table mode safety rules for file insert.
   *
   * <ul>
   *   <li>Append tables (OFFLINE or REALTIME without upsert/dedup): allowed</li>
   *   <li>REALTIME full-upsert: allowed only if partition config is present</li>
   *   <li>REALTIME partial-upsert: rejected</li>
   *   <li>Dedup tables: rejected</li>
   * </ul>
   *
   * @param tableConfig the table configuration to validate
   * @return error message if rejected, or {@code null} if allowed
   */
  @Nullable
  String validateTableModeSafety(TableConfig tableConfig) {
    // Check dedup
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    if (dedupConfig != null && dedupConfig.isDedupEnabled()) {
      return "Dedup tables do not support direct file insert in this version.";
    }

    // Check upsert
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE) {
      if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
        return "Partial upsert tables do not support direct file insert. "
            + "Use stream ingestion for partial upsert.";
      }

      // Full upsert: require partition config
      if (upsertConfig.getMode() == UpsertConfig.Mode.FULL) {
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        if (indexingConfig == null) {
          return "Full upsert table requires explicit partition configuration for file insert, "
              + "but no indexing config found.";
        }
        SegmentPartitionConfig partitionConfig = indexingConfig.getSegmentPartitionConfig();
        if (partitionConfig == null || partitionConfig.getColumnPartitionMap() == null
            || partitionConfig.getColumnPartitionMap().isEmpty()) {
          return "Full upsert table requires explicit partition configuration for file insert. "
              + "Configure segmentPartitionConfig in the table's indexing config.";
        }
      }
    }

    return null;
  }

  /**
   * Validates the file URI is syntactically valid.
   *
   * @param fileUri the URI to validate
   * @return error message if invalid, or {@code null} if valid
   */
  @Nullable
  String validateFileUri(String fileUri) {
    if (fileUri == null || fileUri.isEmpty()) {
      return "File URI must not be empty.";
    }
    try {
      URI.create(fileUri);
    } catch (IllegalArgumentException e) {
      return "Invalid file URI: " + fileUri + " (" + e.getMessage() + ")";
    }
    return null;
  }

  /**
   * Resolves the table name with type suffix. If the table name already has a type suffix,
   * returns it as-is. Otherwise appends the type based on the request's table type.
   */
  private String resolveTableNameWithType(String tableName, TableType tableType) {
    if (TableNameBuilder.isTableResource(tableName)) {
      return tableName;
    }
    if (tableType != null) {
      return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    }
    // Default to OFFLINE for file insert
    return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
  }

  /**
   * Fetches the table config from ZooKeeper via the resource manager.
   */
  @Nullable
  private TableConfig getTableConfig(String tableNameWithType) {
    return _resourceManager.getTableConfig(tableNameWithType);
  }

  /**
   * Builds the custom map for startReplaceSegments with statement tracking info.
   */
  private Map<String, String> buildCustomMap(String statementId) {
    Map<String, String> customMap = new HashMap<>();
    customMap.put(STATEMENT_ID_KEY, statementId);
    return customMap;
  }

  /**
   * Builds the task configuration map for the Minion SegmentGenerationAndPushTask.
   */
  private Map<String, String> buildTaskConfigs(String fileUri, String statementId,
      @Nullable String lineageEntryId, Map<String, String> requestOptions) {
    Map<String, String> taskConfigs = new HashMap<>();
    if (requestOptions != null) {
      taskConfigs.putAll(requestOptions);
    }
    taskConfigs.put(INPUT_DIR_URI, fileUri);
    taskConfigs.put(STATEMENT_ID_KEY, statementId);
    if (lineageEntryId != null) {
      taskConfigs.put(LINEAGE_ENTRY_ID_KEY, lineageEntryId);
    }
    // Tell the Minion task to skip the push step. The INSERT INTO flow manages segment
    // visibility via the replacement protocol (startReplaceSegments/endReplaceSegments)
    // in completeFileInsert(), so the task must not register segments directly.
    taskConfigs.put(SKIP_PUSH_KEY, "true");
    return taskConfigs;
  }

  /**
   * Attempts to revert a segment lineage entry, logging errors but not throwing.
   */
  private void revertLineageEntry(String tableNameWithType, String lineageEntryId, String statementId) {
    try {
      _resourceManager.revertReplaceSegments(tableNameWithType, lineageEntryId, false, null);
      LOGGER.info("Reverted segment lineage entry {} for statement {}", lineageEntryId, statementId);
    } catch (Exception e) {
      LOGGER.error("Failed to revert segment lineage entry {} for statement {}", lineageEntryId, statementId, e);
    }
  }

  /**
   * Builds an error InsertResult with the given state and message.
   */
  private InsertResult buildErrorResult(String statementId, InsertStatementState state, String message,
      String errorCode) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(state)
        .setMessage(message)
        .setErrorCode(errorCode)
        .build();
  }

  /**
   * Returns the manifest for a statement, visible for testing.
   */
  InsertStatementManifest getManifest(String statementId) {
    return _manifests.get(statementId);
  }

  /**
   * Returns the lineage entry ID for a statement, visible for testing.
   */
  String getLineageEntryId(String statementId) {
    return _lineageEntries.get(statementId);
  }
}
