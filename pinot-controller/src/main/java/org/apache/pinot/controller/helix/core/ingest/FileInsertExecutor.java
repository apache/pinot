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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingest.InsertErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Executes INSERT INTO ... FROM FILE statements by scheduling Minion
/// `SegmentGenerationAndPushTask` jobs to generate and push segments.
///
/// This executor validates table mode safety rules, schedules the Minion task, and surrenders
/// lifecycle tracking to the coordinator's ZooKeeper-backed {@link InsertStatementStore}. Segments
/// pushed by the Minion task are registered in ZK/IdealState by the normal push mechanism;
/// {@link #completeFileInsert} then transitions the manifest to VISIBLE.
///
/// This executor holds only a small task-name cache ({@link #_taskNames}) and falls back to the
/// ZK-persisted `minionTaskName` for failover recovery. No other lifecycle state is kept
/// in-memory.
///
/// Instances are thread-safe.
public class FileInsertExecutor implements InsertExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileInsertExecutor.class);

  static final String TASK_TYPE = MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE;
  static final String INPUT_DIR_URI = "inputDirURI";
  static final String STATEMENT_ID_KEY = "insert.statementId";

  private final PinotHelixResourceManager _resourceManager;
  private final PinotTaskManager _taskManager;
  @Nullable
  private final PinotHelixTaskResourceManager _helixTaskResourceManager;

  /// Maps statementId to the Pinot task name returned by {@link PinotTaskManager#createTask}.
  /// Used by {@link #resolveAcceptedStatementIfTaskDone} to poll task completion. This is a
  /// best-effort cache only — on controller failover the map is empty, and callers fall back to
  /// the ZK-persisted `minionTaskName` in the manifest.
  private final ConcurrentHashMap<String, String> _taskNames = new ConcurrentHashMap<>();

  /// Creates a new executor with the given controller dependencies. Task completion polling is
  /// disabled when this constructor is used (no {@link PinotHelixTaskResourceManager} available).
  ///
  /// @param resourceManager the Helix resource manager for table config and segment operations
  /// @param taskManager     the task manager for scheduling Minion tasks
  public FileInsertExecutor(PinotHelixResourceManager resourceManager, PinotTaskManager taskManager) {
    this(resourceManager, taskManager, null);
  }

  /// Creates a new executor with full dependencies. Passing `helixTaskResourceManager`
  /// enables automatic task-completion polling in
  /// {@link #resolveAcceptedStatementIfTaskDone(String, InsertStatementManifest)}.
  ///
  /// @param resourceManager          the Helix resource manager
  /// @param taskManager              the task manager for scheduling Minion tasks
  /// @param helixTaskResourceManager the Helix task resource manager for polling task state;
  ///                                 may be `null` to disable polling
  public FileInsertExecutor(PinotHelixResourceManager resourceManager, PinotTaskManager taskManager,
      @Nullable PinotHelixTaskResourceManager helixTaskResourceManager) {
    _resourceManager = resourceManager;
    _taskManager = taskManager;
    _helixTaskResourceManager = helixTaskResourceManager;
  }

  @Override
  public InsertResult execute(InsertRequest request) {
    String statementId = request.getStatementId();
    String fileUri = request.getFileUri();
    String tableName = request.getTableName();
    TableType tableType = request.getTableType();

    LOGGER.info("Executing file insert statement {} for table {} from URI {}", statementId, tableName, fileUri);

    /// 1. Resolve the fully qualified table name
    String tableNameWithType = resolveTableNameWithType(tableName, tableType);

    /// 2. Look up the table config
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Table not found: " + tableNameWithType, InsertErrorCode.TABLE_NOT_FOUND);
    }

    /// 3. Validate table mode safety rules
    String safetyError = validateTableModeSafety(tableConfig);
    if (safetyError != null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED, safetyError,
          InsertErrorCode.TABLE_MODE_REJECTED);
    }

    /// 4. Validate file URI
    String uriError = validateFileUri(fileUri);
    if (uriError != null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED, uriError, InsertErrorCode.INVALID_FILE_URI);
    }

    /// 5. Schedule Minion SegmentGenerationAndPushTask.
    /// The coordinator has already created the manifest in ZK at state ACCEPTED; we just
    /// schedule the task and stash its name for later polling. The task generates segments,
    /// pushes them, and registers them in ZK/IdealState via the normal push mechanism.
    /// completeFileInsert() is called after the task completes to transition the manifest to
    /// VISIBLE.
    try {
      Map<String, String> taskConfigs = buildTaskConfigs(fileUri, statementId, request.getOptions());
      Map<String, String> taskResult = _taskManager.createTask(
          TASK_TYPE,
          tableNameWithType,
          statementId,
          taskConfigs
      );
      /// Fail-fast if no task was actually created (unschedulable or zero subtasks).
      /// Leaving the manifest in ACCEPTED with no task to poll would result in a timeout-abort.
      String taskName = taskResult.get(tableNameWithType);
      if (taskName == null) {
        LOGGER.error("PinotTaskManager returned no task name for statement {} table {} (result={}). "
            + "Task may be unschedulable or generated zero subtasks.", statementId, tableNameWithType, taskResult);
        return buildErrorResult(statementId, InsertStatementState.ABORTED,
            "Minion task was not created (unschedulable or no subtasks generated)",
            InsertErrorCode.TASK_SCHEDULE_FAILED);
      }
      _taskNames.put(statementId, taskName);
      LOGGER.info("Scheduled Minion task for statement {}: {}", statementId, taskResult);
    } catch (Exception e) {
      LOGGER.error("Failed to schedule Minion task for statement {}", statementId, e);
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Failed to schedule Minion task: " + e.getMessage(), InsertErrorCode.TASK_SCHEDULE_FAILED);
    }

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ACCEPTED)
        .setMessage("File insert task scheduled successfully")
        .build();
  }

  /// Validates that a file insert is in a state where the coordinator can transition it to VISIBLE,
  /// and returns a signal {@link InsertResult} encoding the validation outcome.
  ///
  /// **This method is a pure validation function.** It does NOT mutate the manifest,
  /// does NOT persist anything, and does NOT register segments — those side effects are owned by
  /// the Minion `SegmentGenerationAndPushTask` (which already pushed the segments by the time
  /// this is called) and by the coordinator's `persistWithCasRetry` (which performs the
  /// durable ACCEPTED→VISIBLE transition based on the result returned here).
  ///
  /// The returned result encodes one of three signals to the coordinator:
  /// - `state=VISIBLE` → the coordinator should run the CAS to flip the manifest.
  /// - `state=ABORTED, errorCode=STATEMENT_NOT_FOUND` → manifest was missing.
  /// - `state=ABORTED, errorCode=STATEMENT_ABORTED` → manifest was already aborted.
  ///
  /// @param statementId  the statement to complete (used for logging only)
  /// @param segmentNames the segment names pushed by the Minion task
  /// @param zkManifest   the ZK-loaded manifest; must be non-null (caller reads from
  ///                     {@link InsertStatementStore} first)
  /// @return a non-null signal result; never throws
  public InsertResult prepareCompletionResult(String statementId, List<String> segmentNames,
      @Nullable InsertStatementManifest zkManifest) {
    if (zkManifest == null) {
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Unknown statement: " + statementId, InsertErrorCode.STATEMENT_NOT_FOUND);
    }

    InsertStatementState state = zkManifest.getState();
    /// Tightened precondition: only ACCEPTED can transition to VISIBLE in v1. The FILE-insert path
    /// never enters COMMITTED (no PREPARED→COMMITTED step exists in the v1 file flow). Allowing
    /// COMMITTED here would silently accept a future double-commit when v2 introduces a real
    /// PREPARED→COMMITTED→VISIBLE flow. ABORTED is called out separately for the log-friendly code.
    if (state == InsertStatementState.ABORTED) {
      LOGGER.warn("Ignoring completeFileInsert for already-ABORTED statement {}", statementId);
      return buildErrorResult(statementId, InsertStatementState.ABORTED,
          "Statement already aborted; completion ignored.", InsertErrorCode.STATEMENT_ABORTED);
    }
    if (state != InsertStatementState.ACCEPTED) {
      LOGGER.warn("Ignoring completeFileInsert for statement {} in unexpected state {}", statementId, state);
      return buildErrorResult(statementId, state,
          "Statement is in state " + state + "; cannot transition to VISIBLE.",
          InsertErrorCode.INVALID_STATE_FOR_COMPLETION);
    }

    LOGGER.info("Signalling file insert statement {} for table {} as ready to transition to VISIBLE "
        + "with segments {}", statementId, zkManifest.getTableNameWithType(), segmentNames);
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.VISIBLE)
        .setMessage("Segments are now visible")
        .setSegmentNames(segmentNames)
        .build();
  }

  /// Drops the cached Minion task name. Called by the coordinator only after a successful CAS
  /// transition to VISIBLE so that the in-memory cache stays consistent with the persisted state.
  void taskCompleted(String statementId) {
    _taskNames.remove(statementId);
  }

  @Override
  public void abort(String statementId) {
    /// Best-effort cleanup hook called by InsertStatementCoordinator.delegateAbortToExecutor
    /// AFTER the coordinator has set ABORTED in ZK. For the FILE path the executor owns no
    /// lifecycle state — we just drop the cached task name. Segments pushed by the Minion task
    /// before abort remain registered (v1 limitation, operator cleanup required).
    LOGGER.info("Abort hook for file insert statement {}", statementId);
    _taskNames.remove(statementId);
  }

  /// Validates table mode safety rules for file insert.
  ///
  /// - Append tables (OFFLINE or REALTIME without upsert/dedup): allowed
  /// - REALTIME full-upsert: allowed only if partition config is present
  /// - REALTIME partial-upsert: rejected
  /// - Dedup tables: rejected
  ///
  /// @param tableConfig the table configuration to validate
  /// @return error message if rejected, or `null` if allowed
  @Nullable
  String validateTableModeSafety(TableConfig tableConfig) {
    return InsertTableModeValidator.validate(tableConfig, "file");
  }

  /// Validates the file URI is syntactically valid.
  ///
  /// @param fileUri the URI to validate
  /// @return error message if invalid, or `null` if valid
  @Nullable
  String validateFileUri(String fileUri) {
    if (fileUri == null || fileUri.isEmpty()) {
      return "File URI must not be empty.";
    }
    URI parsed;
    try {
      parsed = URI.create(fileUri);
    } catch (IllegalArgumentException e) {
      return "Invalid file URI: " + fileUri + " (" + e.getMessage() + ")";
    }
    /// Allowlist of remote-storage schemes. Anything else — including unknown schemes, file://,
    /// jar://, and schemeless paths — is rejected so a controller running with elevated
    /// permissions cannot be coerced into reading arbitrary local files (file:///etc/passwd) via
    /// the Minion task. An allowlist also protects against future local-fs aliases (e.g. local://,
    /// resource://) bypassing a denylist.
    String scheme = parsed.getScheme();
    if (scheme == null || scheme.isEmpty()) {
      return "File URI must include a remote-storage scheme (e.g. s3://, gs://, abfs://): " + fileUri;
    }
    String lowerScheme = scheme.toLowerCase(Locale.ROOT);
    if (!ALLOWED_FILE_URI_SCHEMES.contains(lowerScheme)) {
      return "URI scheme '" + scheme + "' is not in the allowlist for INSERT INTO FROM FILE. "
          + "Allowed schemes: " + ALLOWED_FILE_URI_SCHEMES + ". URI: " + fileUri;
    }
    return null;
  }

  /// Allowlist of remote-storage URI schemes for INSERT INTO FROM FILE. Local-filesystem schemes
  /// (file://, jar://, local://, schemeless) are deliberately excluded to prevent privilege
  /// escalation by an authenticated EXECUTE_INSERT principal. Operators that genuinely need
  /// local-file ingestion (dev / quickstart) should add the scheme to this list explicitly via a
  /// future config knob.
  private static final Set<String> ALLOWED_FILE_URI_SCHEMES = Set.of(
      "s3", "s3a", "gs", "abfs", "abfss", "adl", "hdfs", "oss", "wasb", "wasbs", "viewfs", "swebhdfs", "webhdfs");

  /// Resolves the table name with type suffix. If the table name already has a type suffix,
  /// returns it as-is. Otherwise appends the type based on the request's table type.
  private String resolveTableNameWithType(String tableName, TableType tableType) {
    if (TableNameBuilder.isTableResource(tableName)) {
      return tableName;
    }
    if (tableType != null) {
      return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    }
    /// Default to OFFLINE for file insert
    return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
  }

  /// Fetches the table config from ZooKeeper via the resource manager.
  @Nullable
  private TableConfig getTableConfig(String tableNameWithType) {
    return _resourceManager.getTableConfig(tableNameWithType);
  }

  /// Builds the task configuration map for the Minion SegmentGenerationAndPushTask.
  private Map<String, String> buildTaskConfigs(String fileUri, String statementId,
      Map<String, String> requestOptions) {
    Map<String, String> taskConfigs = new HashMap<>();
    if (requestOptions != null) {
      taskConfigs.putAll(requestOptions);
    }
    taskConfigs.put(INPUT_DIR_URI, fileUri);
    taskConfigs.put(STATEMENT_ID_KEY, statementId);
    return taskConfigs;
  }

  /// Builds an error InsertResult with the given state and message.
  private InsertResult buildErrorResult(String statementId, InsertStatementState state, String message,
      String errorCode) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(state)
        .setMessage(message)
        .setErrorCode(errorCode)
        .build();
  }

  /// Checks whether the Minion task for an ACCEPTED file insert has finished, and if so
  /// auto-completes or aborts the insert to prevent the cleanup sweep from incorrectly timing
  /// out a successful task.
  ///
  /// This method is called by the coordinator's cleanup sweep for FILE inserts stuck in
  /// ACCEPTED state. Without this check, a successful Minion task has no other mechanism to
  /// transition the statement from ACCEPTED to VISIBLE.
  ///
  /// @param statementId the statement to check
  /// @param zkManifest  the ZK manifest for failover recovery when in-memory state is absent
  /// @return {@link InsertStatementState#VISIBLE} if the task completed and the insert was
  ///     transitioned; {@link InsertStatementState#ABORTED} if the task failed; `null` if
  ///     the task is still running or task-state polling is not available
  @Nullable
  public InsertStatementState resolveAcceptedStatementIfTaskDone(String statementId,
      @Nullable InsertStatementManifest zkManifest) {
    if (_helixTaskResourceManager == null) {
      return null;
    }
    String taskName = _taskNames.get(statementId);
    if (taskName == null && zkManifest != null) {
      /// Fall back to the ZK-persisted task name so a new controller leader can still poll
      /// after failover (the in-memory _taskNames map is empty after restart).
      taskName = zkManifest.getMinionTaskName();
    }
    if (taskName == null) {
      /// Task name unavailable — cannot poll; leave the cleanup sweep to apply the timeout.
      return null;
    }
    TaskState taskState;
    try {
      taskState = _helixTaskResourceManager.getTaskState(taskName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get task state for statementId={} taskName={}", statementId, taskName, e);
      return null;
    }
    if (taskState == null) {
      return null;
    }
    LOGGER.debug("Minion task {} for statement {} is in state {}", taskName, statementId, taskState);
    if (taskState == TaskState.COMPLETED) {
      /// Segments are already pushed and registered by the task. Transition to VISIBLE.
      ///
      /// KNOWN v1 LIMITATION: we pass an empty segment-names list here because the Minion task
      /// doesn't surface its produced segment names back to the coordinator. The manifest's
      /// segmentNames will be empty for sweep-auto-completed FILE inserts. Operators querying
      /// /insert/list or /insert/status see no segment names — they must consult the table's
      /// IdealState to discover the actually-produced segments. The /insert/complete REST path
      /// (used when the task observer can call back) preserves the names. v2 should plumb the
      /// task's segment list through Helix subtask config so this gap closes.
      InsertResult result = prepareCompletionResult(statementId, Collections.emptyList(), zkManifest);
      if (result.getState() == InsertStatementState.VISIBLE) {
        LOGGER.info("Auto-completed file insert {} after Minion task {} finished (segmentNames not "
            + "surfaced; check IdealState for actual segments)", statementId, taskName);
        return InsertStatementState.VISIBLE;
      }
      return null;
    }
    if (taskState == TaskState.FAILED || taskState == TaskState.ABORTED || taskState == TaskState.TIMED_OUT
        || taskState == TaskState.STOPPED) {
      LOGGER.warn("Minion task {} for statement {} ended in state {}", taskName, statementId, taskState);
      /// Drop cached entry — task is in a terminal state, won't be polled again.
      _taskNames.remove(statementId);
      return InsertStatementState.ABORTED;
    }
    /// NOT_STARTED or IN_PROGRESS — still running
    return null;
  }

  /// Returns the Helix task name stored for a given statement, or `null` if not tracked
  /// (e.g., after controller failover before the ZK-backed manifest is consulted).
  /// Visible for testing.
  @Nullable
  String getScheduledTaskName(String statementId) {
    return _taskNames.get(statementId);
  }
}
