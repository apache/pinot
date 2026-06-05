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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Tests for {@link FileInsertExecutor} covering table mode safety validation, file URI validation,
/// and the execute/abort lifecycle.
public class FileInsertExecutorTest {

  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String REALTIME_TABLE = "testTable_REALTIME";
  private static final String FILE_URI = "s3://my-bucket/data/file.json";

  private PinotHelixResourceManager _resourceManager;
  private PinotTaskManager _taskManager;
  private FileInsertExecutor _executor;

  @BeforeMethod
  public void setUp() {
    _resourceManager = mock(PinotHelixResourceManager.class);
    _taskManager = mock(PinotTaskManager.class);
    _executor = new FileInsertExecutor(_resourceManager, _taskManager);
  }

  @Test
  public void testExecuteAppendOfflineTable()
      throws Exception {
    /// Set up an OFFLINE append table (no upsert, no dedup)
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
    verify(_taskManager).createTask(eq("SegmentGenerationAndPushTask"), eq(TABLE_NAME),
        anyString(), anyMap());
  }

  @Test
  public void testExecuteAppendRealtimeTable()
      throws Exception {
    /// Set up a REALTIME append table (no upsert, no dedup)
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .build();
    mockTableConfig(REALTIME_TABLE, tableConfig);
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(REALTIME_TABLE, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testRejectPartialUpsertTable() {
    /// Set up a REALTIME table with partial upsert
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setUpsertConfig(upsertConfig)
        .build();

    String error = _executor.validateTableModeSafety(tableConfig);
    assertNotNull(error);
    assertEquals(error, "Partial upsert tables do not support direct file insert. "
        + "Use stream ingestion for partial upsert.");
  }

  @Test
  public void testRejectDedupTable() {
    /// Set up a REALTIME table with dedup enabled
    DedupConfig dedupConfig = new DedupConfig(true, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setDedupConfig(dedupConfig)
        .build();

    String error = _executor.validateTableModeSafety(tableConfig);
    assertNotNull(error);
    assertEquals(error, "Dedup tables do not support direct file insert in this version.");
  }

  @Test
  public void testFullUpsertWithPartitionConfig() {
    /// Set up a REALTIME table with full upsert and partition config
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    Map<String, ColumnPartitionConfig> partitionMap = new HashMap<>();
    partitionMap.put("userId", new ColumnPartitionConfig("murmur3", 4));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionMap);
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setSegmentPartitionConfig(partitionConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setUpsertConfig(upsertConfig)
        .build();
    tableConfig.setIndexingConfig(indexingConfig);

    String error = _executor.validateTableModeSafety(tableConfig);
    assertNull(error, "Full upsert with partition config should be allowed");
  }

  @Test
  public void testFullUpsertWithoutPartitionConfigRejected() {
    /// Set up a REALTIME table with full upsert but no partition config
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setUpsertConfig(upsertConfig)
        .build();

    String error = _executor.validateTableModeSafety(tableConfig);
    assertNotNull(error);
    assert error.contains("partition configuration");
  }

  @Test
  public void testExecuteTableNotFound() {
    /// Table config not found in ZK
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(null);

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_NOT_FOUND");
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*fileUri is required.*")
  public void testBuildRejectsEmptyFileUri() {
    new InsertRequest.Builder()
        .setStatementId("stmt-1")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.FILE)
        .setFileUri("")
        .build();
  }

  @Test
  public void testAbortBeforeCompletion()
      throws Exception {
    /// First execute to create state (no lineage entry yet — deferred to complete)
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult executeResult = _executor.execute(request);
    assertEquals(executeResult.getState(), InsertStatementState.ACCEPTED);

    String statementId = request.getStatementId();

    /// Abort before completion — abort hook is void; verify the cached task name was dropped.
    _executor.abort(statementId);
    org.testng.Assert.assertNull(_executor.getScheduledTaskName(statementId),
        "abort hook should drop the cached task name");
  }

  @Test
  public void testCompleteFileInsertReturnsVisibleResult()
      throws Exception {
    /// The coordinator provides the ZK manifest; the executor returns VISIBLE in the result so the
    /// coordinator can persist via persistWithCasRetry. The executor must NOT mutate the passed
    /// manifest (the coordinator's CAS retry re-reads from ZK on each attempt).
    InsertStatementManifest manifest = buildAcceptedManifest("stmt-complete", TABLE_NAME);
    List<String> segmentNames = Arrays.asList("seg1", "seg2");
    InsertResult result = _executor.prepareCompletionResult("stmt-complete", segmentNames, manifest);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getSegmentNames(), segmentNames);
    /// The passed manifest must be unchanged — coordinator owns the durable transition.
    assertEquals(manifest.getState(), InsertStatementState.ACCEPTED,
        "executor must not mutate the manifest; coordinator owns the CAS-driven state transition");
    /// Segments are already registered — no startReplaceSegments/endReplaceSegments needed
    verify(_resourceManager, never()).startReplaceSegments(anyString(), anyList(), anyList(), anyBoolean(), anyMap());
    verify(_resourceManager, never()).endReplaceSegments(anyString(), anyString(), any());
  }

  @Test
  public void testAbortIsBestEffort() {
    /// abort() is a post-ZK-update cleanup hook (void). Calling it with an unknown statementId
    /// must not throw — it's documented as best-effort.
    _executor.abort("unknown-stmt");
  }

  @Test
  public void testCompleteUnknownStatementReturnsAborted() {
    List<String> segmentNames = Arrays.asList("seg1");
    /// Null manifest simulates a caller that could not load from ZK.
    InsertResult result = _executor.prepareCompletionResult("non-existent-stmt", segmentNames, null);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "STATEMENT_NOT_FOUND");
  }

  @Test
  public void testCompleteAbortedStatementIsRejected() {
    /// Caller passes a manifest already in ABORTED state (set by the coordinator on user abort
    /// or cleanup sweep). completeFileInsert must refuse to resurrect it.
    InsertStatementManifest manifest = buildAcceptedManifest("stmt-aborted", TABLE_NAME);
    manifest.setState(InsertStatementState.ABORTED);

    InsertResult result = _executor.prepareCompletionResult("stmt-aborted", Arrays.asList("seg1"), manifest);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "STATEMENT_ABORTED",
        "completeFileInsert on an ABORTED statement must return STATEMENT_ABORTED, not resurrect it");
  }

  @Test
  public void testExecuteCreateTaskFails()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    when(_taskManager.createTask(anyString(), anyString(), anyString(), anyMap()))
        .thenThrow(new Exception("Minion not available"));

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TASK_SCHEDULE_FAILED");
  }

  @Test
  public void testValidateFileUriNull() {
    assertNotNull(_executor.validateFileUri(null));
  }

  @Test
  public void testValidateFileUriEmpty() {
    assertNotNull(_executor.validateFileUri(""));
  }

  @Test
  public void testValidateFileUriValid() {
    assertNull(_executor.validateFileUri("s3://bucket/path/file.json"));
    assertNull(_executor.validateFileUri("hdfs://namenode/data/file.parquet"));
  }

  @Test
  public void testValidateFileUriRejectsLocalScheme() {
    /// Local-filesystem schemes are rejected by default to prevent privilege-escalation via
    /// file:///etc/passwd from an authenticated EXECUTE_INSERT principal.
    assertNotNull(_executor.validateFileUri("file:///etc/passwd"));
    assertNotNull(_executor.validateFileUri("jar:file:/local.jar!/some.csv"));
    /// Schemeless local paths are also rejected (no scheme = no allowlist match).
    assertNotNull(_executor.validateFileUri("/local/path/file.csv"));
  }

  @Test
  public void testAppendTableAllowed() {
    /// OFFLINE append table
    TableConfig offlineConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    assertNull(_executor.validateTableModeSafety(offlineConfig));

    /// REALTIME append table (no upsert, no dedup)
    TableConfig realtimeConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .build();
    assertNull(_executor.validateTableModeSafety(realtimeConfig));
  }

  /// When createTask() returns an empty map (unschedulable or no subtasks), execute() must abort
  /// immediately with TASK_SCHEDULE_FAILED rather than leaving the manifest in ACCEPTED.
  @Test
  public void testExecuteAbortWhenTaskNameIsNull()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    /// Return empty map — no task was created for this table
    when(_taskManager.createTask(anyString(), anyString(), anyString(), anyMap()))
        .thenReturn(Collections.emptyMap());

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TASK_SCHEDULE_FAILED");
  }

  /// After controller failover (in-memory _taskNames is empty), resolveAcceptedStatementIfTaskDone
  /// must fall back to the ZK-persisted minionTaskName on the manifest. If the Minion task has
  /// completed, the statement transitions to VISIBLE.
  @Test
  public void testResolveAcceptedStatementFallsBackToZkManifest()
      throws Exception {
    PinotHelixTaskResourceManager taskResourceManager = mock(PinotHelixTaskResourceManager.class);
    FileInsertExecutor executorWithPolling =
        new FileInsertExecutor(_resourceManager, _taskManager, taskResourceManager);

    String statementId = "stmt-failover";
    String taskName = "task-job-failover";
    String tableNameWithType = TABLE_NAME;

    /// The ZK manifest has the task name (as persisted before failover)
    InsertStatementManifest zkManifest = new InsertStatementManifest(
        statementId, "req-1", "hash-1", tableNameWithType,
        InsertType.FILE, InsertStatementState.ACCEPTED,
        1000L, 2000L, Collections.emptyList(), null, null, taskName);

    /// The Minion task completed
    when(taskResourceManager.getTaskState(taskName)).thenReturn(TaskState.COMPLETED);

    /// No in-memory entry exists (simulating empty _taskNames after failover)
    assertNull(executorWithPolling.getScheduledTaskName(statementId));

    InsertStatementState resolution =
        executorWithPolling.resolveAcceptedStatementIfTaskDone(statementId, zkManifest);

    /// Should have auto-completed via ZK fallback
    assertEquals(resolution, InsertStatementState.VISIBLE);
    /// The manifest must NOT have been mutated in place — the coordinator owns the durable
    /// transition via persistWithCasRetry. The executor only signals "ready to flip via the
    /// returned InsertResult".
    assertEquals(zkManifest.getState(), InsertStatementState.ACCEPTED);
  }

  /// --- Helper methods ---

  private InsertRequest buildFileInsertRequest(String tableName, String fileUri) {
    return new InsertRequest.Builder()
        .setTableName(tableName)
        .setTableType(tableName.endsWith("_REALTIME") ? TableType.REALTIME : TableType.OFFLINE)
        .setInsertType(InsertType.FILE)
        .setFileUri(fileUri)
        .build();
  }

  private void mockTableConfig(String tableNameWithType, TableConfig tableConfig) {
    when(_resourceManager.getTableConfig(tableNameWithType)).thenReturn(tableConfig);
  }

  private static InsertStatementManifest buildAcceptedManifest(String statementId, String tableNameWithType) {
    long now = System.currentTimeMillis();
    return new InsertStatementManifest(statementId, null, null, tableNameWithType, InsertType.FILE,
        InsertStatementState.ACCEPTED, now, now, Collections.emptyList(), null, null, null);
  }

  private void mockCreateTask()
      throws Exception {
    /// Return the second arg (tableNameWithType) as the key so both OFFLINE and REALTIME tests work.
    when(_taskManager.createTask(anyString(), anyString(), anyString(), anyMap()))
        .thenAnswer(inv -> {
          String tableNameWithType = inv.getArgument(1);
          Map<String, String> result = new HashMap<>();
          result.put(tableNameWithType, "task-job-1");
          return result;
        });
  }
}
