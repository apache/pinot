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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Tests for {@link FileInsertExecutor} covering table mode safety validation, file URI validation,
 * and the execute/abort lifecycle.
 */
public class FileInsertExecutorTest {

  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String REALTIME_TABLE = "testTable_REALTIME";
  private static final String FILE_URI = "s3://my-bucket/data/file.json";
  private static final String LINEAGE_ENTRY_ID = "lineage-entry-123";

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
    // Set up an OFFLINE append table (no upsert, no dedup)
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    mockStartReplaceSegments();
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
    assertNotNull(_executor.getLineageEntryId(request.getStatementId()));
    verify(_resourceManager).startReplaceSegments(
        eq(TABLE_NAME), anyList(), anyList(), anyBoolean(), anyMap());
    verify(_taskManager).createTask(eq("SegmentGenerationAndPushTask"), eq(TABLE_NAME),
        anyString(), anyMap());
  }

  @Test
  public void testExecuteAppendRealtimeTable()
      throws Exception {
    // Set up a REALTIME append table (no upsert, no dedup)
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .build();
    mockTableConfig(REALTIME_TABLE, tableConfig);
    mockStartReplaceSegments();
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(REALTIME_TABLE, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testRejectPartialUpsertTable() {
    // Set up a REALTIME table with partial upsert
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
    // Set up a REALTIME table with dedup enabled
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
    // Set up a REALTIME table with full upsert and partition config
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
    // Set up a REALTIME table with full upsert but no partition config
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
    // Table config not found in ZK
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(null);

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_NOT_FOUND");
  }

  @Test
  public void testExecuteInvalidFileUri() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);

    // Build request with empty file URI
    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-1")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.FILE)
        .setFileUri("")
        .build();

    InsertResult result = _executor.execute(request);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "INVALID_FILE_URI");
  }

  @Test
  public void testAbortCleansUpSegments()
      throws Exception {
    // First execute to create state
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    mockStartReplaceSegments();
    mockCreateTask();

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult executeResult = _executor.execute(request);
    assertEquals(executeResult.getState(), InsertStatementState.ACCEPTED);

    String statementId = request.getStatementId();

    // Now abort
    InsertResult abortResult = _executor.abort(statementId);
    assertEquals(abortResult.getState(), InsertStatementState.ABORTED);

    // Verify revertReplaceSegments was called
    verify(_resourceManager).revertReplaceSegments(eq(TABLE_NAME), eq(LINEAGE_ENTRY_ID), eq(false), any());

    // Verify lineage entry is removed
    assertNull(_executor.getLineageEntryId(statementId));
  }

  @Test
  public void testGetStatusUnknownStatement() {
    InsertResult result = _executor.getStatus("unknown-stmt");
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "STATEMENT_NOT_FOUND");
  }

  @Test
  public void testAbortUnknownStatement() {
    InsertResult result = _executor.abort("unknown-stmt");
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "STATEMENT_NOT_FOUND");
  }

  @Test
  public void testExecuteStartReplaceSegmentsFails() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    when(_resourceManager.startReplaceSegments(anyString(), anyList(), anyList(), anyBoolean(), anyMap()))
        .thenThrow(new RuntimeException("ZK connection lost"));

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "SEGMENT_REPLACE_FAILED");
  }

  @Test
  public void testExecuteCreateTaskFails()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    mockTableConfig(TABLE_NAME, tableConfig);
    mockStartReplaceSegments();
    when(_taskManager.createTask(anyString(), anyString(), anyString(), anyMap()))
        .thenThrow(new Exception("Minion not available"));

    InsertRequest request = buildFileInsertRequest(TABLE_NAME, FILE_URI);
    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TASK_SCHEDULE_FAILED");

    // Verify revert was called
    verify(_resourceManager).revertReplaceSegments(eq(TABLE_NAME), eq(LINEAGE_ENTRY_ID), eq(false), any());
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
    assertNull(_executor.validateFileUri("/local/path/file.csv"));
  }

  @Test
  public void testAppendTableAllowed() {
    // OFFLINE append table
    TableConfig offlineConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    assertNull(_executor.validateTableModeSafety(offlineConfig));

    // REALTIME append table (no upsert, no dedup)
    TableConfig realtimeConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .build();
    assertNull(_executor.validateTableModeSafety(realtimeConfig));
  }

  // --- Helper methods ---

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

  private void mockStartReplaceSegments() {
    when(_resourceManager.startReplaceSegments(anyString(), anyList(), anyList(), anyBoolean(), anyMap()))
        .thenReturn(LINEAGE_ENTRY_ID);
  }

  private void mockCreateTask()
      throws Exception {
    Map<String, String> taskResponse = new HashMap<>();
    taskResponse.put(TABLE_NAME, "task-job-1");
    when(_taskManager.createTask(anyString(), anyString(), anyString(), anyMap()))
        .thenReturn(taskResponse);
  }
}
