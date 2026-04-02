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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for {@link RowInsertExecutor} execute, getStatus, and abort flow.
 */
public class RowInsertExecutorTest {

  private File _tempDir;
  private LocalShardLogProvider _shardLogProvider;
  private LocalPreparedStore _preparedStore;
  private ConcurrentHashMap<String, TableConfig> _tableConfigs;
  private RowInsertExecutor _executor;

  @BeforeMethod
  public void setUp() {
    _tempDir = new File(FileUtils.getTempDirectory(), "row-executor-test-" + System.currentTimeMillis());
    _tempDir.mkdirs();

    File logsDir = new File(_tempDir, "logs");
    _shardLogProvider = new LocalShardLogProvider();
    _shardLogProvider.init(logsDir);

    File preparedDir = new File(_tempDir, "prepared");
    _preparedStore = new LocalPreparedStore();
    _preparedStore.init(preparedDir);

    _tableConfigs = new ConcurrentHashMap<>();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    _tableConfigs.put("testTable_OFFLINE", tableConfig);

    _executor = new RowInsertExecutor(_shardLogProvider, _preparedStore, _tableConfigs);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testExecuteSuccess() {
    GenericRow row1 = new GenericRow();
    row1.putValue("id", 1);
    row1.putValue("name", "Alice");

    GenericRow row2 = new GenericRow();
    row2.putValue("id", 2);
    row2.putValue("name", "Bob");

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-1")
        .setTableName("testTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Arrays.asList(row1, row2))
        .build();

    InsertResult result = _executor.execute(request);

    assertNotNull(result);
    assertEquals(result.getStatementId(), "stmt-1");
    assertEquals(result.getState(), InsertStatementState.PREPARED);
    assertNotNull(result.getMessage());
  }

  @Test
  public void testExecuteEmptyRows() {
    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-2")
        .setTableName("testTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.emptyList())
        .build();

    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "EMPTY_ROWS");
  }

  @Test
  public void testExecuteTableNotFound() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-3")
        .setTableName("nonExistentTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(row))
        .build();

    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_NOT_FOUND");
  }

  @Test
  public void testExecuteNullTableName() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-4")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(row))
        .build();

    InsertResult result = _executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "INVALID_TABLE");
  }

  @Test
  public void testGetStatus() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-5")
        .setTableName("testTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(row))
        .build();

    _executor.execute(request);

    InsertResult status = _executor.getStatus("stmt-5");
    assertEquals(status.getStatementId(), "stmt-5");
    assertEquals(status.getState(), InsertStatementState.PREPARED);
  }

  @Test
  public void testGetStatusUnknown() {
    InsertResult status = _executor.getStatus("unknown-stmt");
    assertEquals(status.getState(), InsertStatementState.ABORTED);
    assertNotNull(status.getMessage());
  }

  @Test
  public void testAbort() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-6")
        .setTableName("testTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(row))
        .build();

    _executor.execute(request);

    InsertResult abortResult = _executor.abort("stmt-6");
    assertEquals(abortResult.getStatementId(), "stmt-6");
    assertEquals(abortResult.getState(), InsertStatementState.ABORTED);

    // Verify the state is persisted
    InsertResult statusAfterAbort = _executor.getStatus("stmt-6");
    assertEquals(statusAfterAbort.getState(), InsertStatementState.ABORTED);
  }

  @Test
  public void testAbortUnknown() {
    InsertResult result = _executor.abort("unknown-stmt");
    assertEquals(result.getState(), InsertStatementState.ABORTED);
  }

  @Test
  public void testPreparedDataPersisted() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);
    row.putValue("name", "Test");

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-7")
        .setTableName("testTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(row))
        .build();

    _executor.execute(request);

    // Verify the data is in the prepared store
    byte[] stored = _preparedStore.load("stmt-7", 0, 0);
    assertNotNull(stored);

    List<String> statements = _preparedStore.listPreparedStatements();
    assertEquals(statements.size(), 1);
    assertEquals(statements.get(0), "stmt-7");
  }
}
