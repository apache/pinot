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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.apache.pinot.spi.ingest.ShardLog;
import org.apache.pinot.spi.ingest.ShardLogProvider;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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
    // Builder.build() now validates tableName is required, so this should throw
    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    try {
      new InsertRequest.Builder()
          .setStatementId("stmt-4")
          .setInsertType(InsertType.ROW)
          .setRows(Collections.singletonList(row))
          .build();
      fail("Expected IllegalArgumentException for null tableName");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("tableName"));
    }
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
    LocalShardLog shardLog = (LocalShardLog) _shardLogProvider.getShardLog("testTable_OFFLINE", 0);
    assertEquals(shardLog.getStatementMeta("stmt-6").getStatus(), LocalShardLog.StatementStatus.PREPARED);

    InsertResult abortResult = _executor.abort("stmt-6");
    assertEquals(abortResult.getStatementId(), "stmt-6");
    assertEquals(abortResult.getState(), InsertStatementState.ABORTED);
    assertNull(_preparedStore.load("stmt-6", 0, 0));
    assertEquals(shardLog.getStatementMeta("stmt-6").getStatus(), LocalShardLog.StatementStatus.ABORTED);

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

  @Test
  public void testExecuteRollbackOnFailureAfterPrepare() {
    RecordingShardLogProvider shardLogProvider = new RecordingShardLogProvider();
    FailingPreparedStore preparedStore = new FailingPreparedStore(2);
    ConcurrentHashMap<String, TableConfig> tableConfigs = new ConcurrentHashMap<>();
    tableConfigs.put("partitionedTable_OFFLINE", buildPartitionedTableConfig());
    RowInsertExecutor executor = new RowInsertExecutor(shardLogProvider, preparedStore, tableConfigs);

    GenericRow row0 = new GenericRow();
    row0.putValue("id", "0");
    GenericRow row1 = new GenericRow();
    row1.putValue("id", "1");

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-rollback")
        .setTableName("partitionedTable_OFFLINE")
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(Arrays.asList(row0, row1))
        .build();

    InsertResult result = executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "WRITE_ERROR");
    assertEquals(shardLogProvider.getPrepareCount(), 1);
    assertEquals(shardLogProvider.getAbortCount(), 1);
    assertTrue(preparedStore.isCleanedUp("stmt-rollback"));
    assertEquals(executor.getStatus("stmt-rollback").getState(), InsertStatementState.ABORTED);
  }

  private static TableConfig buildPartitionedTableConfig() {
    ConcurrentHashMap<String, ColumnPartitionConfig> partitionMap = new ConcurrentHashMap<>();
    partitionMap.put("id", new ColumnPartitionConfig("HashCode", 2));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionMap);
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setSegmentPartitionConfig(partitionConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("partitionedTable")
        .build();
    tableConfig.setIndexingConfig(indexingConfig);
    return tableConfig;
  }

  private static final class FailingPreparedStore implements PreparedStore {
    private final int _failureCall;
    private final List<String> _cleanedStatements = new ArrayList<>();
    private int _storeCalls;

    private FailingPreparedStore(int failureCall) {
      _failureCall = failureCall;
    }

    @Override
    public void init(PinotConfiguration config, File dataDir) {
    }

    @Override
    public void store(String statementId, int partitionId, long sequenceNo, byte[] data) {
      _storeCalls++;
      if (_storeCalls == _failureCall) {
        throw new RuntimeException("simulated prepared-store failure");
      }
    }

    @Override
    public byte[] load(String statementId, int partitionId, long sequenceNo) {
      return null;
    }

    @Override
    public List<String> listPreparedStatements() {
      return Collections.emptyList();
    }

    @Override
    public void cleanup(String statementId) {
      _cleanedStatements.add(statementId);
    }

    private boolean isCleanedUp(String statementId) {
      return _cleanedStatements.contains(statementId);
    }
  }

  private static final class RecordingShardLogProvider implements ShardLogProvider {
    private final ConcurrentHashMap<Integer, RecordingShardLog> _logs = new ConcurrentHashMap<>();

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public ShardLog getShardLog(String tableNameWithType, int partitionId) {
      return _logs.computeIfAbsent(partitionId, ignored -> new RecordingShardLog());
    }

    @Override
    public void close() {
    }

    private int getPrepareCount() {
      int prepareCount = 0;
      for (RecordingShardLog log : _logs.values()) {
        prepareCount += log._prepareCount;
      }
      return prepareCount;
    }

    private int getAbortCount() {
      int abortCount = 0;
      for (RecordingShardLog log : _logs.values()) {
        abortCount += log._abortCount;
      }
      return abortCount;
    }
  }

  private static final class RecordingShardLog implements ShardLog {
    private int _prepareCount;
    private int _abortCount;

    @Override
    public long append(byte[] data) {
      return 0;
    }

    @Override
    public void prepare(String statementId, long startOffset, long endOffset) {
      _prepareCount++;
    }

    @Override
    public void commit(String statementId) {
    }

    @Override
    public void abort(String statementId) {
      _abortCount++;
    }

    @Override
    public Iterator<byte[]> read(long fromOffset) {
      return Collections.emptyIterator();
    }
  }
}
