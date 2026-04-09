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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ControllerRowInsertExecutor}, focusing on the two-phase build/upload
 * approach and rollback behavior when upload fails partway through a multi-partition insert.
 */
public class ControllerRowInsertExecutorTest {

  private static final String TABLE_NAME = "testTable_OFFLINE";

  private PinotHelixResourceManager _resourceManager;
  private File _tempDir;

  @BeforeMethod
  public void setUp() {
    _resourceManager = mock(PinotHelixResourceManager.class);
    _tempDir = new File(System.getProperty("java.io.tmpdir"),
        "row_insert_test_" + System.currentTimeMillis());
    _tempDir.mkdirs();
  }

  @AfterMethod
  public void tearDown() {
    org.apache.commons.io.FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Creates a test executor that uses a temp dir for working files and overrides uploadSegment
   * to avoid ControllerFilePathProvider / deep store dependencies.
   */
  private ControllerRowInsertExecutor createTestExecutor(AtomicInteger uploadCallCount,
      List<String> uploadedSegmentNames, int failOnCallNumber) {
    return new ControllerRowInsertExecutor(_resourceManager) {
      @Override
      File createWorkingDir(String tableNameWithType) {
        return new File(_tempDir, "work_" + System.nanoTime());
      }

      @Override
      void uploadSegment(String tableNameWithType, TableConfig config, File segmentDir,
          File segmentTarFile, String segmentName)
          throws Exception {
        int callNum = uploadCallCount.incrementAndGet();
        if (failOnCallNumber > 0 && callNum >= failOnCallNumber) {
          throw new RuntimeException("Simulated upload failure on call " + callNum);
        }
        if (uploadedSegmentNames != null) {
          uploadedSegmentNames.add(segmentName);
        }
      }
    };
  }

  @Test
  public void testEmptyRowsRejected() {
    ControllerRowInsertExecutor executor = createTestExecutor(new AtomicInteger(), null, -1);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-empty")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(new ArrayList<>())
        .build();

    InsertResult result = executor.execute(request);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "EMPTY_ROWS");
  }

  @Test
  public void testTableNotFound() {
    ControllerRowInsertExecutor executor = createTestExecutor(new AtomicInteger(), null, -1);
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(null);

    List<GenericRow> rows = new ArrayList<>();
    GenericRow row = new GenericRow();
    row.putValue("col1", "val1");
    rows.add(row);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-no-table")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(rows)
        .build();

    InsertResult result = executor.execute(request);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_NOT_FOUND");
  }

  @Test
  public void testSuccessfulSinglePartitionInsert() {
    AtomicInteger uploadCallCount = new AtomicInteger();
    List<String> uploadedNames = new ArrayList<>();
    ControllerRowInsertExecutor executor = createTestExecutor(uploadCallCount, uploadedNames, -1);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .build();
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(tableConfig);
    when(_resourceManager.getTableSchema(TABLE_NAME)).thenReturn(schema);

    List<GenericRow> rows = new ArrayList<>();
    GenericRow row = new GenericRow();
    row.putValue("col1", "hello");
    rows.add(row);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-ok")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(rows)
        .build();

    InsertResult result = executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(uploadCallCount.get(), 1);
    assertEquals(result.getSegmentNames().size(), 1);
    verify(_resourceManager, never()).deleteSegment(anyString(), anyString());
  }

  /**
   * Verifies that when upload fails on the second partition of a multi-partition insert,
   * the first partition's already-uploaded segment is rolled back via deleteSegment.
   * This is the core test requested by the adversarial review.
   */
  @Test
  public void testMultiPartitionUploadFailureRollsBackFirstPartition() {
    // Setup: partition config with 2 partitions on col1 using Murmur3
    Map<String, ColumnPartitionConfig> partitionMap = new HashMap<>();
    partitionMap.put("col1", new ColumnPartitionConfig("murmur3", 2));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionMap);
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setSegmentPartitionConfig(partitionConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    tableConfig.setIndexingConfig(indexingConfig);

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .build();
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(tableConfig);
    when(_resourceManager.getTableSchema(TABLE_NAME)).thenReturn(schema);

    // Upload tracking: first upload succeeds, second fails
    AtomicInteger uploadCallCount = new AtomicInteger();
    List<String> uploadedNames = new ArrayList<>();
    ControllerRowInsertExecutor executor = createTestExecutor(uploadCallCount, uploadedNames, 2);

    // Generate enough rows to land in both partitions (Murmur3 with varied keys)
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", "key_" + i);
      rows.add(row);
    }

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-multi-fail")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(rows)
        .build();

    InsertResult result = executor.execute(request);

    // Result should indicate failure with rollback info
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "SEGMENT_UPLOAD_FAILED");
    assertNotNull(result.getMessage());
    // Both segments are tracked for rollback: the first was uploaded successfully,
    // the second was tracked before upload but failed during upload.
    assertTrue(result.getMessage().contains("rolled back 2 segments"),
        "Error message should indicate rollback count: " + result.getMessage());

    // The first segment was uploaded successfully then rolled back;
    // the second segment failed upload but its deep store copy (if any) is also cleaned up.
    assertEquals(uploadedNames.size(), 1, "Exactly one segment should have completed upload before failure");
    // deleteSegment called for both tracked segments
    verify(_resourceManager, times(2)).deleteSegment(eq(TABLE_NAME), anyString());
  }

  /**
   * Verifies that when ALL uploads succeed in a multi-partition insert,
   * no rollback occurs and all segments are reported as visible.
   */
  @Test
  public void testMultiPartitionInsertSuccess() {
    Map<String, ColumnPartitionConfig> partitionMap = new HashMap<>();
    partitionMap.put("col1", new ColumnPartitionConfig("murmur3", 2));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionMap);
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setSegmentPartitionConfig(partitionConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    tableConfig.setIndexingConfig(indexingConfig);

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .build();
    when(_resourceManager.getTableConfig(TABLE_NAME)).thenReturn(tableConfig);
    when(_resourceManager.getTableSchema(TABLE_NAME)).thenReturn(schema);

    AtomicInteger uploadCallCount = new AtomicInteger();
    List<String> uploadedNames = new ArrayList<>();
    ControllerRowInsertExecutor executor = createTestExecutor(uploadCallCount, uploadedNames, -1);

    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", "key_" + i);
      rows.add(row);
    }

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-multi-ok")
        .setTableName(TABLE_NAME)
        .setTableType(TableType.OFFLINE)
        .setInsertType(InsertType.ROW)
        .setRows(rows)
        .build();

    InsertResult result = executor.execute(request);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(uploadCallCount.get(), 2, "Should have uploaded 2 segments (one per partition)");
    assertEquals(result.getSegmentNames().size(), 2);
    verify(_resourceManager, never()).deleteSegment(anyString(), anyString());
  }
}
