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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for columnar reload functionality in RefreshSegmentTask.
 * This test validates the new column-wise segment rebuilding feature.
 */
public class RefreshSegmentColumnarTaskMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected final File _segmentDataDir = new File(_tempDir, "segmentDataDir");
  protected final File _segmentTarDir = new File(_tempDir, "segmentTarDir");

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDataDir, _segmentTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Create schema and tableConfig
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDataDir,
        _segmentTarDir);
    uploadSegments(getTableName(), _segmentTarDir);

    // Set up managers
    _taskManager = _controllerStarter.getTaskManager();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    // Wait for all documents to be loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(getTableName());
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testBasicColumnarReload() throws Exception {
    // Store original segment metadata for comparison
    SegmentMetadata originalMetadata = captureSegmentMetadata();

    // Execute columnar reload
    executeColumnarReload();

    // Validate basic metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcExists();

    // Validate query functionality
    validateBasicQueryWorks();

    System.out.println("Successfully completed basic columnar reload test");
  }

  @Test
  public void testColumnarReloadWithNewColumn() throws Exception {
    // Store original metadata
    Schema originalSchema = _pinotHelixResourceManager.getSchema(getTableName());
    int originalColumnCount = originalSchema.getColumnNames().size();
    SegmentMetadata originalMetadata = captureSegmentMetadata();

    // Clear transform configs and add new column
    clearTransformConfigs();
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec("NewTestColumn", FieldSpec.DataType.STRING, true));
    forceUpdateSchema(schema);

    // Execute columnar reload
    executeColumnarReload();

    // Validate metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcUpdated(originalMetadata);

    // Validate schema changes
    Schema updatedSchema = _pinotHelixResourceManager.getSchema(getTableName());
    assertEquals(updatedSchema.getColumnNames().size(), originalColumnCount + 1,
        "Schema should have one additional column");
    assertTrue(updatedSchema.getColumnNames().contains("NewTestColumn"),
        "New column should be present in schema");

    // Validate query functionality
    validateBasicQueryWorks();

    System.out.println("Successfully completed columnar reload with new column test");
  }

  @Test
  public void testColumnarReloadWithDatatypeChange() throws Exception {
    // Store original metadata and data types
    SegmentMetadata originalMetadata = captureSegmentMetadata();
    Schema originalSchema = _pinotHelixResourceManager.getSchema(getTableName());

    // Verify original data types (based on createSchema() method)
    assertEquals(originalSchema.getFieldSpecFor("ArrTime").getDataType(), FieldSpec.DataType.INT,
        "ArrTime should originally be INT datatype");
    assertEquals(originalSchema.getFieldSpecFor("AirlineID").getDataType(), FieldSpec.DataType.LONG,
        "AirlineID should originally be LONG datatype");

    // Change datatype for columns - use compatible changes
    Schema schema = createSchema();
    schema.getFieldSpecFor("ArrTime").setDataType(FieldSpec.DataType.LONG); // INT -> LONG (compatible)
    schema.getFieldSpecFor("ActualElapsedTime").setDataType(FieldSpec.DataType.FLOAT); // INT -> FLOAT (compatible)
    forceUpdateSchema(schema);

    // Verify schema was actually updated
    Schema updatedSchema = _pinotHelixResourceManager.getSchema(getTableName());
    assertEquals(updatedSchema.getFieldSpecFor("ArrTime").getDataType(), FieldSpec.DataType.LONG,
        "Schema update failed: ArrTime should be LONG datatype after update");
    assertEquals(updatedSchema.getFieldSpecFor("ActualElapsedTime").getDataType(), FieldSpec.DataType.FLOAT,
        "Schema update failed: ActualElapsedTime should be FLOAT datatype after update");

    // Execute columnar reload
    executeColumnarReload();

    // Validate metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcUpdated(originalMetadata);

    // Validate datatype changes are reflected in query responses
    validateQueryDataType("ArrTime", "LONG");
    validateQueryDataType("ActualElapsedTime", "FLOAT");

    // Validate data integrity with type-specific operations
    validateDataIntegrityAfterCompatibleTypeChange();

    System.out.println("Successfully completed columnar reload with datatype change test");
  }

  // Helper class to store segment metadata
  private static class SegmentMetadata {
    private final Map<String, Long> _segmentCrcMap = new HashMap<>();
    private final Map<String, String> _refreshTimeMap = new HashMap<>();
    private final String _refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;

    public void addSegment(String segmentName, Long crc, String refreshTime) {
      _segmentCrcMap.put(segmentName, crc);
      if (refreshTime != null) {
        _refreshTimeMap.put(segmentName, refreshTime);
      }
    }

    public Map<String, Long> getSegmentCrcMap() {
      return _segmentCrcMap;
    }

    public Map<String, String> getRefreshTimeMap() {
      return _refreshTimeMap;
    }

    public String getRefreshKey() {
      return _refreshKey;
    }
  }

  private SegmentMetadata captureSegmentMetadata() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    SegmentMetadata metadata = new SegmentMetadata();
    String refreshKey = metadata.getRefreshKey();

    for (SegmentZKMetadata zkMetadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = zkMetadata.getSegmentName();
      Long crc = zkMetadata.getCrc();
      String refreshTime = null;

      Map<String, String> customMap = zkMetadata.getCustomMap();
      if (customMap != null && customMap.containsKey(refreshKey)) {
        refreshTime = customMap.get(refreshKey);
      }

      metadata.addSegment(segmentName, crc, refreshTime);
    }

    return metadata;
  }

  private void executeColumnarReload() throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Enable columnar reload for the table
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(offlineTableName);
    tableConfig.setTaskConfig(getColumnarReloadTaskConfig());
    _pinotHelixResourceManager.updateTableConfig(tableConfig);

    // Schedule refresh task
    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));

    waitForTaskToComplete();
  }

  private void validateRefreshTimestampUpdated(SegmentMetadata originalMetadata) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    String refreshKey = originalMetadata.getRefreshKey();

    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = metadata.getSegmentName();

      // Check that refresh timestamp exists
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey),
          "Refresh timestamp not found in segment metadata for: " + segmentName);

      String newRefreshTime = customMap.get(refreshKey);
      if (originalMetadata.getRefreshTimeMap().containsKey(segmentName)) {
        // If segment was refreshed before, timestamp should be updated
        assertTrue(!originalMetadata.getRefreshTimeMap().get(segmentName).equals(newRefreshTime),
            "Refresh timestamp should be updated for segment: " + segmentName);
      }
    }
  }

  private void validateCrcExists() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = metadata.getSegmentName();
      Long crc = metadata.getCrc();
      assertNotNull(crc, "CRC should not be null for segment: " + segmentName);
    }
  }

  private void validateCrcUpdated(SegmentMetadata originalMetadata) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = metadata.getSegmentName();
      Long newCrc = metadata.getCrc();
      Long originalCrc = originalMetadata.getSegmentCrcMap().get(segmentName);

      assertTrue(!originalCrc.equals(newCrc),
          "CRC should be updated for segment: " + segmentName);
    }
  }

  private void validateBasicQueryWorks() {
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM mytable";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("rows").get(0).get(0).asLong() > 0;
      } catch (Exception e) {
        return false;
      }
    });
  }

  private void validateQueryDataType(String column, String expectedDataType) {
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT " + column + " FROM mytable LIMIT 10";
        JsonNode response = postQuery(query);
        JsonNode dataSchema = response.get("resultTable").get("dataSchema");
        JsonNode columnDataTypes = dataSchema.get("columnDataTypes");
        return columnDataTypes.toString().contains(expectedDataType);
      } catch (Exception e) {
        return false;
      }
    });
  }

  private void validateDataIntegrityAfterCompatibleTypeChange() {
    // Test that LONG operations work on ArrTime (INT -> LONG conversion)
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM mytable WHERE ArrTime > 1000";
        JsonNode response = postQuery(query);
        long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
        return count >= 0; // Should not throw exception
      } catch (Exception e) {
        System.err.println("Error validating LONG operations on ArrTime: " + e.getMessage());
        return false;
      }
    });

    // Test that FLOAT operations work on ActualElapsedTime (INT -> FLOAT conversion)
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT AVG(ActualElapsedTime) FROM mytable";
        JsonNode response = postQuery(query);
        // Should be able to calculate average on FLOAT column
        JsonNode rows = response.get("resultTable").get("rows");
        return rows != null && rows.size() > 0; // Should not throw exception
      } catch (Exception e) {
        System.err.println("Error validating FLOAT operations on ActualElapsedTime: " + e.getMessage());
        return false;
      }
    });

    // Test arithmetic operations work with the new types
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM mytable WHERE ArrTime + ActualElapsedTime > 0";
        JsonNode response = postQuery(query);
        long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
        return count >= 0; // Should not throw exception with mixed LONG and FLOAT operations
      } catch (Exception e) {
        System.err.println("Error validating mixed type operations: " + e.getMessage());
        return false;
      }
    });
  }



  private void clearTransformConfigs() throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(offlineTableName);

    boolean configChanged = false;
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getTransformConfigs() != null) {
      tableConfig.getIngestionConfig().setTransformConfigs(null);
      configChanged = true;
    }
    if (configChanged) {
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
    }
  }

  private TableTaskConfig getColumnarReloadTaskConfig() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("columnarReloadAndSkipTransformation", "true");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, taskConfigs));
  }

  protected void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(MinionConstants.RefreshSegmentTask.TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }

  protected void waitForServerSegmentDownload(Function<Void, Boolean> conditionFunc) {
    TestUtils.waitForCondition(aVoid -> {
      boolean val = conditionFunc.apply(aVoid);
      return val;
    }, 60_000L, "Failed to meet condition");
  }

  @Override
  protected String getTableName() {
    return "myTable";
  }

  @Override
  protected String getAvroTarFileName() {
    return "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz";
  }
}
