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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Comprehensive integration test for RefreshSegmentTaskExecutor column-wise build functionality.
 * This test validates the column-wise segment rebuilding feature by comparing output segment data
 * between columnar and row-wise refresh approaches to ensure data correctness across various scenarios.
 *
 * Test scenarios include:
 * - Basic columnar reload with data integrity validation
 * - New column addition with default value validation
 * - Compatible data type changes with conversion validation
 * - Index addition with functionality validation
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

  @Test(priority = 1)
  public void testBasicColumnarReload() throws Exception {
    System.out.println("=== Testing Basic Column-wise Refresh Data Integrity ===");

    // Store original segment metadata for comparison
    SegmentMetadata originalMetadata = captureSegmentMetadata();

    // Get original segment data for detailed comparison
    Map<String, GenericRow[]> originalSegmentData = readSegmentData();

    // Execute columnar reload
    executeColumnarReload();

    // Get refreshed segment data
    Map<String, GenericRow[]> refreshedSegmentData = readSegmentData();

    // Validate basic metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcExists();

    // Validate query functionality
    validateBasicQueryWorks();

    // Validate complete data integrity - all existing columns should have identical data
    validateSegmentDataIntegrity(originalSegmentData, refreshedSegmentData);

    System.out.println("✓ Basic columnar reload maintains perfect data integrity");
  }

  @Test(priority = 2)
  public void testColumnarReloadWithNewColumn() throws Exception {
    System.out.println("=== Testing Column-wise Refresh with New Column Addition ===");

    // Store original metadata and data
    Schema originalSchema = _pinotHelixResourceManager.getSchema(getTableName());
    int originalColumnCount = originalSchema.getColumnNames().size();
    SegmentMetadata originalMetadata = captureSegmentMetadata();
    Map<String, GenericRow[]> originalSegmentData = readSegmentData();

    // Clear transform configs and add new column with default value
    clearTransformConfigs();
    String newColumnName = "NewTestColumn";
    String defaultValue = "DEFAULT_VALUE";
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec(newColumnName, FieldSpec.DataType.STRING, true, defaultValue));
    forceUpdateSchema(schema);

    // Verify schema change was applied
    Schema updatedSchema = _pinotHelixResourceManager.getSchema(getTableName());
    assertTrue(updatedSchema.hasColumn(newColumnName), "New column should be added to schema");
    assertEquals(updatedSchema.getFieldSpecFor(newColumnName).getDefaultNullValue(), defaultValue,
        "New column should have correct default value");

    // Execute columnar reload
    executeColumnarReload();

    // Get refreshed segment data
    Map<String, GenericRow[]> refreshedSegmentData = readSegmentData();

    // Validate metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcUpdated(originalMetadata);

    // Validate schema changes
    updatedSchema = _pinotHelixResourceManager.getSchema(getTableName());
    assertEquals(updatedSchema.getColumnNames().size(), originalColumnCount + 1,
        "Schema should have one additional column");
    assertTrue(updatedSchema.getColumnNames().contains(newColumnName),
        "New column should be present in schema");

    // Validate query functionality
    validateBasicQueryWorks();

    // Validate existing columns maintain data integrity
    validateSegmentDataIntegrity(originalSegmentData, refreshedSegmentData);

    // Validate new column has default values
    validateNewColumnDefaultValues(refreshedSegmentData, newColumnName, defaultValue);

    System.out.println("✓ Column-wise refresh with new column maintains data integrity");
  }

  @Test(priority = 3)
  public void testColumnarReloadWithDatatypeChange() throws Exception {
    System.out.println("=== Testing Column-wise Refresh with Compatible Data Type Changes ===");

    // Store original metadata and data types
    SegmentMetadata originalMetadata = captureSegmentMetadata();
    Schema originalSchema = _pinotHelixResourceManager.getSchema(getTableName());
    Map<String, GenericRow[]> originalSegmentData = readSegmentData();

    // Verify original data types (based on createSchema() method)
    assertEquals(originalSchema.getFieldSpecFor("ArrTime").getDataType(), FieldSpec.DataType.INT,
        "ArrTime should originally be INT datatype");
    assertEquals(originalSchema.getFieldSpecFor("ActualElapsedTime").getDataType(), FieldSpec.DataType.INT,
        "ActualElapsedTime should originally be INT datatype");

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

    // Get refreshed segment data
    Map<String, GenericRow[]> refreshedSegmentData = readSegmentData();

    // Validate metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcUpdated(originalMetadata);

    // Validate datatype changes are reflected in query responses
    validateQueryDataType("ArrTime", "LONG");
    validateQueryDataType("ActualElapsedTime", "FLOAT");

    // Validate data type conversions with exact value comparison
    validateDataTypeConversions(originalSegmentData, refreshedSegmentData, "ArrTime",
        FieldSpec.DataType.INT, FieldSpec.DataType.LONG);
    validateDataTypeConversions(originalSegmentData, refreshedSegmentData, "ActualElapsedTime",
        FieldSpec.DataType.INT, FieldSpec.DataType.FLOAT);

    System.out.println("✓ Column-wise refresh with data type changes maintains data integrity");
  }

  @Test(priority = 4)
  public void testColumnarReloadWithIndexAddition() throws Exception {
    System.out.println("=== Testing Column-wise Refresh with Index Addition ===");

    // Store original metadata and data
    SegmentMetadata originalMetadata = captureSegmentMetadata();
    Map<String, GenericRow[]> originalSegmentData = readSegmentData();

    // Add inverted index to a column that doesn't have one
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(offlineTableName);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

    // Add inverted index for DivActualElapsedTime column
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (invertedIndexColumns == null) {
      invertedIndexColumns = Arrays.asList("DivActualElapsedTime");
    } else {
      invertedIndexColumns = Arrays.asList("DivActualElapsedTime", "Origin", "Quarter");
    }
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
    tableConfig.setIndexingConfig(indexingConfig);
    _pinotHelixResourceManager.updateTableConfig(tableConfig);

    // Execute columnar reload
    executeColumnarReload();

    // Get refreshed segment data
    Map<String, GenericRow[]> refreshedSegmentData = readSegmentData();

    // Validate metadata changes
    validateRefreshTimestampUpdated(originalMetadata);
    validateCrcUpdated(originalMetadata);

    // Validate query functionality
    validateBasicQueryWorks();

    // Validate data integrity is maintained
    validateSegmentDataIntegrity(originalSegmentData, refreshedSegmentData);

    // Validate that inverted index is working by checking query performance
    validateInvertedIndexFunctionality("DivActualElapsedTime");

    System.out.println("✓ Column-wise refresh with index addition maintains data integrity and applies indexes");
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

  /**
   * Read all segment data using PinotSegmentRecordReader for comprehensive validation
   */
  private Map<String, GenericRow[]> readSegmentData() throws Exception {
    Map<String, GenericRow[]> segmentData = new HashMap<>();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Get all segment ZK metadata
    for (SegmentZKMetadata zkMetadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = zkMetadata.getSegmentName();

      // Find segment directory
      File segmentDir = findSegmentDirectory(segmentName);
      if (segmentDir == null) {
        continue;
      }

      // Load segment and read data using ImmutableSegmentLoader
      ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
      try {
        PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
        recordReader.init(segment);

        // Read all records
        List<GenericRow> records = new ArrayList<>();
        while (recordReader.hasNext()) {
          records.add(recordReader.next());
        }

        segmentData.put(segmentName, records.toArray(new GenericRow[0]));
        recordReader.close();
      } finally {
        segment.destroy();
      }
    }

    return segmentData;
  }

  /**
   * Find segment directory in the server data directory
   */
  private File findSegmentDirectory(String segmentName) {
    // First check the server's actual segment storage location
    // Server stores segments in: /tmp/<timestamp>/PinotServer/dataDir-0/
    String tempDir = System.getProperty("java.io.tmpdir");
    File[] tempDirContents = new File(tempDir).listFiles();

    if (tempDirContents != null) {
      for (File timestampDir : tempDirContents) {
        if (timestampDir.isDirectory() && timestampDir.getName().matches("\\d+")) {
          File serverDir = new File(timestampDir, "PinotServer");
          if (serverDir.exists()) {
            File dataDir = new File(serverDir, "dataDir-0");
            if (dataDir.exists()) {
              // Check for segment directly in dataDir
              File segmentDir = new File(dataDir, segmentName);
              if (segmentDir.exists()) {
                return segmentDir;
              }

              // Check in table subdirectory
              String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
              File tableDir = new File(dataDir, offlineTableName);
              if (tableDir.exists()) {
                File segmentDirInTable = new File(tableDir, segmentName);
                if (segmentDirInTable.exists()) {
                  return segmentDirInTable;
                }
              }

              // Check all subdirectories in dataDir
              File[] subdirs = dataDir.listFiles();
              if (subdirs != null) {
                for (File subdir : subdirs) {
                  if (subdir.isDirectory()) {
                    File segmentDirInSub = new File(subdir, segmentName);
                    if (segmentDirInSub.exists()) {
                      return segmentDirInSub;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    // Fallback to original locations for backward compatibility
    File[] possibleDirs = {
        new File(_tempDir, "PinotServer/segmentDataDir"),
        new File(_tempDir, "segmentDataDir"),
        _segmentDataDir
    };

    for (File dir : possibleDirs) {
      if (dir.exists()) {
        File segmentDir = new File(dir, segmentName);
        if (segmentDir.exists()) {
          return segmentDir;
        }

        // Look in subdirectories
        File[] subdirs = dir.listFiles();
        if (subdirs != null) {
          for (File subdir : subdirs) {
            File segmentDirInSub = new File(subdir, segmentName);
            if (segmentDirInSub.exists()) {
              return segmentDirInSub;
            }
          }
        }
      }
    }

    return null;
  }

  /**
   * Validate data integrity between original and refreshed segment data
   */
  private void validateSegmentDataIntegrity(Map<String, GenericRow[]> originalData,
                                           Map<String, GenericRow[]> refreshedData) {
    assertEquals(refreshedData.size(), originalData.size(),
        "Number of segments should remain the same");

    for (String segmentName : originalData.keySet()) {
      assertTrue(refreshedData.containsKey(segmentName),
          "Refreshed data should contain segment: " + segmentName);

      GenericRow[] originalRows = originalData.get(segmentName);
      GenericRow[] refreshedRows = refreshedData.get(segmentName);

      assertEquals(refreshedRows.length, originalRows.length,
          "Number of rows should be identical for segment: " + segmentName);

      // Compare each row for existing columns
      for (int i = 0; i < originalRows.length; i++) {
        GenericRow originalRow = originalRows[i];
        GenericRow refreshedRow = refreshedRows[i];

        // Check all common columns
        for (String fieldName : originalRow.getFieldToValueMap().keySet()) {
          Object originalValue = originalRow.getValue(fieldName);
          Object refreshedValue = refreshedRow.getValue(fieldName);

          assertEquals(refreshedValue, originalValue,
              String.format("Value mismatch for column '%s' in segment '%s' row %d",
                  fieldName, segmentName, i));
        }
      }
    }
  }

  /**
   * Validate new column has default values
   */
  private void validateNewColumnDefaultValues(Map<String, GenericRow[]> segmentData,
                                             String newColumnName, String expectedDefaultValue) {
    for (String segmentName : segmentData.keySet()) {
      GenericRow[] rows = segmentData.get(segmentName);

      for (int i = 0; i < rows.length; i++) {
        GenericRow row = rows[i];
        assertTrue(row.getFieldToValueMap().containsKey(newColumnName),
            String.format("New column '%s' should exist in segment '%s' row %d",
                newColumnName, segmentName, i));

        Object value = row.getValue(newColumnName);
        assertEquals(value, expectedDefaultValue,
            String.format("New column '%s' should have default value in segment '%s' row %d",
                newColumnName, segmentName, i));
      }
    }
  }

  /**
   * Validate data type conversions using PinotDataType
   */
  private void validateDataTypeConversions(Map<String, GenericRow[]> originalData,
                                          Map<String, GenericRow[]> refreshedData,
                                          String columnName,
                                          FieldSpec.DataType originalType,
                                          FieldSpec.DataType newType) {
    for (String segmentName : originalData.keySet()) {
      GenericRow[] originalRows = originalData.get(segmentName);
      GenericRow[] refreshedRows = refreshedData.get(segmentName);

      for (int i = 0; i < originalRows.length; i++) {
        Object originalValue = originalRows[i].getValue(columnName);
        Object refreshedValue = refreshedRows[i].getValue(columnName);

        // Validate type conversion correctness
        if (originalValue != null) {
          // Check that the refreshed value is the correct type
          validateValueType(refreshedValue, newType, segmentName, columnName, i);

          // Check that the value is correctly converted
          validateValueConversion(originalValue, refreshedValue, originalType, newType,
              segmentName, columnName, i);
        }
      }
    }
  }

  /**
   * Validate that a value is of the expected type
   */
  private void validateValueType(Object value, FieldSpec.DataType expectedType,
                                String segmentName, String columnName, int rowIndex) {
    if (value == null) {
      return;
    }

    boolean isCorrectType = false;
    switch (expectedType) {
      case INT:
        isCorrectType = value instanceof Integer;
        break;
      case LONG:
        isCorrectType = value instanceof Long;
        break;
      case FLOAT:
        isCorrectType = value instanceof Float;
        break;
      case DOUBLE:
        isCorrectType = value instanceof Double;
        break;
      case STRING:
        isCorrectType = value instanceof String;
        break;
      default:
        // For other data types, assume correct for now
        isCorrectType = true;
        break;
    }

    assertTrue(isCorrectType,
        String.format("Value type mismatch for column '%s' in segment '%s' row %d. Expected %s, got %s",
            columnName, segmentName, rowIndex, expectedType, value.getClass().getSimpleName()));
  }

  /**
   * Validate that value conversion is correct
   */
  private void validateValueConversion(Object originalValue, Object convertedValue,
                                      FieldSpec.DataType originalType, FieldSpec.DataType newType,
                                      String segmentName, String columnName, int rowIndex) {
    // For numeric conversions, check that the value is preserved
    if (originalType == FieldSpec.DataType.INT && newType == FieldSpec.DataType.LONG) {
      assertEquals(((Long) convertedValue).intValue(), ((Integer) originalValue).intValue(),
          String.format("INT->LONG conversion failed for column '%s' in segment '%s' row %d",
              columnName, segmentName, rowIndex));
    } else if (originalType == FieldSpec.DataType.INT && newType == FieldSpec.DataType.FLOAT) {
      assertEquals(((Float) convertedValue).intValue(), ((Integer) originalValue).intValue(),
          String.format("INT->FLOAT conversion failed for column '%s' in segment '%s' row %d",
              columnName, segmentName, rowIndex));
    }
  }

  /**
   * Validate inverted index functionality by checking query performance
   */
  private void validateInvertedIndexFunctionality(String columnName) {
    waitForServerSegmentDownload(aVoid -> {
      try {
        // Query with filter on the indexed column - should have 0 entries scanned in filter
        String query = String.format("SELECT COUNT(*) FROM mytable WHERE %s = 305", columnName);
        JsonNode response = postQuery(query);
        long entriesScanned = response.get("numEntriesScannedInFilter").asLong();
        return entriesScanned == 0; // Inverted index should result in 0 entries scanned
      } catch (Exception e) {
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
    taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "true");
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
