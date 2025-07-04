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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class RefreshSegmentMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
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
    tableConfig.setTaskConfig(getRefreshSegmentTaskConfig());
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    // Create segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDataDir,
        _segmentTarDir);
    uploadSegments(getTableName(), _segmentTarDir);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test(priority = 1)
  public void testFirstSegmentRefresh() {
    // This will create the inverted index as we disable inverted index creation during segment push.
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    waitForTaskToComplete();

    // Check that metadata contains expected values
    Map<String, String> segmentRefreshTime = new HashMap<>();
    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Get the value in segment metadata
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey));
      segmentRefreshTime.put(metadata.getSegmentName(), customMap.get(refreshKey));
    }

    // This should be no-op as nothing changes.
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Get the value in segment metadata
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(
          customMap.containsKey(MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX));
      assertEquals(segmentRefreshTime.get(metadata.getSegmentName()), customMap.get(refreshKey),
          "Refresh Time doesn't match");
    }
  }

  @Test(priority = 2)
  public void testValidDatatypeChange() throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Change datatype from INT -> LONG for airlineId
    Schema schema = createSchema();
    schema.getFieldSpecFor("ArrTime").setDataType(FieldSpec.DataType.LONG);
    schema.getFieldSpecFor("AirlineID").setDataType(FieldSpec.DataType.STRING);
    schema.getFieldSpecFor("ActualElapsedTime").setDataType(FieldSpec.DataType.FLOAT);
    schema.getFieldSpecFor("DestAirportID").setDataType(FieldSpec.DataType.STRING);
    forceUpdateSchema(schema);

    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    waitForTaskToComplete();

    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT ArrTime FROM mytable LIMIT 10";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("LONG");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT AirlineID FROM mytable LIMIT 10";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("STRING");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT ActualElapsedTime FROM mytable LIMIT 10";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("FLOAT");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT DestAirportID FROM mytable LIMIT 10";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("STRING");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Reset the schema back to it's original state.
    schema = createSchema();
    schema.getFieldSpecFor("ArrTime").setDataType(FieldSpec.DataType.INT);
    schema.getFieldSpecFor("AirlineID").setDataType(FieldSpec.DataType.LONG);
    schema.getFieldSpecFor("ActualElapsedTime").setDataType(FieldSpec.DataType.INT);
    schema.getFieldSpecFor("DestAirportID").setDataType(FieldSpec.DataType.INT);
    forceUpdateSchema(schema);
  }

  @Test(priority = 3)
  public void testIndexChanges() throws Exception {
    /**
     * Adding bare-minimum tests for addition and removal of indexes. The segment generation code already
     * has enough tests and testing each index addition/removal does not seem necessary.
     */

    // Current inverted index columns are "FlightNum", "Origin", "Quarter"
    String query = "SELECT * FROM mytable WHERE flightNum = 3151 LIMIT 10";
    assertEquals(postQuery(query).get("numEntriesScannedInFilter").asLong(), 0L);
    query = "SELECT * from mytable where Origin = 'SFO' LIMIT 10";
    assertEquals(postQuery(query).get("numEntriesScannedInFilter").asLong(), 0L);
    query = "SELECT * from mytable where Quarter = 1 LIMIT 10";
    assertEquals(postQuery(query).get("numEntriesScannedInFilter").asLong(), 0L);

    TableConfig tableConfig = getOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    // Add inverted index for DivActualElapsedTime
    // Remove inverted index for "FlightNum"
    indexingConfig.setInvertedIndexColumns(Arrays.asList("DivActualElapsedTime", "Origin", "Quarter"));
    updateTableConfig(tableConfig);

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    waitForTaskToComplete();

    waitForServerSegmentDownload(aVoid -> {
      try {
        String newQuery = "SELECT * FROM mytable where flightNum = 3151 LIMIT 10";
        return postQuery(newQuery).get("numEntriesScannedInFilter").asLong() > 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    waitForServerSegmentDownload(aVoid -> {
      try {
        String newQuery = "SELECT * FROM mytable where DivActualElapsedTime = 305 LIMIT 10";
        return postQuery(newQuery).get("numEntriesScannedInFilter").asLong() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test(priority = 4)
  public void checkColumnAddition() throws Exception {
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec("NewAddedDerivedDivAirportSeqIDs", FieldSpec.DataType.INT, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedDivAirportSeqIDsString", FieldSpec.DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("NewAddedRawDerivedStringDimension", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("NewAddedRawDerivedMVIntDimension", FieldSpec.DataType.INT, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedNullString", FieldSpec.DataType.STRING, true, "nil"));
    schema.setEnableColumnBasedNullHandling(true);
    updateSchema(schema);

    TableConfig tableConfig = getOfflineTableConfig();
    List<TransformConfig> transformConfigs =
        List.of(new TransformConfig("NewAddedDerivedDivAirportSeqIDs", "DivAirportSeqIDs"),
            new TransformConfig("NewAddedDerivedDivAirportSeqIDsString", "DivAirportSeqIDs"),
            new TransformConfig("NewAddedRawDerivedStringDimension", "reverse(DestCityName)"),
            new TransformConfig("NewAddedRawDerivedMVIntDimension", "ActualElapsedTime"),
            new TransformConfig("NewAddedDerivedNullString", "caseWhen(true, null, null)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    noDictionaryColumns.add("NewAddedRawDerivedStringDimension");
    noDictionaryColumns.add("NewAddedRawDerivedMVIntDimension");
    updateTableConfig(tableConfig);

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext().setTablesToSchedule(Set.of(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext().setTablesToSchedule(Set.of(offlineTableName))
        .setTasksToSchedule(Set.of(MinionConstants.RefreshSegmentTask.TASK_TYPE)), _taskManager);
    waitForTaskToComplete();

    // Check that metadata contains processed times.
    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Get the value in segment metadata
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey));
    }

    String referenceQuery = "SELECT COUNT(*) FROM mytable WHERE ActualElapsedTime > 150";
    int referenceCount = postQuery(referenceQuery).get("resultTable").get("rows").get(0).get(0).asInt();
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM mytable WHERE NewAddedRawDerivedMVIntDimension > 150";
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("rows").get(0).get(0).asLong() == referenceCount;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Verify the index sizes
    List<String> columns =
        List.of("DivAirportSeqIDs", "NewAddedDerivedDivAirportSeqIDs", "NewAddedDerivedDivAirportSeqIDsString",
            "NewAddedRawDerivedStringDimension", "NewAddedRawDerivedMVIntDimension", "NewAddedDerivedNullString");
    JsonNode columnIndexSizeMap = JsonUtils.stringToJsonNode(
            sendGetRequest(_controllerRequestURLBuilder.forTableAggregateMetadata(getTableName(), columns)))
        .get("columnIndexSizeMap");
    assertEquals(columnIndexSizeMap.size(), 6);
    JsonNode originalColumnIndexSizes = columnIndexSizeMap.get("DivAirportSeqIDs");
    JsonNode derivedColumnIndexSizes = columnIndexSizeMap.get("NewAddedDerivedDivAirportSeqIDs");
    JsonNode derivedStringColumnIndexSizes = columnIndexSizeMap.get("NewAddedDerivedDivAirportSeqIDsString");
    JsonNode derivedRawStringColumnIndex = columnIndexSizeMap.get("NewAddedRawDerivedStringDimension");
    JsonNode derivedRawMVIntColumnIndex = columnIndexSizeMap.get("NewAddedRawDerivedMVIntDimension");
    JsonNode derivedNullStringColumnIndex = columnIndexSizeMap.get("NewAddedDerivedNullString");

    // Derived int column should have the same dictionary size as the original column
    double originalColumnDictionarySize = originalColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble();
    assertEquals(derivedColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble(), originalColumnDictionarySize);

    // Derived string column should have larger dictionary size than the original column
    assertTrue(
        derivedStringColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble() > originalColumnDictionarySize);

    // Both derived columns should have same forward index size
    double derivedColumnForwardIndexSize = derivedColumnIndexSizes.get(StandardIndexes.FORWARD_ID).asDouble();
    assertEquals(derivedStringColumnIndexSizes.get(StandardIndexes.FORWARD_ID).asDouble(),
        derivedColumnForwardIndexSize);

    assertTrue(derivedRawStringColumnIndex.has(StandardIndexes.FORWARD_ID));
    assertFalse(derivedRawStringColumnIndex.has(StandardIndexes.DICTIONARY_ID));

    assertTrue(derivedRawMVIntColumnIndex.has(StandardIndexes.FORWARD_ID));
    assertFalse(derivedRawMVIntColumnIndex.has(StandardIndexes.DICTIONARY_ID));

    assertTrue(derivedNullStringColumnIndex.has(StandardIndexes.NULL_VALUE_VECTOR_ID));
  }

  @Test(priority = 5)
  public void checkRefreshNotNecessary() throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    Map<String, Long> segmentCrc = new HashMap<>();
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      segmentCrc.put(metadata.getSegmentName(), metadata.getCrc());
    }

    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQuotaConfig(new QuotaConfig(null, "10"));

    updateTableConfig(tableConfig);

    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    waitForTaskToComplete();

    // Check that metadata contains expected values
    Map<String, String> segmentRefreshTime = new HashMap<>();

    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Get the value in segment metadata
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey));
      segmentRefreshTime.put(metadata.getSegmentName(), customMap.get(refreshKey));
      assertEquals(segmentCrc.get(metadata.getSegmentName()), metadata.getCrc(), "CRC does not match");
    }

    // This should be no-op as nothing changes.
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        _taskManager);
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Get the value in segment metadata
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(
          customMap.containsKey(MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX));
      assertEquals(segmentRefreshTime.get(metadata.getSegmentName()), customMap.get(refreshKey),
          "Refresh Time doesn't match");
    }
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

  private TableTaskConfig getRefreshSegmentTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, tableTaskConfigs));
  }
}
