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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.integration.tests.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for RefreshSegmentTask minion task.
 * Tests segment refresh after index configuration changes.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class RefreshSegmentMinionTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "RefreshSegmentMinionTest";
  private static final String INT_COL = "intCol";
  private static final String LONG_COL = "longCol";
  private static final String STRING_COL = "stringCol";
  private static final String FLIGHT_NUM_COL = "flightNum";
  private static final int NUM_ROWS = 500;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_ROWS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(FLIGHT_NUM_COL, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(INT_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(LONG_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(STRING_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(FLIGHT_NUM_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COL, i);
        record.put(LONG_COL, (long) i * 100);
        record.put(STRING_COL, "value_" + (i % 50));
        record.put(FLIGHT_NUM_COL, 3000 + (i % 200));
        writers.get(i % writers.size()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(FLIGHT_NUM_COL, STRING_COL))
        .build();
    tableConfig.setTaskConfig(getRefreshSegmentTaskConfig());
    return tableConfig;
  }

  // Uses base class setUp() — default avro-based table creation works here

  @Test(priority = 1)
  public void testFirstSegmentRefresh() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    assertTrue(getHelixTaskResourceManager().getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RefreshSegmentTask.TASK_TYPE)));
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    Map<String, String> segmentRefreshTime = new HashMap<>();
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey));
      segmentRefreshTime.put(metadata.getSegmentName(), customMap.get(refreshKey));
    }

    // No-op: nothing changed, should not schedule
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      Map<String, String> customMap = metadata.getCustomMap();
      assertTrue(customMap.containsKey(refreshKey));
      assertEquals(segmentRefreshTime.get(metadata.getSegmentName()), customMap.get(refreshKey));
    }
  }

  @Test(priority = 2)
  public void testValidDatatypeChange()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Change datatype: intCol INT -> LONG, flightNum INT -> STRING
    Schema schema = createSchema();
    schema.getFieldSpecFor(INT_COL).setDataType(FieldSpec.DataType.LONG);
    schema.getFieldSpecFor(FLIGHT_NUM_COL).setDataType(FieldSpec.DataType.STRING);
    forceUpdateSchema(schema);

    assertNotNull(getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    // Verify intCol is now LONG
    waitForServerSegmentDownload(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT " + INT_COL + " FROM " + TABLE_NAME + " LIMIT 10");
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("LONG");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Verify flightNum is now STRING
    waitForServerSegmentDownload(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT " + FLIGHT_NUM_COL + " FROM " + TABLE_NAME + " LIMIT 10");
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("STRING");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Reset schema back to original types
    schema = createSchema();
    forceUpdateSchema(schema);
  }

  @Test(priority = 3)
  public void testIndexChanges()
      throws Exception {
    // Verify inverted index is active on flightNum
    String query = "SELECT * FROM " + TABLE_NAME + " WHERE " + FLIGHT_NUM_COL + " = 3151 LIMIT 10";
    assertEquals(postQuery(query).get("numEntriesScannedInFilter").asLong(), 0L);

    // Remove inverted index for flightNum, add for intCol
    TableConfig tableConfig = getSharedHelixResourceManager().getOfflineTableConfig(TABLE_NAME);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setInvertedIndexColumns(Arrays.asList(INT_COL, STRING_COL));
    updateTableConfig(tableConfig);

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    // flightNum should now scan filter (no inverted index)
    waitForServerSegmentDownload(aVoid -> {
      try {
        String newQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + FLIGHT_NUM_COL + " = 3151 LIMIT 10";
        return postQuery(newQuery).get("numEntriesScannedInFilter").asLong() > 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // intCol should now use inverted index (0 scan in filter)
    waitForServerSegmentDownload(aVoid -> {
      try {
        String newQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + INT_COL + " = 42 LIMIT 10";
        return postQuery(newQuery).get("numEntriesScannedInFilter").asLong() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test(priority = 4)
  public void checkColumnAddition()
      throws Exception {
    // Add a new derived column to the schema
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec("derivedLongCol", FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec("derivedStringCol", FieldSpec.DataType.STRING, true));
    updateSchema(schema);

    // Add transform configs for the new columns
    TableConfig tableConfig = getSharedHelixResourceManager().getOfflineTableConfig(TABLE_NAME);
    List<TransformConfig> transformConfigs = List.of(
        new TransformConfig("derivedLongCol", LONG_COL),
        new TransformConfig("derivedStringCol", STRING_COL));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);
    updateTableConfig(tableConfig);

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(
        getTaskManager().scheduleTasks(new TaskSchedulingContext().setTablesToSchedule(Set.of(offlineTableName)))
            .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    MinionTaskTestUtils.assertNoTaskSchedule(
        new TaskSchedulingContext().setTablesToSchedule(Set.of(offlineTableName))
            .setTasksToSchedule(Set.of(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    // Verify refresh metadata is set
    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      assertTrue(metadata.getCustomMap().containsKey(refreshKey));
    }

    // Verify derived column is queryable and has correct values
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE derivedLongCol > 15000";
        JsonNode response = postQuery(query);
        // longCol = i*100, so derivedLongCol = longCol. Values > 15000 means i > 150.
        // With 500 rows (0..499), that's 349 rows.
        return response.get("resultTable").get("rows").get(0).get(0).asLong() == 349;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test(priority = 5)
  public void testRefreshNotNecessary() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    Map<String, Long> segmentCrc = new HashMap<>();
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      segmentCrc.put(metadata.getSegmentName(), metadata.getCrc());
    }

    // Schedule refresh — should result in no actual changes since nothing changed
    assertNotNull(getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    // CRCs should not change since no refresh was needed
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      assertEquals(segmentCrc.get(metadata.getSegmentName()), metadata.getCrc(), "CRC should not change");
    }
  }

  @Test(priority = 6)
  public void testMetadataPushRefresh()
      throws Exception {
    // Update table config to use METADATA push mode and clear any leftover ingestion config
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TableConfig tableConfig = getSharedHelixResourceManager().getOfflineTableConfig(TABLE_NAME);
    tableConfig.setTaskConfig(getRefreshSegmentMetadataPushTaskConfig());
    tableConfig.setIngestionConfig(null);
    updateTableConfig(tableConfig);

    // Make a schema change to trigger a refresh (reset to base schema first, then modify)
    Schema schema = createSchema();
    forceUpdateSchema(schema);
    schema.getFieldSpecFor(LONG_COL).setDataType(FieldSpec.DataType.FLOAT);
    forceUpdateSchema(schema);

    assertNotNull(getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));
    MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName))
            .setTasksToSchedule(Collections.singleton(MinionConstants.RefreshSegmentTask.TASK_TYPE)),
        getTaskManager());
    waitForTaskToComplete();

    // Verify refresh metadata is set
    String refreshKey = MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(offlineTableName)) {
      assertTrue(metadata.getCustomMap().containsKey(refreshKey));
    }

    // Verify the datatype change took effect
    waitForServerSegmentDownload(aVoid -> {
      try {
        JsonNode response = postQuery("SELECT " + LONG_COL + " FROM " + TABLE_NAME + " LIMIT 10");
        return response.get("resultTable").get("dataSchema").get("columnDataTypes").toString().contains("FLOAT");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Reset schema and task config back to original
    schema = createSchema();
    forceUpdateSchema(schema);
    tableConfig = getSharedHelixResourceManager().getOfflineTableConfig(TABLE_NAME);
    tableConfig.setTaskConfig(getRefreshSegmentTaskConfig());
    updateTableConfig(tableConfig);
  }

  private void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      for (TaskState taskState : getHelixTaskResourceManager()
          .getTaskStates(MinionConstants.RefreshSegmentTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }

  private void waitForServerSegmentDownload(Function<Void, Boolean> conditionFunc) {
    TestUtils.waitForCondition(aVoid -> conditionFunc.apply(aVoid), 60_000L, "Failed to meet condition");
  }

  private TableTaskConfig getRefreshSegmentTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, tableTaskConfigs));
  }

  private TableTaskConfig getRefreshSegmentMetadataPushTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    tableTaskConfigs.put(MinionTaskUtils.ALLOW_METADATA_PUSH_WITH_LOCAL_FS, "true");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, tableTaskConfigs));
  }
}
