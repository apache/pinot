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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for minion task of type "RealtimeToOfflineSegmentsTask"
 * With every task run, a new segment is created in the offline table for 1 day. Watermark also keeps progressing
 * accordingly.
 */
public class RealtimeToOfflineSegmentsMinionClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private PinotHelixTaskResourceManager _taskResourceManager;
  private PinotTaskManager _taskManager;
  private String _realtimeTableName;
  private String _offlineTableName;

  private String _realtimeMetadataTableName;
  private String _offlineMetadataTableName;
  private long _dataSmallestTimeMs;
  private long _dataSmallestMetadataTableTimeMs;

  @Override
  protected SegmentPartitionConfig getSegmentPartitionConfig() {
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    ColumnPartitionConfig columnOneConfig = new ColumnPartitionConfig("murmur", 3);
    columnPartitionConfigMap.put("AirlineID", columnOneConfig);
    ColumnPartitionConfig columnTwoConfig = new ColumnPartitionConfig("hashcode", 2);
    columnPartitionConfigMap.put("OriginAirportID", columnTwoConfig);
    return new SegmentPartitionConfig(columnPartitionConfigMap);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table configs with a TIMESTAMP field
    Schema schema = createSchema();
    schema.addField(new DateTimeFieldSpec("ts", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS"));
    addSchema(schema);

    TableConfig realtimeTableConfig = createRealtimeTableConfig(avroFiles.get(0));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("ts", "fromEpochDays(DaysSinceEpoch)")));
    realtimeTableConfig.setIngestionConfig(ingestionConfig);
    FieldConfig tsFieldConfig =
        new FieldConfig("ts", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TIMESTAMP, null, null,
            new TimestampConfig(Arrays.asList(TimestampIndexGranularity.HOUR, TimestampIndexGranularity.DAY,
                TimestampIndexGranularity.WEEK, TimestampIndexGranularity.MONTH)), null);
    realtimeTableConfig.setFieldConfigList(Collections.singletonList(tsFieldConfig));

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(BatchConfigProperties.OVERWRITE_OUTPUT, "true");
    realtimeTableConfig.setTaskConfig(new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs)));
    addTableConfig(realtimeTableConfig);

    TableConfig offlineTableConfig = createOfflineTableConfig();
    offlineTableConfig.setFieldConfigList(Collections.singletonList(tsFieldConfig));
    addTableConfig(offlineTableConfig);

    Map<String, String> taskConfigsWithMetadata = new HashMap<>();
    taskConfigsWithMetadata.put(BatchConfigProperties.OVERWRITE_OUTPUT, "true");
    taskConfigsWithMetadata.put(
        BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.toString());
    String tableWithMetadataPush = "myTable2";
    TableConfig realtimeMetadataTableConfig = createRealtimeTableConfig(avroFiles.get(0), tableWithMetadataPush,
        new TableTaskConfig(Collections.singletonMap(
            MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigsWithMetadata)));
    realtimeMetadataTableConfig.setIngestionConfig(ingestionConfig);
    realtimeMetadataTableConfig.setFieldConfigList(Collections.singletonList(tsFieldConfig));
    addTableConfig(realtimeMetadataTableConfig);

    TableConfig offlineMetadataTableConfig =
        createOfflineTableConfig(tableWithMetadataPush, null, getSegmentPartitionConfig());
    offlineMetadataTableConfig.setFieldConfigList(Collections.singletonList(tsFieldConfig));
    addTableConfig(offlineMetadataTableConfig);


    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    waitForDocsLoaded(600_000L, true, tableWithMetadataPush);


    _taskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    _offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    _realtimeMetadataTableName = TableNameBuilder.REALTIME.tableNameWithType(tableWithMetadataPush);
    _offlineMetadataTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableWithMetadataPush);

    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_realtimeTableName);
    long minSegmentTimeMs = Long.MAX_VALUE;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
        minSegmentTimeMs = Math.min(minSegmentTimeMs, segmentZKMetadata.getStartTimeMs());
      }
    }
    _dataSmallestTimeMs = minSegmentTimeMs;

   segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_realtimeMetadataTableName);
   minSegmentTimeMs = Long.MAX_VALUE;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
        minSegmentTimeMs = Math.min(minSegmentTimeMs, segmentZKMetadata.getStartTimeMs());
      }
    }
    _dataSmallestMetadataTableTimeMs = minSegmentTimeMs;
  }

  private TableConfig createOfflineTableConfig(String tableName, @Nullable TableTaskConfig taskConfig,
      @Nullable SegmentPartitionConfig partitionConfig) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled()).setSegmentPartitionConfig(partitionConfig).build();
  }

  protected TableConfig createRealtimeTableConfig(File sampleAvroFile, String tableName, TableTaskConfig taskConfig) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setQueryConfig(getQueryConfig())
        .setLLC(useLlc()).setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  // TODO: enable this test to `useBothQueryEngines` after fixing the issue.
  @Test(dataProvider = "useV1QueryEngine")
  public void testRealtimeToOfflineSegmentsTask(boolean useMultiStageQueryEngine)
      throws Exception {
    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineTableName);
    assertTrue(segmentsZKMetadata.isEmpty());

    // The number of offline segments would be equal to the product of number of partitions for all the
    // partition columns if segment partitioning is configured.
    SegmentPartitionConfig segmentPartitionConfig =
        getOfflineTableConfig().getIndexingConfig().getSegmentPartitionConfig();
    int numOfflineSegmentsPerTask =
        segmentPartitionConfig != null ? segmentPartitionConfig.getColumnPartitionMap().values().stream()
            .map(ColumnPartitionConfig::getNumPartitions).reduce((a, b) -> a * b)
            .orElseThrow(() -> new RuntimeException("Expected accumulated result but not found.")) : 1;

    long expectedWatermark = _dataSmallestTimeMs + 86400000;
    for (int i = 0; i < 3; i++) {
      // Schedule task
      assertNotNull(_taskManager.scheduleTasks(_realtimeTableName)
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      assertTrue(_taskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      assertNull(_taskManager.scheduleTasks(_realtimeTableName)
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));

      // Wait at most 600 seconds for all tasks COMPLETED
      waitForTaskToComplete(expectedWatermark, _realtimeTableName);
      // check segment is in offline
      segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineTableName);
      assertEquals(segmentsZKMetadata.size(), (numOfflineSegmentsPerTask * (i + 1)));

      long expectedOfflineSegmentTimeMs = expectedWatermark - 86400000;
      for (int j = (numOfflineSegmentsPerTask * i); j < segmentsZKMetadata.size(); j++) {
        SegmentZKMetadata segmentZKMetadata = segmentsZKMetadata.get(j);
        assertEquals(segmentZKMetadata.getStartTimeMs(), expectedOfflineSegmentTimeMs);
        assertEquals(segmentZKMetadata.getEndTimeMs(), expectedOfflineSegmentTimeMs);
        if (segmentPartitionConfig != null) {
          assertEquals(segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().keySet(),
              segmentPartitionConfig.getColumnPartitionMap().keySet());
          for (String partitionColumn : segmentPartitionConfig.getColumnPartitionMap().keySet()) {
            assertEquals(segmentZKMetadata.getPartitionMetadata().getPartitions(partitionColumn).size(), 1);
          }
        }
      }
      expectedWatermark += 86400000;
    }

    testHardcodedQueries(useMultiStageQueryEngine);
  }

  @Test
  public void testRealtimeToOfflineSegmentsMetadataPushTask()
      throws Exception {
    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineMetadataTableName);
    assertTrue(segmentsZKMetadata.isEmpty());

    // The number of offline segments would be equal to the product of number of partitions for all the
    // partition columns if segment partitioning is configured.
    SegmentPartitionConfig segmentPartitionConfig =
        getOfflineTableConfig().getIndexingConfig().getSegmentPartitionConfig();
    int numOfflineSegmentsPerTask =
        segmentPartitionConfig != null ? segmentPartitionConfig.getColumnPartitionMap().values().stream()
            .map(ColumnPartitionConfig::getNumPartitions).reduce((a, b) -> a * b)
            .orElseThrow(() -> new RuntimeException("Expected accumulated result but not found.")) : 1;

    long expectedWatermark = _dataSmallestMetadataTableTimeMs + 86400000;
    _taskManager.cleanUpTask();
    for (int i = 0; i < 3; i++) {
      // Schedule task
      assertNotNull(_taskManager.scheduleTasks(_realtimeMetadataTableName)
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      assertTrue(_taskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      assertNull(_taskManager.scheduleTasks(_realtimeMetadataTableName)
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));

      // Wait at most 600 seconds for all tasks COMPLETED
      waitForTaskToComplete(expectedWatermark, _realtimeMetadataTableName);
      // check segment is in offline
      segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineMetadataTableName);
      assertEquals(segmentsZKMetadata.size(), (numOfflineSegmentsPerTask * (i + 1)));

      long expectedOfflineSegmentTimeMs = expectedWatermark - 86400000;
      for (int j = (numOfflineSegmentsPerTask * i); j < segmentsZKMetadata.size(); j++) {
        SegmentZKMetadata segmentZKMetadata = segmentsZKMetadata.get(j);
        assertEquals(segmentZKMetadata.getStartTimeMs(), expectedOfflineSegmentTimeMs);
        assertEquals(segmentZKMetadata.getEndTimeMs(), expectedOfflineSegmentTimeMs);
        if (segmentPartitionConfig != null) {
          assertEquals(segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().keySet(),
              segmentPartitionConfig.getColumnPartitionMap().keySet());
          for (String partitionColumn : segmentPartitionConfig.getColumnPartitionMap().keySet()) {
            assertEquals(segmentZKMetadata.getPartitionMetadata().getPartitions(partitionColumn).size(), 1);
          }
        }
      }
      expectedWatermark += 86400000;
    }
  }

  private void waitForTaskToComplete(long expectedWatermark, String realtimeTableName) {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _taskResourceManager.getTaskStates(
          MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");

    // Check segment ZK metadata
    ZNRecord znRecord = _taskManager.getClusterInfoAccessor()
        .getMinionTaskMetadataZNRecord(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, realtimeTableName);
    RealtimeToOfflineSegmentsTaskMetadata minionTaskMetadata =
        znRecord != null ? RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord) : null;
    assertNotNull(minionTaskMetadata);
    assertEquals(minionTaskMetadata.getWatermarkMs(), expectedWatermark);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(_realtimeTableName);
    assertNull(MinionTaskMetadataUtils.fetchTaskMetadata(_propertyStore,
        MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, _realtimeTableName));
    dropOfflineTable(_offlineTableName);

    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
