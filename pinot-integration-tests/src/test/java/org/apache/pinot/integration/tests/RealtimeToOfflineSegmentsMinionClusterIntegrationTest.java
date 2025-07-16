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
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
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
  private static final String UPSERT_INPUT_DATA_TAR_FILE = "gameScores_upsert_realtimeToOffline_csv.tar.gz";
  private static final String UPSERT_CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String UPSERT_CSV_DELIMITER = ",";
  private static final String UPSERT_TABLE_NAME = "myUpsertTable";
  private static final String UPSERT_PRIMARY_KEY_COL = "playerId";
  private static final String UPSERT_TIME_COL_NAME = "timestampInEpoch";
  private static final String UPSERT_SCHEMA_FILE_NAME = "upsert_table_test.schema";
  private static final String DELETE_COL = "deleted";

  private PinotHelixTaskResourceManager _taskResourceManager;
  private PinotTaskManager _taskManager;
  private String _realtimeTableName;
  private String _offlineTableName;

  private String _realtimeMetadataTableName;
  private String _offlineMetadataTableName;
  private String _realtimeUpsertTableName;
  private String _offlineUpsertTableName;
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
    taskConfigsWithMetadata.put(BatchConfigProperties.PUSH_MODE,
        BatchConfigProperties.SegmentPushType.METADATA.toString());
    String tableWithMetadataPush = "myTable2";
    schema.setSchemaName(tableWithMetadataPush);
    addSchema(schema);
    TableConfig realtimeMetadataTableConfig = createRealtimeTableConfig(avroFiles.get(0), tableWithMetadataPush,
        new TableTaskConfig(Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE,
            taskConfigsWithMetadata)));
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

    // Create upsert table for testing upsert support using gameScores data
    setUpUpsertTable();

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
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn()).setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns()).setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns()).setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode())
        .setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(partitionConfig).build();
  }

  private TableConfig createOfflineUpsertTableConfig() {
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(UPSERT_PRIMARY_KEY_COL, new ColumnPartitionConfig("Murmur", getNumKafkaPartitions()));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(UPSERT_TABLE_NAME)
        .setTimeColumnName(UPSERT_TIME_COL_NAME)
        .setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap)).build();
  }

  protected TableConfig createRealtimeTableConfig(File sampleAvroFile, String tableName, TableTaskConfig taskConfig) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn()).setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns()).setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns()).setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode())
        .setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig()).setQueryConfig(getQueryConfig()).setStreamConfigs(getStreamConfigs())
        .setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  private TableConfig createRealtimeUpsertTableConfig(String kafkaTopicName) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    upsertConfig.setSnapshot(Enablement.ENABLE);

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(BatchConfigProperties.OVERWRITE_OUTPUT, "true");
    // Use yearly buckets since gameScores data spans multiple years (2019-2024)
    taskConfigs.put(MinionConstants.RealtimeToOfflineSegmentsTask.BUCKET_TIME_PERIOD_KEY, "365d");

    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(UPSERT_PRIMARY_KEY_COL, new ColumnPartitionConfig("Murmur", getNumKafkaPartitions()));

    Map<String, String> streamConfigsMap = getStreamConfigMap();
    // Set flush threshold to 500 to create multiple completed segments
    // Actual flush threshold is going to be 500 per segment (1000 / partitionsPerInstance)
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "1000");
    streamConfigsMap.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
        kafkaTopicName);
    streamConfigsMap.putAll(getCSVDecoderProperties(UPSERT_CSV_DELIMITER, UPSERT_CSV_SCHEMA_HEADER));
    return new TableConfigBuilder(TableType.REALTIME).setTableName(UPSERT_TABLE_NAME)
        .setTimeColumnName(UPSERT_TIME_COL_NAME)
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(new TableTaskConfig(Collections.singletonMap(
            MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs))).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setStreamConfigs(streamConfigsMap)
        .setNullHandlingEnabled(UpsertConfig.Mode.PARTIAL.equals(upsertConfig.getMode()) || getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(UPSERT_PRIMARY_KEY_COL, 1))
        .setUpsertConfig(upsertConfig).build();
  }

  private void setUpUpsertTable()
      throws Exception {
    // Create upsert schema from schema file
    Schema upsertSchema = createSchema(UPSERT_SCHEMA_FILE_NAME);
    upsertSchema.setSchemaName(UPSERT_TABLE_NAME);
    addSchema(upsertSchema);

    // Set up gameScores CSV data for upsert table
    String upsertKafkaTopicName = getKafkaTopic() + "-upsert";
    createKafkaTopic(upsertKafkaTopicName);
    List<File> gameScoresFiles = unpackTarData(UPSERT_INPUT_DATA_TAR_FILE, _tempDir);
    pushCsvIntoKafka(gameScoresFiles.get(0), upsertKafkaTopicName, 0);

    TableConfig realtimeUpsertTableConfig = createRealtimeUpsertTableConfig(upsertKafkaTopicName);
    addTableConfig(realtimeUpsertTableConfig);

    TableConfig offlineUpsertTableConfig = createOfflineUpsertTableConfig();
    addTableConfig(offlineUpsertTableConfig);

    _realtimeUpsertTableName = TableNameBuilder.REALTIME.tableNameWithType(UPSERT_TABLE_NAME);
    _offlineUpsertTableName = TableNameBuilder.OFFLINE.tableNameWithType(UPSERT_TABLE_NAME);
  }

  @Test
  public void testRealtimeToOfflineSegmentsTask()
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
      assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeTableName)))
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      assertTrue(_taskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeTableName))
              .setTasksToSchedule(Collections.singleton(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)),
          _taskManager);

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

    testHardcodedQueries();
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
      assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeMetadataTableName)))
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      assertTrue(_taskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeMetadataTableName))
              .setTasksToSchedule(Collections.singleton(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)),
          _taskManager);

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

  private long getMinUpsertTimestamp() {
    ResultSetGroup resultSetGroup =
        getPinotConnection().execute("SELECT MIN(" + UPSERT_TIME_COL_NAME + ") FROM " + UPSERT_TABLE_NAME
            + " OPTION(skipUpsert=true)");
    if (resultSetGroup.getResultSetCount() > 0) {
      return (long) resultSetGroup.getResultSet(0).getDouble(0);
    }
    return 0L;
  }

  private long getScore() {
    return (long) getPinotConnection().execute(
            "SELECT score FROM " + UPSERT_TABLE_NAME + " WHERE playerId = 101")
        .getResultSet(0).getFloat(0);
  }

  @Test
  public void testRealtimeToOfflineSegmentsUpsertTask()
      throws Exception {
    // wait for documents to be loaded: 1000 (2019-2023) + 6 (2024)
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResultWithoutUpsert(UPSERT_TABLE_NAME) == 1006,
        600_000L,
        "Failed to load all documents for upsert");
    assertEquals(getCurrentCountStarResult(UPSERT_TABLE_NAME), 5);
    assertEquals(getScore(), 3692);

    // wait for segments to converge first
    waitForNumQueriedSegmentsToConverge(UPSERT_TABLE_NAME, 10_000L, 3, 2);

    // Pause consumption to force segment completion, then resume to trigger snapshot
    sendPostRequest(_controllerRequestURLBuilder.forPauseConsumption(UPSERT_TABLE_NAME));
    waitForNumQueriedSegmentsToConverge(UPSERT_TABLE_NAME, 600_000L, 3, 0);
    sendPostRequest(_controllerRequestURLBuilder.forResumeConsumption(UPSERT_TABLE_NAME));
    waitForNumQueriedSegmentsToConverge(UPSERT_TABLE_NAME, 600_000L, 5, 2);

    // Run a single task iteration for upsert table
    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineUpsertTableName);
    assertTrue(segmentsZKMetadata.isEmpty());

    // Query minimum timestamp directly from the data
    long minDataTimeMs = getMinUpsertTimestamp();

    // Round down to bucket boundary (365 days) to match task generator logic
    long yearMs = 365 * 24 * 3600 * 1000L; // 365 days
    long windowStartMs = (minDataTimeMs / yearMs) * yearMs;
    long expectedWatermark = windowStartMs + yearMs;
    _taskManager.cleanUpTask();

    long totalOfflineDocuments = 0;
    for (int i = 0; i < 5; i++) {
      // Schedule task for upsert table
      assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeUpsertTableName)))
          .get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      assertTrue(_taskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));

      // Should not generate more tasks
      MinionTaskTestUtils.assertNoTaskSchedule(new TaskSchedulingContext()
              .setTablesToSchedule(Collections.singleton(_realtimeUpsertTableName))
              .setTasksToSchedule(Collections.singleton(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)),
          _taskManager);

      // Wait at most 600 seconds for all tasks COMPLETED
      waitForTaskToComplete(expectedWatermark, _realtimeUpsertTableName);

      // Count documents from the offline segments created for upsert table
      segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(_offlineUpsertTableName);
      long expectedOfflineWindowStartMs = expectedWatermark - yearMs;
      for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
        assertTrue(segmentZKMetadata.getStartTimeMs() >= expectedOfflineWindowStartMs);
        assertTrue(segmentZKMetadata.getStartTimeMs() < expectedWatermark);
        assertTrue(segmentZKMetadata.getEndTimeMs() >= expectedOfflineWindowStartMs);
        assertTrue(segmentZKMetadata.getEndTimeMs() < expectedWatermark);
        totalOfflineDocuments += segmentZKMetadata.getTotalDocs();
      }
      expectedWatermark += yearMs;
    }

    // offline segments should contain 3 records (distinct primary keys)
    // minus 2 primary keys from 2024 (last window cannot be processed as it could overlap with CONSUMING segments)
    assertEquals(totalOfflineDocuments, 3);
    // Number of segments must be increased by 2: one per partition in the offline table
    waitForNumQueriedSegmentsToConverge(UPSERT_TABLE_NAME, 600_000L, 7, 2);
    // 3 + 6 records from the realtime table (2024)
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResultWithoutUpsert(UPSERT_TABLE_NAME) == 9,
        600_000L,
        "Failed to load all documents for upsert");
    assertEquals(getCurrentCountStarResult(UPSERT_TABLE_NAME),
        5); // 3 + 2 records (primary keys only) from the realtime table (2024)
    assertEquals(getScore(), 3692);
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

    // Clean up upsert tables
    dropRealtimeTable(_realtimeUpsertTableName);
    dropOfflineTable(_offlineUpsertTableName);

    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
