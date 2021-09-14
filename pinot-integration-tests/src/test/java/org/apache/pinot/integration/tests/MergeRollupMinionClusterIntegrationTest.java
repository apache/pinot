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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.compat.tests.SqlResultComparator;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for minion task of type "MergeRollupTask"
 */
public class MergeRollupMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String SINGLE_LEVEL_CONCAT_TEST_TABLE = "myTable1";
  private static final String SINGLE_LEVEL_ROLLUP_TEST_TABLE = "myTable2";
  private static final String MULTI_LEVEL_CONCAT_TEST_TABLE = "myTable3";

  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;

  protected final File _segmentDir1 = new File(_tempDir, "segmentDir1");
  protected final File _segmentDir2 = new File(_tempDir, "segmentDir2");
  protected final File _segmentDir3 = new File(_tempDir, "segmentDir3");
  protected final File _tarDir1 = new File(_tempDir, "tarDir1");
  protected final File _tarDir2 = new File(_tempDir, "tarDir2");
  protected final File _tarDir3 = new File(_tempDir, "tarDir3");

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir1, _segmentDir2, _segmentDir3, _tarDir1, _tarDir2,
        _tarDir3);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig singleLevelConcatTableConfig =
        createOfflineTableConfig(SINGLE_LEVEL_CONCAT_TEST_TABLE, getSingleLevelConcatTaskConfig());
    TableConfig singleLevelRollupTableConfig =
        createOfflineTableConfig(SINGLE_LEVEL_ROLLUP_TEST_TABLE, getSingleLevelRollupTaskConfig());
    TableConfig multiLevelConcatTableConfig =
        createOfflineTableConfig(MULTI_LEVEL_CONCAT_TEST_TABLE, getMultiLevelConcatTaskConfig());
    addTableConfig(singleLevelConcatTableConfig);
    addTableConfig(singleLevelRollupTableConfig);
    addTableConfig(multiLevelConcatTableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, singleLevelConcatTableConfig, schema, 0, _segmentDir1, _tarDir1);
    buildSegmentsFromAvroWithPostfix(avroFiles, singleLevelRollupTableConfig, schema, 0, _segmentDir2, _tarDir2, "1");
    buildSegmentsFromAvroWithPostfix(avroFiles, singleLevelRollupTableConfig, schema, 0, _segmentDir2, _tarDir2, "2");
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, multiLevelConcatTableConfig, schema, 0, _segmentDir3, _tarDir3);
    uploadSegments(SINGLE_LEVEL_CONCAT_TEST_TABLE, _tarDir1);
    uploadSegments(SINGLE_LEVEL_ROLLUP_TEST_TABLE, _tarDir2);
    uploadSegments(MULTI_LEVEL_CONCAT_TEST_TABLE, _tarDir3);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    startMinion();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
  }

  private TableConfig createOfflineTableConfig(String tableName, TableTaskConfig taskConfig) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  private TableTaskConfig getSingleLevelConcatTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("100days.mergeType", "concat");
    tableTaskConfigs.put("100days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("100days.bucketTimePeriod", "100d");
    tableTaskConfigs.put("100days.maxNumRecordsPerSegment", "15000");
    tableTaskConfigs.put("100days.maxNumRecordsPerTask", "15000");
    tableTaskConfigs.put("ActualElapsedTime.aggregationType", "min");
    tableTaskConfigs.put("WeatherDelay.aggregationType", "sum");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
  }

  private TableTaskConfig getSingleLevelRollupTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("150days.mergeType", "rollup");
    tableTaskConfigs.put("150days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("150days.bucketTimePeriod", "150d");
    tableTaskConfigs.put("150days.roundBucketTimePeriod", "7d");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
  }

  private TableTaskConfig getMultiLevelConcatTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("45days.mergeType", "concat");
    tableTaskConfigs.put("45days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("45days.bucketTimePeriod", "45d");
    tableTaskConfigs.put("45days.maxNumRecordsPerSegment", "100000");
    tableTaskConfigs.put("45days.maxNumRecordsPerTask", "100000");

    tableTaskConfigs.put("90days.mergeType", "concat");
    tableTaskConfigs.put("90days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("90days.bucketTimePeriod", "90d");
    tableTaskConfigs.put("90days.maxNumRecordsPerSegment", "100000");
    tableTaskConfigs.put("90days.maxNumRecordsPerTask", "100000");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
  }

  private static void buildSegmentsFromAvroWithPostfix(List<File> avroFiles, TableConfig tableConfig,
      org.apache.pinot.spi.data.Schema schema, int baseSegmentIndex, File segmentDir, File tarDir, String postfix)
      throws Exception {
    int numAvroFiles = avroFiles.size();
    ExecutorService executorService = Executors.newFixedThreadPool(numAvroFiles);
    List<Future<Void>> futures = new ArrayList<>(numAvroFiles);
    for (int i = 0; i < numAvroFiles; i++) {
      File avroFile = avroFiles.get(i);
      int segmentIndex = i + baseSegmentIndex;
      futures.add(executorService.submit(() -> {
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setInputFilePath(avroFile.getPath());
        segmentGeneratorConfig.setOutDir(segmentDir.getPath());
        segmentGeneratorConfig.setTableName(tableConfig.getTableName());
        // Test segment with space and special character in the file name
        segmentGeneratorConfig.setSegmentNamePostfix(segmentIndex + "_" + postfix);

        // Build the segment
        SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig);
        driver.build();

        // Tar the segment
        String segmentName = driver.getSegmentName();
        File indexDir = new File(segmentDir, segmentName);
        File segmentTarFile = new File(tarDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);
        return null;
      }));
    }
    executorService.shutdown();
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  /**
   * Test single level concat task with maxNumRecordPerTask, maxNumRecordPerSegment constraints
   */
  @Test
  public void testSingleLevelConcat()
      throws Exception {
    // The original segments are time partitioned by month:
    // segmentName (totalDocs)
    // myTable1_16071_16101_3 (9746)
    // myTable1_16102_16129_4 (8690)
    // myTable1_16130_16159_5 (9621)
    // myTable1_16160_16189_6 (9454)
    // myTable1_16190_16220_7 (10329)
    // myTable1_16221_16250_8 (10468)
    // myTable1_16251_16281_9 (10499)
    // myTable1_16282_16312_10 (10196)
    // myTable1_16313_16342_11 (9136)
    // myTable1_16343_16373_0 (9292)
    // myTable1_16374_16404_1 (8736)
    // myTable1_16405_16435_2 (9378)

    // Expected merge tasks and result segments:
    // 1.
    //    {myTable1_16071_16101_3}
    //      -> {merged_100days_T1_0_myTable1_16071_16099_0, merged_100days_T1_0_myTable1_16100_16101_1}
    // 2.
    //    {merged_100days_T1_0_myTable1_16100_16101_1, myTable1_16102_16129_4, myTable1_16130_16159_5}
    //      -> {merged_100days_T2_0_myTable1_16100_???_0(15000), merged_100days_T2_0_myTable1_???_16159_1}
    //    {myTable1_16160_16189_6, myTable1_16190_16220_7}
    //      -> {merged_100days_T2_1_myTable1_16160_16199_0, merged_100days_T2_1_myTable1_16200_16220_1}
    // 3.
    //    {merged_100days_T2_1_myTable1_16200_16220_1, myTable1_16221_16250_8}
    //      -> {merged_100days_T3_0_myTable1_16200_???_0(15000), merged_100days_T3_0_myTable1_???_16250_1}
    //    {myTable1_16251_16281_9, myTable1_16282_16312_10}
    //      -> {merged_100days_T3_1_myTable1_16251_???_0(15000), merged_100days_T3_1_myTable1_???_16299_1,
    //      merged_100days_T3_1_myTable1_16300_16312_2}
    // 4.
    //    {merged_100days_T3_1_myTable1_16300_16312_2, myTable1_16313_16342_11, myTable1_16343_16373_0}
    //      -> {merged_100days_T4_0_myTable1_16300_???_0(15000), merged_100days_T4_0_myTable1_???_16373_1}
    //    {myTable1_16374_16404_1}
    //      -> {merged_100days_T4_1_16374_16399_0, merged_100days_T4_1_16400_16404_1}
    // 5.
    //    {merged_100days_T4_1_16400_16404_1, myTable1_16405_16435_2}
    //      -> {merged_100days_T5_0_myTable1_16400_16435_0}

    String sqlQuery = "SELECT count(*) FROM mytable1"; // 115545 rows for the test table
    JsonNode expectedJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    int[] expectedNumSubTasks = {1, 2, 2, 2, 1};
    int[] expectedNumSegmentsQueried = {13, 12, 13, 13, 12};
    long expectedWatermark = 16000 * 86_400_000L;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(SINGLE_LEVEL_CONCAT_TEST_TABLE);
    int numTasks = 0;
    for (String tasks = _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE);
        tasks != null; tasks =
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE), numTasks++) {
      Assert.assertEquals(_helixTaskResourceManager.getTaskConfigs(tasks).size(), expectedNumSubTasks[numTasks]);
      Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
          .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.MergeRollupTask.TASK_TYPE)));
      // Will not schedule task if there's incomplete task
      Assert.assertNull(
          _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      waitForTaskToComplete();

      // Check watermark
      MergeRollupTaskMetadata minionTaskMetadata = MergeRollupTaskMetadata
          .fromZNRecord(_taskManager.getClusterInfoAccessor().getMinionMergeRollupTaskZNRecord(offlineTableName));
      assertNotNull(minionTaskMetadata);
      Assert.assertEquals((long) minionTaskMetadata.getWatermarkMap().get("100days"), expectedWatermark);
      expectedWatermark += 100 * 86_400_000L;

      // Check metadata of merged segments
      for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
        if (metadata.getSegmentName().startsWith("merged")) {
          // Check merged segment zk metadata
          assertNotNull(metadata.getCustomMap());
          assertEquals("100days",
              metadata.getCustomMap().get(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
          // Check merged segments are time partitioned
          assertEquals(metadata.getEndTimeMs() / (86_400_000L * 100), metadata.getStartTimeMs() / (86_400_000L * 100));
        }
      }

      // Check num total doc of merged segments are the same as the original segments
      JsonNode actualJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
      SqlResultComparator.areEqual(actualJson, expectedJson, sqlQuery);
      // Check query routing
      int numSegmentsQueried = actualJson.get("numSegmentsQueried").asInt();
      assertEquals(numSegmentsQueried, expectedNumSegmentsQueried[numTasks]);
    }
    // Check total tasks
    assertEquals(numTasks, 5);

    // Drop the table
    dropOfflineTable(SINGLE_LEVEL_CONCAT_TEST_TABLE);

    // Check if the task metadata is cleaned up on table deletion
    verifyTableDelete(offlineTableName);
  }

  /**
   * Test single level rollup task with duplicate data (original segments * 2)
   */
  @Test
  public void testSingleLevelRollup()
      throws Exception {
    // The original segments are time partitioned by month:
    // segmentName (totalDocs)
    // myTable2_16071_16101_3_1, myTable2_16071_16101_3_2 (9746)
    // myTable2_16102_16129_4_1, myTable2_16102_16129_4_2 (8690)
    // myTable2_16130_16159_5_1, myTable2_16130_16159_5_2 (9621)
    // myTable2_16160_16189_6_1, myTable2_16160_16189_6_2 (9454)
    // myTable2_16190_16220_7_1, myTable2_16190_16220_7_2 (10329)
    // myTable2_16221_16250_8_1, myTable2_16221_16250_8_2 (10468)
    // myTable2_16251_16281_9_1, myTable2_16251_16281_9_2 (10499)
    // myTable2_16282_16312_10_1, myTable2_16282_16312_10_2 (10196)
    // myTable2_16313_16342_11_1, myTable2_16313_16342_11_2 (9136)
    // myTable2_16343_16373_0_1, myTable2_16343_16373_0_2 (9292)
    // myTable2_16374_16404_1_1, myTable2_16374_16404_1_2 (8736)
    // myTable2_16405_16435_2_1, myTable2_16405_16435_2_2 (9378)

    // Expected merge tasks and result segments:
    // 1.
    //    {myTable2_16071_16101_3_1, myTable2_16071_16101_3_2, myTable2_16102_16129_4_1, myTable2_16102_16129_4_2,
    //     myTable2_16130_16159_5_1, myTable2_16130_16159_5_2, myTable2_16160_16189_6_1, myTable2_16160_16189_6_2
    //     myTable2_16190_16220_7}
    //      -> {merged_150days_T1_0_myTable2_16065_16198_0, merged_150days_T1_0_myTable2_16205_16219_1}
    // 2.
    //    {merged_150days_T1_0_myTable2_16205_16219_1, myTable2_16221_16250_8_1, myTable2_16221_16250_8_2,
    //     myTable2_16251_16281_9_1, myTable2_16251_16281_9_2, myTable2_16282_16312_10_1
    //     myTable2_16282_16312_10_2, myTable2_16313_16342_11_1, myTable2_16313_16342_11_2,
    //     myTable2_16343_16373_0_1, myTable2_16343_16373_0_2}
    //      -> {merged_150days_1628644088146_0_myTable2_16205_16345_0,
    //          merged_150days_1628644088146_0_myTable2_16352_16373_1}
    // 3.
    //    {merged_150days_1628644088146_0_myTable2_16352_16373_1, myTable2_16374_16404_1_1, myTable2_16374_16404_1_2
    //     myTable2_16405_16435_2_1, myTable2_16405_16435_2_2}
    //      -> {merged_150days_1628644105127_0_myTable2_16352_16429_0}

    String sqlQuery = "SELECT count(*) FROM myTable2"; // 115545 rows for the test table
    JsonNode expectedJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    int[] expectedNumSegmentsQueried = {16, 7, 3};
    long expectedWatermark = 16050 * 86_400_000L;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(SINGLE_LEVEL_ROLLUP_TEST_TABLE);
    int numTasks = 0;
    for (String tasks = _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE);
        tasks != null; tasks =
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE), numTasks++) {
      Assert.assertEquals(_helixTaskResourceManager.getTaskConfigs(tasks).size(), 1);
      Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
          .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.MergeRollupTask.TASK_TYPE)));
      // Will not schedule task if there's incomplete task
      Assert.assertNull(
          _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      waitForTaskToComplete();

      // Check watermark
      MergeRollupTaskMetadata minionTaskMetadata = MergeRollupTaskMetadata
          .fromZNRecord(_taskManager.getClusterInfoAccessor().getMinionMergeRollupTaskZNRecord(offlineTableName));
      assertNotNull(minionTaskMetadata);
      Assert.assertEquals((long) minionTaskMetadata.getWatermarkMap().get("150days"), expectedWatermark);
      expectedWatermark += 150 * 86_400_000L;

      // Check metadata of merged segments
      for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
        if (metadata.getSegmentName().startsWith("merged")) {
          // Check merged segment zk metadata
          assertNotNull(metadata.getCustomMap());
          assertEquals("150days",
              metadata.getCustomMap().get(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
          // Check merged segments are time partitioned
          assertEquals(metadata.getEndTimeMs() / (86_400_000L * 150), metadata.getStartTimeMs() / (86_400_000L * 150));
        }
      }

      // Check total doc of merged segments are less than the original segments
      JsonNode actualJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
      assertTrue(
          actualJson.get("resultTable").get("rows").get(0).get(0).asInt() < expectedJson.get("resultTable").get("rows")
              .get(0).get(0).asInt());
      // Check query routing
      int numSegmentsQueried = actualJson.get("numSegmentsQueried").asInt();
      assertEquals(numSegmentsQueried, expectedNumSegmentsQueried[numTasks]);
    }

    // Check total doc is half of the original after all merge tasks are finished
    JsonNode actualJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    assertEquals(actualJson.get("resultTable").get("rows").get(0).get(0).asInt(),
        expectedJson.get("resultTable").get("rows").get(0).get(0).asInt() / 2);
    // Check time column is rounded
    JsonNode responseJson =
        postSqlQuery("SELECT count(*), DaysSinceEpoch FROM myTable2 GROUP BY DaysSinceEpoch ORDER BY DaysSinceEpoch");
    for (int i = 0; i < responseJson.get("resultTable").get("rows").size(); i++) {
      int daysSinceEpoch = responseJson.get("resultTable").get("rows").get(i).get(1).asInt();
      Assert.assertTrue(daysSinceEpoch % 7 == 0);
    }
    // Check total tasks
    assertEquals(numTasks, 3);
  }

  /**
   * Test multi level concat task
   */
  @Test
  public void testMultiLevelConcat()
      throws Exception {
    // The original segments are time partitioned by month:
    // segmentName (totalDocs)
    // myTable3_16071_16101_3 (9746)
    // myTable3_16102_16129_4 (8690)
    // myTable3_16130_16159_5 (9621)
    // myTable3_16160_16189_6 (9454)
    // myTable3_16190_16220_7 (10329)
    // myTable3_16221_16250_8 (10468)
    // myTable3_16251_16281_9 (10499)
    // myTable3_16282_16312_10 (10196)
    // myTable3_16313_16342_11 (9136)
    // myTable3_16343_16373_0 (9292)
    // myTable3_16374_16404_1 (8736)
    // myTable3_16405_16435_2 (9378)

    // Expected merge tasks and results:
    // 1.
    //    45days: {myTable3_16071_16101_3, myTable3_16102_16129_4}
    //      -> {merged_45days_T1_0_myTable3_16071_16109_0, merged_45days_T1_0_myTable3_16110_16129_1}
    //    watermark: {45days: 16065, 90days: null}
    // 2.
    //    45days: {merged_45days_T1_0_myTable3_16110_16129_1, myTable3_16130_16159_5}
    //      -> {merged_45days_T2_0_myTable3_16110_16154_0, merged_45days_T2_0_myTable3_16155_16159_1}
    //    90days: {merged_45days_T1_0_myTable3_16071_16109_0}
    //      -> {merged_90days_T2_0_myTable3_16071_16109_0}
    //    watermark: {45days: 16110, 90days: 16020}
    // 3.
    //    45days: {merged_45days_T2_0_myTable3_16155_16159_1, myTable3_16160_16189_6, myTable3_16190_16220_7}
    //      -> {merged_45days_T3_0_myTable3_16155_16199_0, merged_45days_T3_0_myTable3_16200_16220_1}
    //    watermark: {45days: 16155, 90days: 16020}
    // 4.
    //    45days: {merged_45days_T3_-_myTable3_16200_16220_1, myTable3_16221_16250_8}
    //      -> {merged_45days_T4_0_myTable3_16200_16244_0, merged_45days_T4_0_myTable3_16245_16250_1}
    //    90days: {merged_45days_T2_0_myTable3_16110_16154_0, merged_45days_T3_0_myTable3_16155_16199_0}
    //      -> {merged_90days_T4_0_myTable3_16110_16199_0}
    //    watermark: {45days: 16200, 90days: 16110}
    // 5.
    //    45days: {merged_45days_T4_0_myTable3_16245_16250_1, myTable3_16251_16281_9, myTable3_16282_16312_10}
    //      -> {merged_45days_T5_0_myTable3_16245_16289_0, merged_45days_T5_0_myTable3_16290_16312_1}
    //    watermark: {45days: 16245, 90days: 16110}
    // 6.
    //    45days: {merged_45days_T5_0_myTable3_16290_16312_1, myTable3_16313_16342_11}
    //      -> {merged_45days_T6_0_myTable3_16290_16334_0, merged_45days_T6_0_myTable3_16335_16342_1}
    //    90days: {merged_45days_T4_0_myTable3_16200_16244_0, merged_45days_T5_0_myTable3_16245_16289_0}
    //      -> {merged_90days_T6_0_myTable3_16200_16289_0}
    //    watermark: {45days: 16290, 90days: 16200}
    // 7.
    //    45days: {merged_45days_T6_0_myTable3_16335_16342_1, myTable_16343_16373_0, myTable_16374_16404_1}
    //      -> {merged_45days_T7_0_myTable3_16335_16379_0, merged_45days_T7_0_myTable3_16380_16404_1}
    //    watermark: {45days: 16335, 90days: 16200}
    // 8.
    //    45days: {merged_45days_T7_0_myTable3_16380_16404_1, myTable3_16405_16435_2}
    //      -> {merged_45days_T8_0_myTable3_16380_16424_0, merged_45days_T8_1_myTable3_16425_16435_1}
    //    90days: {merged_45days_T6_0_myTable3_16290_16334_0, merged_45days_T7_0_myTable3_16335_16379_0}
    //      -> {merged_90days_T8_0_myTable3_16290_16379_0}
    //    watermark: {45days:16380, 90days: 16290}
    // 9.
    //    45days: no segment left, not scheduling
    //    90days: [16380, 16470) is not a valid merge window because windowEndTime > 45days watermark, not scheduling

    String sqlQuery = "SELECT count(*) FROM myTable3"; // 115545 rows for the test table
    JsonNode expectedJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    int[] expectedNumSubTasks = {1, 2, 1, 2, 1, 2, 1, 2, 1};
    int[] expectedNumSegmentsQueried = {12, 12, 11, 10, 9, 8, 7, 6, 5};
    Long[] expectedWatermarks45Days = {16065L, 16110L, 16155L, 16200L, 16245L, 16290L, 16335L, 16380L};
    Long[] expectedWatermarks90Days = {null, 16020L, 16020L, 16110L, 16110L, 16200L, 16200L, 16290L};
    for (int i = 0; i < expectedWatermarks45Days.length; i++) {
      expectedWatermarks45Days[i] *= 86_400_000L;
    }
    for (int i = 1; i < expectedWatermarks90Days.length; i++) {
      expectedWatermarks90Days[i] *= 86_400_000L;
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(MULTI_LEVEL_CONCAT_TEST_TABLE);
    int numTasks = 0;
    for (String tasks = _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE);
        tasks != null; tasks =
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE), numTasks++) {
      Assert.assertEquals(_helixTaskResourceManager.getTaskConfigs(tasks).size(), expectedNumSubTasks[numTasks]);
      Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
          .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.MergeRollupTask.TASK_TYPE)));
      // Will not schedule task if there's incomplete task
      Assert.assertNull(
          _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      waitForTaskToComplete();

      // Check watermark
      MergeRollupTaskMetadata minionTaskMetadata = MergeRollupTaskMetadata
          .fromZNRecord(_taskManager.getClusterInfoAccessor().getMinionMergeRollupTaskZNRecord(offlineTableName));
      assertNotNull(minionTaskMetadata);
      Assert.assertEquals(minionTaskMetadata.getWatermarkMap().get("45days"), expectedWatermarks45Days[numTasks]);
      Assert.assertEquals(minionTaskMetadata.getWatermarkMap().get("90days"), expectedWatermarks90Days[numTasks]);

      // Check metadata of merged segments
      for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
        if (metadata.getSegmentName().startsWith("merged")) {
          // Check merged segment zk metadata
          assertNotNull(metadata.getCustomMap());
          if (metadata.getSegmentName().startsWith("merged_45days")) {
            assertEquals("45days",
                metadata.getCustomMap().get(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
            assertEquals(metadata.getEndTimeMs() / (86_400_000L * 45), metadata.getStartTimeMs() / (86_400_000L * 45));
          }
          if (metadata.getSegmentName().startsWith("merged_90days")) {
            assertEquals("90days",
                metadata.getCustomMap().get(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
            assertEquals(metadata.getEndTimeMs() / (86_400_000L * 90), metadata.getStartTimeMs() / (86_400_000L * 90));
          }
        }
      }

      // Check total doc of merged segments are the same as the original segments
      JsonNode actualJson = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
      SqlResultComparator.areEqual(actualJson, expectedJson, sqlQuery);
      // Check query routing
      int numSegmentsQueried = actualJson.get("numSegmentsQueried").asInt();
      assertEquals(numSegmentsQueried, expectedNumSegmentsQueried[numTasks]);
    }
    // Check total tasks
    assertEquals(numTasks, 8);
  }

  protected void verifyTableDelete(String tableNameWithType) {
    TestUtils.waitForCondition(input -> {
      // Check if the segment lineage is cleaned up
      if (SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, tableNameWithType) != null) {
        System.out.println("Not deleted..");
        return false;
      }
      // Check if the task metadata is cleaned up
      if (MinionTaskMetadataUtils
          .fetchTaskMetadata(_propertyStore, MinionConstants.MergeRollupTask.TASK_TYPE, tableNameWithType) != null) {
        return false;
      }
      return true;
    }, 1_000L, 60_000L, "Failed to delete table");
  }

  protected void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
