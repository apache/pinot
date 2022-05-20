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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test for minion task of type "PurgeTask"
 */
public class PurgeMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PURGE_FIRST_RUN_TABLE = "myTable1";
  private static final String PURGE_DELTA_PASSED_TABLE = "myTable2";
  private static final String PURGE_DELTA_NOT_PASSED_TABLE = "myTable3";

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
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir1, _tarDir1,
        _segmentDir2, _tarDir2, _segmentDir3, _tarDir3);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig purgeTableConfig =
        createOfflineTableConfig(PURGE_FIRST_RUN_TABLE, getPurgeTaskConfig());
    TableConfig purgeDeltaPassedTableConfig =
        createOfflineTableConfig(PURGE_DELTA_PASSED_TABLE, getPurgeTaskConfig());
    TableConfig purgeDeltaNotPassedTableConfig =
        createOfflineTableConfig(PURGE_DELTA_NOT_PASSED_TABLE, getPurgeTaskConfig());
    addTableConfig(purgeTableConfig);
    addTableConfig(purgeDeltaPassedTableConfig);
    addTableConfig(purgeDeltaNotPassedTableConfig);




    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, purgeTableConfig, schema, 0, _segmentDir1, _tarDir1);

    buildSegmentsFromAvroWithPurgeTime(avroFiles, purgeDeltaPassedTableConfig, schema, 0,
        _segmentDir2, _tarDir2, String.valueOf(System.currentTimeMillis() - 863660000));
    buildSegmentsFromAvroWithPurgeTime(avroFiles, purgeDeltaNotPassedTableConfig, schema, 0,
        _segmentDir3, _tarDir3, String.valueOf(System.currentTimeMillis() - 400000));

    uploadSegments(PURGE_FIRST_RUN_TABLE, _tarDir1);
    uploadSegments(PURGE_DELTA_PASSED_TABLE, _tarDir2);
    uploadSegments(PURGE_DELTA_NOT_PASSED_TABLE, _tarDir3);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    startMinion();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    //set up metadata on segment to check how code handle passed and not passed delay
    String tablenameOfflineNotPassed = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_NOT_PASSED_TABLE);
    String tablenameOfflinePassed = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_PASSED_TABLE);

    //set up passed delay
    List<SegmentZKMetadata> segmentsZKMetadataDeltaPassed = _pinotHelixResourceManager
        .getSegmentsZKMetadata(tablenameOfflinePassed);

    Map<String, String> customSegmentMetadataPassed = new HashMap<>();
    customSegmentMetadataPassed.put(MinionConstants.PurgeTask.TASK_TYPE
        + MinionConstants.TASK_TIME_SUFFIX, String.valueOf(System.currentTimeMillis() - 88400000));

    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadataDeltaPassed) {
      segmentZKMetadata.setCustomMap(customSegmentMetadataPassed);
      _pinotHelixResourceManager.updateZkMetadata(tablenameOfflinePassed, segmentZKMetadata);
    }
    //set up not passed delay
    List<SegmentZKMetadata> segmentsZKMetadataDeltaNotPassed = _pinotHelixResourceManager
        .getSegmentsZKMetadata(tablenameOfflineNotPassed);
    Map<String, String> customSegmentMetadataNotPassed = new HashMap<>();
    customSegmentMetadataNotPassed.put(MinionConstants.PurgeTask.TASK_TYPE
        + MinionConstants.TASK_TIME_SUFFIX, String.valueOf(System.currentTimeMillis() - 4000));
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadataDeltaNotPassed) {
      segmentZKMetadata.setCustomMap(customSegmentMetadataNotPassed);
      _pinotHelixResourceManager.updateZkMetadata(tablenameOfflineNotPassed, segmentZKMetadata);
    }
  }

  private TableConfig createOfflineTableConfig(String tableName, TableTaskConfig taskConfig) {
    return createOfflineTableConfig(tableName, taskConfig, null);
  }

  private TableConfig createOfflineTableConfig(String tableName, TableTaskConfig taskConfig,
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

  private TableTaskConfig getPurgeTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("deltaPurgeTimePeriod", "1d");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE, tableTaskConfigs));
  }



  private static void buildSegmentsFromAvroWithPurgeTime(List<File> avroFiles, TableConfig tableConfig,
      Schema schema, int baseSegmentIndex, File segmentDir, File tarDir, String purgeTime)
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
        SegmentZKPropsConfig c = new SegmentZKPropsConfig();
        //generate segment with old purge time
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
   * Test purge with no metadata on the segments (checking null safe implementation)
   */
  @Test
  public void testFirstRunPurge()
      throws Exception {
    // Expected purge task generation :
    // 1. No previous purge run so all segment should be processed and purge metadata sould be added to the segments
    //

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_FIRST_RUN_TABLE);
    _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE);
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.PurgeTask.TASK_TYPE)));
      // Will not schedule task if there's incomplete task
    assertNull(
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    waitForTaskToComplete();
    // check that metadat
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Check purgeTimeIn
      assertTrue(metadata.getCustomMap().containsKey(MinionConstants.PurgeTask.TASK_TYPE
          + MinionConstants.TASK_TIME_SUFFIX));
    }
    // should not reload a new purge as the last time purge is not greater than last + 1day (default purge delay)
    assertNull(
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));


    // Drop the table
    dropOfflineTable(PURGE_FIRST_RUN_TABLE);

    // Check if the task metadata is cleaned up on table deletion
    verifyTableDelete(offlineTableName);
  }




  /**
   * Test purge with passed delay
   */
  @Test
  public void testPassedDelayTimePurge()
      throws Exception {
    // Expected purge task generation :
    // 1. The purge time on this test is greater than the threshold expected (863660000 > 1d (86400000) )
    // purge should be run on all segments

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_PASSED_TABLE);
    _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE);
    assertTrue(_helixTaskResourceManager.getTaskQueues()
          .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.PurgeTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    assertNull(
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    waitForTaskToComplete();
    // check that metadata contains expected values
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Check purgeTimeIn
      assertTrue(metadata.getCustomMap().containsKey(MinionConstants.PurgeTask.TASK_TYPE
          + MinionConstants.TASK_TIME_SUFFIX));
      //check that the purge have been run on these segments
      assertTrue(System.currentTimeMillis() - Long.valueOf(metadata.getCustomMap()
          .get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) < 86400000);
    }
    // should not reload a new purge as the last time purge is not greater than last + 1day (default purge delay)
    assertNull(
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));

    // Drop the table
    dropOfflineTable(PURGE_DELTA_PASSED_TABLE);

    // Check if the task metadata is cleaned up on table deletion
    verifyTableDelete(offlineTableName);
  }


  /**
   * Test purge with not passed delay
   */
  @Test
  public void testNotPassedDelayTimePurge()
      throws Exception {
    // Expected purge task generation :
    // 1. segment purge time is set to System.currentTimeMillis() -
    // 4000 so a new purge should not be triggered as the default delay is 1d 86400000 ms

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_NOT_PASSED_TABLE);

    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
    }

    //no task should be schedule as the delay is not passed
    assertNull(
        _taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));


    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      assertTrue(metadata.getCustomMap().containsKey(MinionConstants.PurgeTask.TASK_TYPE
          + MinionConstants.TASK_TIME_SUFFIX));
      //check that the purge have not been run on these segments
      assertTrue(System.currentTimeMillis() - Long.valueOf(metadata.getCustomMap()
          .get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) > 4000);
      assertTrue(System.currentTimeMillis() - Long.valueOf(metadata.getCustomMap()
          .get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) < 86400000);
    }

    // Drop the table
    dropOfflineTable(PURGE_DELTA_NOT_PASSED_TABLE);

    // Check if the task metadata is cleaned up on table deletion
    verifyTableDelete(offlineTableName);
  }






  protected void verifyTableDelete(String tableNameWithType) {
    TestUtils.waitForCondition(input -> {
      // Check if the segment lineage is cleaned up
      if (SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, tableNameWithType) != null) {
        return false;
      }
      // Check if the task metadata is cleaned up
      if (MinionTaskMetadataUtils
          .fetchTaskMetadata(_propertyStore, MinionConstants.PurgeTask.TASK_TYPE, tableNameWithType) != null) {
        return false;
      }
      return true;
    }, 1_000L, 60_000L, "Failed to delete table");
  }

  protected void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(MinionConstants.PurgeTask.TASK_TYPE)
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
