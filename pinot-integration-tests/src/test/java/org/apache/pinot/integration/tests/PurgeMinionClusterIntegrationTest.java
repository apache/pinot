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
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
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
 * Integration test for minion task of type "PurgeTask"
 */
public class PurgeMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PURGE_FIRST_RUN_TABLE = "myTable1";
  private static final String PURGE_DELTA_PASSED_TABLE = "myTable2";
  private static final String PURGE_DELTA_NOT_PASSED_TABLE = "myTable3";

  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected String _tableName;

  protected final File _segmentDir1 = new File(_tempDir, "segmentDir1");
  protected final File _segmentDir2 = new File(_tempDir, "segmentDir2");
  protected final File _segmentDir3 = new File(_tempDir, "segmentDir3");

  protected final File _tarDir1 = new File(_tempDir, "tarDir1");
  protected final File _tarDir2 = new File(_tempDir, "tarDir2");
  protected final File _tarDir3 = new File(_tempDir, "tarDir3");

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir1, _tarDir1, _segmentDir2, _tarDir2, _segmentDir3,
        _tarDir3);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    setTableName(PURGE_DELTA_NOT_PASSED_TABLE);
    TableConfig purgeDeltaNotPassedTableConfig = createOfflineTableConfig();
    purgeDeltaNotPassedTableConfig.setTaskConfig(getPurgeTaskConfig());
    setTableName(PURGE_FIRST_RUN_TABLE);
    TableConfig purgeTableConfig = createOfflineTableConfig();
    purgeTableConfig.setTaskConfig(getPurgeTaskConfig());

    setTableName(PURGE_DELTA_PASSED_TABLE);
    TableConfig purgeDeltaPassedTableConfig = createOfflineTableConfig();
    purgeDeltaPassedTableConfig.setTaskConfig(getPurgeTaskConfig());

    addTableConfig(purgeTableConfig);
    addTableConfig(purgeDeltaPassedTableConfig);
    addTableConfig(purgeDeltaNotPassedTableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, purgeTableConfig, schema, 0, _segmentDir1, _tarDir1);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, purgeDeltaPassedTableConfig, schema, 0, _segmentDir2,
        _tarDir2);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, purgeDeltaNotPassedTableConfig, schema, 0,
        _segmentDir3, _tarDir3);

    uploadSegments(PURGE_FIRST_RUN_TABLE, _tarDir1);
    uploadSegments(PURGE_DELTA_PASSED_TABLE, _tarDir2);
    uploadSegments(PURGE_DELTA_NOT_PASSED_TABLE, _tarDir3);

    startMinion();
    setRecordPurger();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    // Set up segments' ZK metadata to check how code handle passed and not passed delay
    String offlineTableNamePassed = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_PASSED_TABLE);
    String offlineTableNameNotPassed = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_NOT_PASSED_TABLE);

    // Set up passed delay
    List<SegmentZKMetadata> segmentsZKMetadataDeltaPassed =
        _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableNamePassed);
    Map<String, String> customSegmentMetadataPassed = new HashMap<>();
    customSegmentMetadataPassed.put(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
        String.valueOf(System.currentTimeMillis() - 88400000));
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadataDeltaPassed) {
      segmentZKMetadata.setCustomMap(customSegmentMetadataPassed);
      _pinotHelixResourceManager.updateZkMetadata(offlineTableNamePassed, segmentZKMetadata);
    }

    // Set up not passed delay
    List<SegmentZKMetadata> segmentsZKMetadataDeltaNotPassed =
        _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableNameNotPassed);
    Map<String, String> customSegmentMetadataNotPassed = new HashMap<>();
    customSegmentMetadataNotPassed.put(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
        String.valueOf(System.currentTimeMillis() - 4000));
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadataDeltaNotPassed) {
      segmentZKMetadata.setCustomMap(customSegmentMetadataNotPassed);
      _pinotHelixResourceManager.updateZkMetadata(offlineTableNameNotPassed, segmentZKMetadata);
    }
  }

  private void setRecordPurger() {
    MinionContext minionContext = MinionContext.getInstance();
    minionContext.setRecordPurgerFactory(rawTableName -> {
      List<String> tableNames =
          Arrays.asList(PURGE_FIRST_RUN_TABLE, PURGE_DELTA_PASSED_TABLE, PURGE_DELTA_NOT_PASSED_TABLE);
      if (tableNames.contains(rawTableName)) {
        return row -> row.getValue("Quarter").equals(1);
      } else {
        return null;
      }
    });
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  private TableTaskConfig getPurgeTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.PurgeTask.LAST_PURGE_TIME_THREESOLD_PERIOD, "1d");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE, tableTaskConfigs));
  }

  /**
   * Test purge with no metadata on the segments (checking null safe implementation)
   */
  @Test
  public void testFirstRunPurge()
      throws Exception {
    // Expected purge task generation :
    // 1. No previous purge run so all segment should be processed and purge metadata sould be added to the segments
    // 2. Check that we cannot run on same time two purge generation ensuring running segment will be skipped
    // 3. Check segment ZK metadata to ensure purge time is updated into the metadata
    // 4. Check after the first run of the purge if we rerun a purge task generation no task should be scheduled
    // 5. Check the purge process itself by setting an expecting number of rows

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_FIRST_RUN_TABLE);
    assertNotNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.PurgeTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    assertNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    waitForTaskToComplete();

    // Check that metadata contains expected values
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Check purge time
      assertTrue(
          metadata.getCustomMap().containsKey(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX));
    }
    // Should not generate new purge task as the last time purge is not greater than last + 1day (default purge delay)
    assertNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));

    // 28057 rows with quarter = 1
    // 115545 totals rows
    // Expecting 115545 - 28057 = 87488 rows after purging
    // It might take some time for server to load the purged segments
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(PURGE_FIRST_RUN_TABLE) == 87488, 60_000L,
        "Failed to get expected purged records");

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
    // 1. The purge time on this test is greater than the threshold expected (88400000 > 1d (86400000))
    // 2. Check that we cannot run on same time two purge generation ensuring running segment will be skipped
    // 3. Check segment ZK metadata to ensure purge time is updated into the metadata
    // 4. Check after the first run of the purge if we rerun a purge task generation no task should be scheduled
    // 5. Check the purge process itself by setting an expecting number of rows

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_PASSED_TABLE);
    assertNotNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.PurgeTask.TASK_TYPE)));
    // Will not schedule task if there's incomplete task
    assertNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    waitForTaskToComplete();

    // Check that metadata contains expected values
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Check purge time
      String purgeTime =
          metadata.getCustomMap().get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX);
      assertNotNull(purgeTime);
      assertTrue(System.currentTimeMillis() - Long.parseLong(purgeTime) < 86400000);
    }
    // Should not generate new purge task as the last time purge is not greater than last + 1day (default purge delay)
    assertNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));

    // 28057 rows with quarter = 1
    // 115545 totals rows
    // Expecting 115545 - 28057 = 87488 rows after purging
    // It might take some time for server to load the purged segments
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(PURGE_DELTA_PASSED_TABLE) == 87488, 60_000L,
        "Failed to get expected purged records");

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
    // Expected no purge task generation :
    // 1. segment purge time is set to System.currentTimeMillis() - 4000 so a new purge should not be triggered as the
    //    delay is 1d (86400000ms)
    // 2. Check no task has been scheduled
    // 3. Check segment ZK metadata to ensure purge time is not updated into the metadata
    // 4. Check the purge process itself have not been run by setting an expecting number of rows

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_DELTA_NOT_PASSED_TABLE);

    // No task should be schedule as the delay is not passed
    assertNull(_taskManager.scheduleTasks(offlineTableName).get(MinionConstants.PurgeTask.TASK_TYPE));
    for (SegmentZKMetadata metadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      // Check purge time
      String purgeTime =
          metadata.getCustomMap().get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX);
      assertNotNull(purgeTime);
      long purgeTimeMs = Long.parseLong(purgeTime);
      assertTrue(System.currentTimeMillis() - purgeTimeMs > 4000);
      assertTrue(System.currentTimeMillis() - purgeTimeMs < 86400000);
    }

    // Nothing should be purged
    assertEquals(getCurrentCountStarResult(PURGE_DELTA_NOT_PASSED_TABLE), 115545);

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
      if (MinionTaskMetadataUtils.fetchTaskMetadata(_propertyStore, MinionConstants.PurgeTask.TASK_TYPE,
          tableNameWithType) != null) {
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
