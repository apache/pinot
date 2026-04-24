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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that runs the Purge minion task with METADATA {@link
 * org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentPushType} to verify the full flow.
 * Only {@link #testFirstRunPurge()} is enabled; other tests from the base class are disabled.
 */
public class PurgeMetadataPushMinionClusterIntegrationTest extends PurgeMinionClusterIntegrationTest {

  @Override
  protected boolean shouldUseSharedRichCluster() {
    return true;
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      super.setUp();
      return;
    }

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDataDir, _segmentTarDir);

    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    setTableName(PURGE_FIRST_RUN_TABLE);
    Schema schema = createSchema();
    schema.setSchemaName(PURGE_FIRST_RUN_TABLE);
    addSchema(schema);

    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setTaskConfig(getPurgeTaskConfig());
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDataDir,
        _segmentTarDir);
    uploadSegments(PURGE_FIRST_RUN_TABLE, _segmentTarDir);

    setRecordPurger();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
  }

  @Override
  protected TableTaskConfig getPurgeTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.PurgeTask.LAST_PURGE_TIME_THREESOLD_PERIOD, "1d");
    tableTaskConfigs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    tableTaskConfigs.put(MinionTaskUtils.ALLOW_METADATA_PUSH_WITH_LOCAL_FS, "true");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE, tableTaskConfigs));
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      super.tearDown();
      return;
    }

    try {
      cleanUpSharedMetadataPushState();
    } finally {
      MinionContext.getInstance().setRecordPurgerFactory(null);
      super.tearDown();
    }
  }

  private void cleanUpSharedMetadataPushState()
      throws Exception {
    if (_pinotHelixResourceManager != null && _pinotHelixResourceManager.hasOfflineTable(PURGE_FIRST_RUN_TABLE)) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(PURGE_FIRST_RUN_TABLE);
      dropOfflineTable(PURGE_FIRST_RUN_TABLE);
      verifyTableDelete(offlineTableName);
    }
    if (_pinotHelixResourceManager != null) {
      _pinotHelixResourceManager.deleteSchema(PURGE_FIRST_RUN_TABLE);
    }
    deletePurgeTaskQueue();
  }

  private void deletePurgeTaskQueue() {
    if (_helixTaskResourceManager == null
        || !_helixTaskResourceManager.getTaskTypes().contains(MinionConstants.PurgeTask.TASK_TYPE)) {
      return;
    }

    _helixTaskResourceManager.deleteTaskQueue(MinionConstants.PurgeTask.TASK_TYPE, false);
    TestUtils.waitForCondition(
        input -> !_helixTaskResourceManager.getTaskTypes().contains(MinionConstants.PurgeTask.TASK_TYPE), 60_000L,
        "Failed to delete purge task queue");
  }

  @Override
  @Test(enabled = false)
  public void testPassedDelayTimePurge() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testNotPassedDelayTimePurge() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testPurgeOnOldSegmentsWithIndicesOnNewColumns() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testSegmentDeletionWhenAllRecordsPurged() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testRealtimeLastSegmentPreservation() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }
}
