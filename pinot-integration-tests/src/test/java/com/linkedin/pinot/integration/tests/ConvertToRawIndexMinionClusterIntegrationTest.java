/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends HybridClusterIntegrationTest and add Minions into the cluster to convert 3 metric
 * columns' index into raw index for OFFLINE segments.
 */
public class ConvertToRawIndexMinionClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final int NUM_MINIONS = 3;
  private static final String COLUMNS_TO_CONVERT = "ActualElapsedTime,ArrDelay,DepDelay,CRSDepTime";

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @Nullable
  @Override
  protected List<String> getRawIndexColumns() {
    return null;
  }

  @Override
  protected TableTaskConfig getTaskConfig() {
    TableTaskConfig taskConfig = new TableTaskConfig();
    Map<String, String> convertToRawIndexTaskConfigs = new HashMap<>();
    convertToRawIndexTaskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "5");
    convertToRawIndexTaskConfigs.put(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY, COLUMNS_TO_CONVERT);
    taskConfig.setTaskTypeConfigsMap(
        Collections.singletonMap(MinionConstants.ConvertToRawIndexTask.TASK_TYPE, convertToRawIndexTaskConfigs));
    return taskConfig;
  }

  @BeforeClass
  public void setUp() throws Exception {
    // The parent setUp() sets up Zookeeper, Kafka, controller, broker and servers
    super.setUp();

    startMinions(NUM_MINIONS, null);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
  }

  @Test
  public void testConvertToRawIndexTask() throws Exception {
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    File testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-0", offlineTableName);
    if (!testDataDir.isDirectory()) {
      testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-1", offlineTableName);
    }
    Assert.assertTrue(testDataDir.isDirectory());
    final File tableDataDir = testDataDir;

    // Check that all columns have dictionary
    File[] indexDirs = tableDataDir.listFiles();
    Assert.assertNotNull(indexDirs);
    for (File indexDir : indexDirs) {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
      for (String columnName : segmentMetadata.getSchema().getColumnNames()) {
        Assert.assertTrue(segmentMetadata.hasDictionary(columnName));
      }
    }

    // Should create the task queues and generate a ConvertToRawIndexTask task with 5 child tasks
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(MinionConstants.ConvertToRawIndexTask.TASK_TYPE));
    Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.ConvertToRawIndexTask.TASK_TYPE)));

    // Should generate one more ConvertToRawIndexTask task with 3 child tasks
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(MinionConstants.ConvertToRawIndexTask.TASK_TYPE));

    // Should not generate more tasks
    Assert.assertFalse(_taskManager.scheduleTasks().containsKey(MinionConstants.ConvertToRawIndexTask.TASK_TYPE));

    // Wait at most 600 seconds for all tasks COMPLETED and new segments refreshed
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        // Check task state
        for (TaskState taskState : _helixTaskResourceManager.getTaskStates(
            MinionConstants.ConvertToRawIndexTask.TASK_TYPE).values()) {
          if (taskState != TaskState.COMPLETED) {
            return false;
          }
        }

        // Check segment ZK metadata
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : _helixResourceManager.getOfflineSegmentMetadata(
            offlineTableName)) {
          Map<String, String> customMap = offlineSegmentZKMetadata.getCustomMap();
          if (customMap == null || customMap.size() != 1 || !customMap.containsKey(
              MinionConstants.ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) {
            return false;
          }
        }

        // Check segment metadata
        File[] indexDirs = tableDataDir.listFiles();
        Assert.assertNotNull(indexDirs);
        for (File indexDir : indexDirs) {
          SegmentMetadata segmentMetadata;

          // Segment metadata file might not exist if the segment is refreshing
          try {
            segmentMetadata = new SegmentMetadataImpl(indexDir);
          } catch (Exception e) {
            return false;
          }

          // The columns in COLUMNS_TO_CONVERT should have raw index
          List<String> rawIndexColumns = Arrays.asList(StringUtils.split(COLUMNS_TO_CONVERT, ','));
          for (String columnName : segmentMetadata.getSchema().getColumnNames()) {
            if (rawIndexColumns.contains(columnName)) {
              if (segmentMetadata.hasDictionary(columnName)) {
                return false;
              }
            } else {
              if (!segmentMetadata.hasDictionary(columnName)) {
                return false;
              }
            }
          }
        }

        return true;
      }
    }, 600_000L, "Failed to get all tasks COMPLETED and new segments refreshed");
  }

  @Test
  public void testPinotHelixResourceManagerAPIs() {
    // Instance APIs
    Assert.assertEquals(_helixResourceManager.getAllInstances().size(), 6);
    Assert.assertEquals(_helixResourceManager.getOnlineInstanceList().size(), 6);
    Assert.assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), 0);
    Assert.assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 0);

    // Table APIs
    String rawTableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    List<String> tableNames = _helixResourceManager.getAllTables();
    Assert.assertEquals(tableNames.size(), 2);
    Assert.assertTrue(tableNames.contains(offlineTableName));
    Assert.assertTrue(tableNames.contains(realtimeTableName));
    Assert.assertEquals(_helixResourceManager.getAllRawTables(), Collections.singletonList(rawTableName));
    Assert.assertEquals(_helixResourceManager.getAllRealtimeTables(), Collections.singletonList(realtimeTableName));

    // Tenant APIs
    Assert.assertEquals(_helixResourceManager.getAllBrokerTenantNames(), Collections.singleton("TestTenant"));
    Assert.assertEquals(_helixResourceManager.getAllServerTenantNames(), Collections.singleton("TestTenant"));
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopMinion();

    super.tearDown();
  }
}
