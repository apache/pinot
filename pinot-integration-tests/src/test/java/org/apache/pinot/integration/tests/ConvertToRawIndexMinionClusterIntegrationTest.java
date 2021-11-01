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
import org.apache.commons.lang.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.ConvertToRawIndexTask;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends HybridClusterIntegrationTest and add Minions into the cluster to convert 3 metric
 * columns' index into raw index for OFFLINE segments.
 */
public class ConvertToRawIndexMinionClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final String COLUMNS_TO_CONVERT = "ActualElapsedTime,ArrDelay,DepDelay,CRSDepTime";

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @Nullable
  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  // NOTE: Only allow converting raw index for v1 segment
  @Override
  protected String getSegmentVersion() {
    return SegmentVersion.v1.name();
  }

  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> convertToRawIndexTaskConfigs = new HashMap<>();
    convertToRawIndexTaskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "5");
    convertToRawIndexTaskConfigs.put(ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY, COLUMNS_TO_CONVERT);
    return new TableTaskConfig(Collections.singletonMap(ConvertToRawIndexTask.TASK_TYPE, convertToRawIndexTaskConfigs));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // The parent setUp() sets up Zookeeper, Kafka, controller, broker and servers
    super.setUp();

    startMinion();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
  }

  @Test
  public void testConvertToRawIndexTask()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    File testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-0", offlineTableName);
    if (!testDataDir.isDirectory()) {
      testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-1", offlineTableName);
    }
    Assert.assertTrue(testDataDir.isDirectory());
    File tableDataDir = testDataDir;

    // Check that all columns have dictionary
    File[] indexDirs = tableDataDir.listFiles();
    Assert.assertNotNull(indexDirs);
    for (File indexDir : indexDirs) {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
      for (String columnName : segmentMetadata.getSchema().getColumnNames()) {
        Assert.assertTrue(segmentMetadata.getColumnMetadataFor(columnName).hasDictionary());
      }
    }

    // Should create the task queues and generate a ConvertToRawIndexTask task with 5 child tasks
    Assert.assertNotNull(_taskManager.scheduleTasks().get(ConvertToRawIndexTask.TASK_TYPE));
    Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(ConvertToRawIndexTask.TASK_TYPE)));

    // Should generate one more ConvertToRawIndexTask task with 3 child tasks
    Assert.assertNotNull(_taskManager.scheduleTasks().get(ConvertToRawIndexTask.TASK_TYPE));

    // Should not generate more tasks
    Assert.assertNull(_taskManager.scheduleTasks().get(ConvertToRawIndexTask.TASK_TYPE));

    // Wait at most 600 seconds for all tasks COMPLETED and new segments refreshed
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(ConvertToRawIndexTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }

      // Check segment ZK metadata
      for (SegmentZKMetadata segmentZKMetadata : _helixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
        Map<String, String> customMap = segmentZKMetadata.getCustomMap();
        if (customMap == null || customMap.size() != 1 || !customMap
            .containsKey(ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) {
          return false;
        }
      }

      // Check segment metadata
      File[] indexDirs1 = tableDataDir.listFiles();
      Assert.assertNotNull(indexDirs1);
      for (File indexDir : indexDirs1) {
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
            if (segmentMetadata.getColumnMetadataFor(columnName).hasDictionary()) {
              return false;
            }
          } else {
            if (!segmentMetadata.getColumnMetadataFor(columnName).hasDictionary()) {
              return false;
            }
          }
        }
      }

      return true;
    }, 600_000L, "Failed to get all tasks COMPLETED and new segments refreshed");
  }

  @Test
  public void testPinotHelixResourceManagerAPIs() {
    // Instance APIs
    Assert.assertEquals(_helixResourceManager.getAllInstances().size(), 5);
    Assert.assertEquals(_helixResourceManager.getOnlineInstanceList().size(), 5);
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
  public void tearDown()
      throws Exception {
    stopMinion();

    super.tearDown();
  }
}
