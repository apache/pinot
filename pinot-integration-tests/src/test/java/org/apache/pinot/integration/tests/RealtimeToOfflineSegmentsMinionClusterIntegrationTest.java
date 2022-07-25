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

import java.io.IOException;
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
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for minion task of type "RealtimeToOfflineSegmentsTask"
 * With every task run, a new segment is created in the offline table for 1 day. Watermark also keeps progressing
 * accordingly.
 */
public class RealtimeToOfflineSegmentsMinionClusterIntegrationTest extends RealtimeClusterIntegrationTest {

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;
  private PinotHelixResourceManager _pinotHelixResourceManager;

  private long _dataSmallestTimeMs;
  private String _realtimeTableName;
  private String _offlineTableName;

  @Override
  protected TableTaskConfig getTaskConfig() {
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>()));
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // Setup realtime table, and blank offline table
    super.setUp();
    addTableConfig(createOfflineTableConfig());
    startMinion();

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    _offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(_realtimeTableName);
    long minSegmentTimeMs = Long.MAX_VALUE;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
        minSegmentTimeMs = Math.min(minSegmentTimeMs, segmentZKMetadata.getStartTimeMs());
      }
    }
    _dataSmallestTimeMs = minSegmentTimeMs;
  }

  @Test
  public void testRealtimeToOfflineSegmentsTask()
      throws IOException {
    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(_offlineTableName);
    Assert.assertTrue(segmentsZKMetadata.isEmpty());

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
      Assert.assertNotNull(_taskManager.scheduleTasks().get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      Assert.assertTrue(_helixTaskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      Assert.assertNull(_taskManager.scheduleTasks().get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));

      // Wait at most 600 seconds for all tasks COMPLETED
      waitForTaskToComplete(expectedWatermark);
      // check segment is in offline
      segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(_offlineTableName);
      Assert.assertEquals(segmentsZKMetadata.size(), (numOfflineSegmentsPerTask * (i + 1)));

      long expectedOfflineSegmentTimeMs = expectedWatermark - 86400000;
      for (int j = (numOfflineSegmentsPerTask * i); j < segmentsZKMetadata.size(); j++) {
        SegmentZKMetadata segmentZKMetadata = segmentsZKMetadata.get(j);
        Assert.assertEquals(segmentZKMetadata.getStartTimeMs(), expectedOfflineSegmentTimeMs);
        Assert.assertEquals(segmentZKMetadata.getEndTimeMs(), expectedOfflineSegmentTimeMs);
        if (segmentPartitionConfig != null) {
          Assert.assertEquals(segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().keySet(),
              segmentPartitionConfig.getColumnPartitionMap().keySet());
          for (String partitionColumn : segmentPartitionConfig.getColumnPartitionMap().keySet()) {
            Assert.assertEquals(segmentZKMetadata.getPartitionMetadata().getPartitions(partitionColumn).size(), 1);
          }
        }
      }
      expectedWatermark += 86400000;
    }
    this.testHardcodedQueries();

    // Delete the table
    dropRealtimeTable(_realtimeTableName);

    // Check if the metadata is cleaned up on table deletion
    verifyTableDelete(_realtimeTableName);
  }

  @Nullable
  @Override
  protected SegmentPartitionConfig getSegmentPartitionConfig() {
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    ColumnPartitionConfig columnOneConfig = new ColumnPartitionConfig("murmur", 3);
    columnPartitionConfigMap.put("AirlineID", columnOneConfig);
    ColumnPartitionConfig columnTwoConfig = new ColumnPartitionConfig("hashcode", 2);
    columnPartitionConfigMap.put("OriginAirportID", columnTwoConfig);
    return new SegmentPartitionConfig(columnPartitionConfigMap);
  }

  protected void verifyTableDelete(String tableNameWithType) {
    TestUtils.waitForCondition(input -> {
      // Check if the task metadata is cleaned up
      if (MinionTaskMetadataUtils.fetchTaskMetadata(_propertyStore,
          MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, tableNameWithType) != null) {
        return false;
      }
      return true;
    }, 1_000L, 60_000L, "Failed to delete table");
  }

  private void waitForTaskToComplete(long expectedWatermark) {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(
          MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");

    // Check segment ZK metadata
    ZNRecord znRecord = _taskManager.getClusterInfoAccessor()
        .getMinionTaskMetadataZNRecord(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, _realtimeTableName);
    RealtimeToOfflineSegmentsTaskMetadata minionTaskMetadata =
        znRecord != null ? RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord) : null;
    Assert.assertNotNull(minionTaskMetadata);
    Assert.assertEquals(minionTaskMetadata.getWatermarkMs(), expectedWatermark);
  }

  @Test(enabled = false)
  @Override
  public void testDictionaryBasedQueries() {
  }

  @Test(enabled = false)
  @Override
  public void testHardcodedQueries() {
  }

  @Test(enabled = false)
  @Override
  public void testQueriesFromQueryFile() {
  }

  @Test(enabled = false)
  @Override
  public void testGeneratedQueriesWithMultiValues() {
  }

  @Test(enabled = false)
  @Override
  public void testQueryExceptions() {
  }

  @Test(enabled = false)
  @Override
  public void testInstanceShutdown() {
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
