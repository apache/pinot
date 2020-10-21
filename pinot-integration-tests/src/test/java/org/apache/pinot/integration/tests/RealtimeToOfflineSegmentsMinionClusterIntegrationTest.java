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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for minion task of type "RealtimeToOfflineSegmentsTask"
 * With every task run, a new segment is created in the offline table for 1 day. Watermark also keeps progressing accordingly.
 */
public class RealtimeToOfflineSegmentsMinionClusterIntegrationTest extends RealtimeClusterIntegrationTest {

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;
  private PinotHelixResourceManager _pinotHelixResourceManager;

  private long _dataSmallestTimeMillis;
  private long _dateSmallestDays;
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
    startMinion(null, null);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    _offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    List<RealtimeSegmentZKMetadata> realtimeSegmentMetadata =
        _pinotHelixResourceManager.getRealtimeSegmentMetadata(_realtimeTableName);
    long minSegmentTime = Long.MAX_VALUE;
    for (RealtimeSegmentZKMetadata metadata : realtimeSegmentMetadata) {
      if (metadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
        if (metadata.getStartTime() < minSegmentTime) {
          minSegmentTime = metadata.getStartTime();
        }
      }
    }
    _dataSmallestTimeMillis = minSegmentTime;
    _dateSmallestDays = minSegmentTime / 86400000;
  }

  @Test
  public void testRealtimeToOfflineSegmentsTask() {

    List<OfflineSegmentZKMetadata> offlineSegmentMetadata =
        _pinotHelixResourceManager.getOfflineSegmentMetadata(_offlineTableName);
    Assert.assertTrue(offlineSegmentMetadata.isEmpty());

    long expectedWatermark = _dataSmallestTimeMillis;
    int numOfflineSegments = 0;
    long offlineSegmentTime = _dateSmallestDays;
    for (int i = 0; i < 3; i++) {
      // Schedule task
      Assert.assertTrue(
          _taskManager.scheduleTasks().containsKey(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      Assert.assertTrue(_helixTaskResourceManager.getTaskQueues().contains(
          PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE)));
      // Should not generate more tasks
      Assert.assertFalse(
          _taskManager.scheduleTasks().containsKey(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));

      expectedWatermark = expectedWatermark + 86400000;
      // Wait at most 600 seconds for all tasks COMPLETED
      waitForTaskToComplete(expectedWatermark);
      // check segment is in offline
      offlineSegmentMetadata = _pinotHelixResourceManager.getOfflineSegmentMetadata(_offlineTableName);
      Assert.assertEquals(offlineSegmentMetadata.size(), ++numOfflineSegments);
      Assert.assertEquals(offlineSegmentMetadata.get(i).getStartTime(), offlineSegmentTime);
      Assert.assertEquals(offlineSegmentMetadata.get(i).getEndTime(), offlineSegmentTime);
      offlineSegmentTime++;
    }
    testHardcodedSqlQueries();
  }

  private void waitForTaskToComplete(long expectedWatermark) {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager
          .getTaskStates(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");

    // Check segment ZK metadata
    RealtimeToOfflineSegmentsTaskMetadata minionTaskMetadata =
        _taskManager.getClusterInfoAccessor().getMinionRealtimeToOfflineSegmentsTaskMetadata(_realtimeTableName);
    Assert.assertNotNull(minionTaskMetadata);
    Assert.assertEquals(minionTaskMetadata.getWatermarkMs(), expectedWatermark);
  }

  @Test(enabled = false)
  public void testSegmentListApi() {
  }

  @Test(enabled = false)
  public void testBrokerDebugOutput() {
  }

  @Test(enabled = false)
  public void testBrokerDebugRoutingTableSQL() {
  }

  @Test(enabled = false)
  public void testBrokerResponseMetadata() {
  }

  @Test(enabled = false)
  public void testDictionaryBasedQueries() {
  }

  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues() {
  }

  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues() {
  }

  @Test(enabled = false)
  public void testHardcodedQueries() {
  }

  @Test(enabled = false)
  public void testHardcodedSqlQueries() {
  }

  @Test(enabled = false)
  public void testInstanceShutdown() {
  }

  @Test(enabled = false)
  public void testQueriesFromQueryFile() {
  }

  @Test(enabled = false)
  public void testQueryExceptions() {
  }

  @Test(enabled = false)
  public void testReload(boolean includeOfflineTable) {
  }

  @Test(enabled = false)
  public void testSqlQueriesFromQueryFile() {
  }

  @Test(enabled = false)
  public void testVirtualColumnQueries() {
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopMinion();

    super.tearDown();
  }
}
