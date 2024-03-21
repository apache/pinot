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
package org.apache.pinot.controller.helix.core.minion.generator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class TaskGeneratorUtilsTest {
  @Test
  public void testForRunningTasks() {
    String tableName = "mytable_OFFLINE";
    String taskType = "myTaskType";
    ClusterInfoAccessor mockClusterInfoAccessor = createMockClusterInfoAccessor();

    Map<String, TaskState> taskStatesMap = new HashMap<>();
    String taskID = System.currentTimeMillis() + "_0";
    when(mockClusterInfoAccessor.getTaskStates(taskType)).thenReturn(taskStatesMap);
    when(mockClusterInfoAccessor.getTaskConfigs(taskID))
        .thenReturn(Collections.singletonList(createTaskConfig(taskType, tableName, taskID)));

    int[] count = new int[1];
    TaskState[] nonFinalTaskStates = new TaskState[]{
        TaskState.NOT_STARTED, TaskState.IN_PROGRESS, TaskState.FAILING, TaskState.STOPPING, TaskState.STOPPED,
        TaskState.TIMING_OUT
    };
    for (TaskState taskState : nonFinalTaskStates) {
      taskStatesMap.put(taskID, taskState);
      TaskGeneratorUtils.forRunningTasks(tableName, taskType, mockClusterInfoAccessor, taskConfig -> {
        assertEquals(taskConfig.get(MinionConstants.TABLE_NAME_KEY), tableName);
        assertEquals(taskConfig.get("taskID"), taskID);
        count[0]++;
      });
    }
    assertEquals(count[0], nonFinalTaskStates.length);
    for (TaskState taskState : new TaskState[]{
        TaskState.COMPLETED, TaskState.FAILED, TaskState.ABORTED, TaskState.TIMED_OUT
    }) {
      taskStatesMap.put(taskID, taskState);
      TaskGeneratorUtils.forRunningTasks(tableName, taskType, mockClusterInfoAccessor, taskConfig -> {
        fail("Task should be in final state");
      });
    }
    TaskGeneratorUtils.forRunningTasks("fooTable", taskType, mockClusterInfoAccessor, taskConfig -> {
      fail("Different table name");
    });
    TaskGeneratorUtils.forRunningTasks(tableName, "fooTask", mockClusterInfoAccessor, taskConfig -> {
      fail("Different task type");
    });
  }

  private static PinotTaskConfig createTaskConfig(String taskType, String tableNameWithType, String taskID) {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_NAME_KEY, tableNameWithType);
    taskConfigs.put("taskID", taskID);
    return new PinotTaskConfig(taskType, taskConfigs);
  }

  private static ClusterInfoAccessor createMockClusterInfoAccessor() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPropertyStore.set(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
        .thenReturn(true);
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockHelixResourceManager.getPropertyStore()).thenReturn(mockPropertyStore);
    ClusterInfoAccessor mockClusterInfoAcessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAcessor.getVipUrl()).thenReturn("http://localhost:9000");
    when(mockClusterInfoAcessor.getPinotHelixResourceManager()).thenReturn(mockHelixResourceManager);
    return mockClusterInfoAcessor;
  }

  @Test
  public void testExtractMinionInstanceTag() {
    // correct minionInstanceTag extraction
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("100days.mergeType", "concat");
    tableTaskConfigs.put("100days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("100days.bucketTimePeriod", "100d");
    tableTaskConfigs.put("100days.maxNumRecordsPerSegment", "15000");
    tableTaskConfigs.put("100days.maxNumRecordsPerTask", "15000");
    tableTaskConfigs.put(PinotTaskManager.MINION_INSTANCE_TAG_CONFIG, "minionInstance1");
    TableTaskConfig tableTaskConfig =
        new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertEquals(TaskGeneratorUtils.extractMinionInstanceTag(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE), "minionInstance1");

    // no minionInstanceTag passed
    tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("100days.mergeType", "concat");
    tableTaskConfigs.put("100days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("100days.bucketTimePeriod", "100d");
    tableTaskConfigs.put("100days.maxNumRecordsPerSegment", "15000");
    tableTaskConfigs.put("100days.maxNumRecordsPerTask", "15000");
    tableTaskConfig =
        new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertEquals(TaskGeneratorUtils.extractMinionInstanceTag(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE), CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
  }
}
