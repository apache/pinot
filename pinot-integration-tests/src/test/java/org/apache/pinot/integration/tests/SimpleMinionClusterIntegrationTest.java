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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.config.PinotTaskConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TableTaskConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoProvider;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.minion.events.MinionEventObserver;
import org.apache.pinot.minion.events.MinionEventObserverFactory;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.minion.executor.BaseTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class SimpleMinionClusterIntegrationTest extends ClusterTest {
  private static final String TABLE_NAME_1 = "testTable1";
  private static final String TABLE_NAME_2 = "testTable2";
  private static final String TABLE_NAME_3 = "testTable3";
  private static final int NUM_MINIONS = 1;
  private static final AtomicBoolean HOLD = new AtomicBoolean();
  private static final AtomicBoolean TASK_START_NOTIFIED = new AtomicBoolean();
  private static final AtomicBoolean TASK_SUCCESS_NOTIFIED = new AtomicBoolean();
  private static final AtomicBoolean TASK_CANCELLED_NOTIFIED = new AtomicBoolean();
  private static final AtomicBoolean TASK_ERROR_NOTIFIED = new AtomicBoolean();

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();

    // Add 3 offline tables, where 2 of them have TestTask enabled
    TableTaskConfig taskConfig = new TableTaskConfig();
    Map<String, Map<String, String>> taskTypeConfigsMap = new HashMap<>();
    taskTypeConfigsMap.put(TestTaskGenerator.TASK_TYPE, Collections.emptyMap());
    taskConfig.setTaskTypeConfigsMap(taskTypeConfigsMap);
    addOfflineTable(TABLE_NAME_1, null, null, null, null, null, SegmentVersion.v1, null, null, taskConfig, null, null);
    addOfflineTable(TABLE_NAME_2, null, null, null, null, null, SegmentVersion.v1, null, null, taskConfig, null, null);
    addOfflineTable(TABLE_NAME_3, null, null, null, null, null, SegmentVersion.v1, null, null, null, null, null);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();

    // Register the test task generator into task manager
    _taskManager.registerTaskGenerator(new TestTaskGenerator(_taskManager.getClusterInfoProvider()));

    Map<String, PinotTaskExecutorFactory> taskExecutorFactoryRegistry =
        Collections.singletonMap(TestTaskGenerator.TASK_TYPE, new TestTaskExecutorFactory());
    Map<String, MinionEventObserverFactory> eventObserverFactoryRegistry =
        Collections.singletonMap(TestTaskGenerator.TASK_TYPE, new TestEventObserverFactory());
    startMinions(NUM_MINIONS, taskExecutorFactoryRegistry, eventObserverFactoryRegistry);
  }

  @Test
  public void testStopAndResumeTaskQueue()
      throws Exception {
    // Hold the task
    HOLD.set(true);

    // Should create the task queues and generate a task
    assertTrue(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(TestTaskGenerator.TASK_TYPE)));

    // Should generate one more task
    assertTrue(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));

    // Should not generate more tasks
    assertFalse(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));

    // Wait at most 60 seconds for all tasks IN_PROGRESS
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
      assertEquals(taskStates.size(), 2);
      for (TaskState taskState : taskStates) {
        if (taskState != TaskState.IN_PROGRESS) {
          return false;
        }
      }
      assertTrue(TASK_START_NOTIFIED.get());
      assertFalse(TASK_SUCCESS_NOTIFIED.get());
      assertFalse(TASK_CANCELLED_NOTIFIED.get());
      assertFalse(TASK_ERROR_NOTIFIED.get());
      return true;
    }, 60_000L, "Failed to get all tasks IN_PROGRESS");

    // Stop the task queue
    _helixTaskResourceManager.stopTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks STOPPED
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
      assertEquals(taskStates.size(), 2);
      for (TaskState taskState : taskStates) {
        if (taskState != TaskState.STOPPED) {
          return false;
        }
      }
      assertTrue(TASK_START_NOTIFIED.get());
      assertFalse(TASK_SUCCESS_NOTIFIED.get());
      assertTrue(TASK_CANCELLED_NOTIFIED.get());
      assertFalse(TASK_ERROR_NOTIFIED.get());
      return true;
    }, 60_000L, "Failed to get all tasks STOPPED");

    // Resume the task queue, and let the task complete
    _helixTaskResourceManager.resumeTaskQueue(TestTaskGenerator.TASK_TYPE);
    HOLD.set(false);

    // Wait at most 60 seconds for all tasks COMPLETED
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
      assertEquals(taskStates.size(), 2);
      for (TaskState taskState : taskStates) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      assertTrue(TASK_START_NOTIFIED.get());
      assertTrue(TASK_SUCCESS_NOTIFIED.get());
      assertTrue(TASK_CANCELLED_NOTIFIED.get());
      assertFalse(TASK_ERROR_NOTIFIED.get());
      return true;
    }, 60_000L, "Failed to get all tasks COMPLETED");

    // Delete the task queue
    // Note: Comment out the api for now since there is a known race condition
    // where helix controller might write a deleted workflow back to ZK because it's still caching it.
    // TODO: revert this after merging the fix
//    _helixTaskResourceManager.deleteTaskQueue(TestTaskGenerator.TASK_TYPE);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(TABLE_NAME_1);
    dropOfflineTable(TABLE_NAME_2);
    dropOfflineTable(TABLE_NAME_3);
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  private static class TestTaskGenerator implements PinotTaskGenerator {
    public static final String TASK_TYPE = "TestTask";

    private final ClusterInfoProvider _clusterInfoProvider;

    public TestTaskGenerator(ClusterInfoProvider clusterInfoProvider) {
      _clusterInfoProvider = clusterInfoProvider;
    }

    @Nonnull
    @Override
    public String getTaskType() {
      return TASK_TYPE;
    }

    @Nonnull
    @Override
    public List<PinotTaskConfig> generateTasks(@Nonnull List<TableConfig> tableConfigs) {
      assertEquals(tableConfigs.size(), 2);

      // Generate at most 2 tasks
      if (_clusterInfoProvider.getTaskStates(TASK_TYPE).size() >= 2) {
        return Collections.emptyList();
      }

      List<PinotTaskConfig> taskConfigs = new ArrayList<>();
      for (TableConfig tableConfig : tableConfigs) {
        Map<String, String> configs = new HashMap<>();
        configs.put("tableName", tableConfig.getTableName());
        configs.put("tableType", tableConfig.getTableType().toString());
        taskConfigs.add(new PinotTaskConfig(TASK_TYPE, configs));
      }
      return taskConfigs;
    }

    @Override
    public int getNumConcurrentTasksPerInstance() {
      return DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
    }

    @Override
    public void nonLeaderCleanUp() {
    }
  }

  public static class TestTaskExecutorFactory implements PinotTaskExecutorFactory {
    @Override
    public PinotTaskExecutor create() {
      return new BaseTaskExecutor() {
        @Override
        public Boolean executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
          assertTrue(MINION_CONTEXT.getDataDir().exists());
          assertNotNull(MINION_CONTEXT.getMinionMetrics());
          assertNotNull(MINION_CONTEXT.getMinionVersion());

          assertEquals(pinotTaskConfig.getTaskType(), TestTaskGenerator.TASK_TYPE);
          Map<String, String> configs = pinotTaskConfig.getConfigs();
          assertEquals(configs.size(), 2);
          String offlineTableName = configs.get("tableName");
          assertEquals(TableNameBuilder.getTableTypeFromTableName(offlineTableName), TableType.OFFLINE);
          String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
          assertTrue(rawTableName.equals(TABLE_NAME_1) || rawTableName.equals(TABLE_NAME_2));
          assertEquals(configs.get("tableType"), TableType.OFFLINE.toString());

          do {
            if (_cancelled) {
              throw new TaskCancelledException("Task has been cancelled");
            }
          } while (HOLD.get());
          return true;
        }
      };
    }
  }

  public static class TestEventObserverFactory implements MinionEventObserverFactory {

    @Override
    public MinionEventObserver create() {
      return new MinionEventObserver() {
        @Override
        public void notifyTaskStart(@Nonnull PinotTaskConfig pinotTaskConfig) {
          TASK_START_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskSuccess(@Nonnull PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
          assertTrue(executionResult instanceof Boolean);
          assertTrue((Boolean) executionResult);
          TASK_SUCCESS_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskCancelled(@Nonnull PinotTaskConfig pinotTaskConfig) {
          TASK_CANCELLED_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskError(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull Exception exception) {
          TASK_ERROR_NOTIFIED.set(true);
        }
      };
    }
  }
}
