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
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.events.MinionEventObserver;
import org.apache.pinot.minion.events.MinionEventObserverFactory;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.minion.executor.BaseTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class SimpleMinionClusterIntegrationTest extends ClusterTest {
  private static final String TABLE_NAME_1 = "testTable1";
  private static final String TABLE_NAME_2 = "testTable2";
  private static final String TABLE_NAME_3 = "testTable3";
  private static final long STATE_TRANSITION_TIMEOUT_MS = 60_000L;  // 1 minute

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
    TableTaskConfig taskConfig =
        new TableTaskConfig(Collections.singletonMap(TestTaskGenerator.TASK_TYPE, Collections.emptyMap()));
    addTableConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_1).setTaskConfig(taskConfig).build());
    addTableConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_2).setTaskConfig(taskConfig).build());
    addTableConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_3).build());

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();

    // Register the test task generator into task manager
    _taskManager.registerTaskGenerator(new TestTaskGenerator(_taskManager.getClusterInfoAccessor()));

    Map<String, PinotTaskExecutorFactory> taskExecutorFactoryRegistry =
        Collections.singletonMap(TestTaskGenerator.TASK_TYPE, new TestTaskExecutorFactory());
    Map<String, MinionEventObserverFactory> eventObserverFactoryRegistry =
        Collections.singletonMap(TestTaskGenerator.TASK_TYPE, new TestEventObserverFactory());
    startMinion(taskExecutorFactoryRegistry, eventObserverFactoryRegistry);
  }

  @Test
  public void testStopResumeDeleteTaskQueue() {
    // Hold the task
    HOLD.set(true);

    // Should create the task queues and generate a task
    assertNotNull(_taskManager.scheduleTasks().get(TestTaskGenerator.TASK_TYPE));
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(TestTaskGenerator.TASK_TYPE)));

    // Should generate one more task
    assertNotNull(_taskManager.scheduleTask(TestTaskGenerator.TASK_TYPE));

    // Should not generate more tasks
    assertNull(_taskManager.scheduleTasks().get(TestTaskGenerator.TASK_TYPE));
    assertNull(_taskManager.scheduleTask(TestTaskGenerator.TASK_TYPE));

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
    }, STATE_TRANSITION_TIMEOUT_MS, "Failed to get all tasks IN_PROGRESS");

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
    }, STATE_TRANSITION_TIMEOUT_MS, "Failed to get all tasks STOPPED");

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
    }, STATE_TRANSITION_TIMEOUT_MS, "Failed to get all tasks COMPLETED");

    // Delete the task queue
    _helixTaskResourceManager.deleteTaskQueue(TestTaskGenerator.TASK_TYPE, false);

    // Wait at most 60 seconds for task queue to be deleted
    TestUtils.waitForCondition(input -> !_helixTaskResourceManager.getTaskTypes().contains(TestTaskGenerator.TASK_TYPE),
        STATE_TRANSITION_TIMEOUT_MS, "Failed to delete the task queue");
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

    private final ClusterInfoAccessor _clusterInfoAccessor;

    public TestTaskGenerator(ClusterInfoAccessor clusterInfoAccessor) {
      _clusterInfoAccessor = clusterInfoAccessor;
    }

    @Override
    public String getTaskType() {
      return TASK_TYPE;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
      assertEquals(tableConfigs.size(), 2);

      // Generate at most 2 tasks
      if (_clusterInfoAccessor.getTaskStates(TASK_TYPE).size() >= 2) {
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
  }

  public static class TestTaskExecutorFactory implements PinotTaskExecutorFactory {
    @Override
    public PinotTaskExecutor create() {
      return new BaseTaskExecutor() {
        @Override
        public Boolean executeTask(PinotTaskConfig pinotTaskConfig) {
          assertTrue(MINION_CONTEXT.getDataDir().exists());
          assertNotNull(MINION_CONTEXT.getMinionMetrics());
          assertNotNull(MINION_CONTEXT.getHelixPropertyStore());

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
        public void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
          TASK_START_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
          assertTrue(executionResult instanceof Boolean);
          assertTrue((Boolean) executionResult);
          TASK_SUCCESS_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
          TASK_CANCELLED_NOTIFIED.set(true);
        }

        @Override
        public void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception exception) {
          TASK_ERROR_NOTIFIED.set(true);
        }
      };
    }
  }
}
