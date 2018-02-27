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
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.BaseTaskExecutor;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.util.TestUtils;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class SimpleMinionClusterIntegrationTest extends ClusterTest {
  private static final String TABLE_NAME_1 = "testTable1";
  private static final String TABLE_NAME_2 = "testTable2";
  private static final String TABLE_NAME_3 = "testTable3";
  private static final int NUM_MINIONS = 1;

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();

    // Add 3 offline tables, where 2 of them have TestTask enabled
    TableTaskConfig taskConfig = new TableTaskConfig();
    Map<String, Map<String, String>> taskTypeConfigsMap = new HashMap<>();
    taskTypeConfigsMap.put(TestTaskGenerator.TASK_TYPE, Collections.<String, String>emptyMap());
    taskConfig.setTaskTypeConfigsMap(taskTypeConfigsMap);
    addOfflineTable(TABLE_NAME_1, null, null, null, null, null, SegmentVersion.v1, null, taskConfig);
    addOfflineTable(TABLE_NAME_2, null, null, null, null, null, SegmentVersion.v1, null, taskConfig);
    addOfflineTable(TABLE_NAME_3, null, null, null, null, null, SegmentVersion.v1, null, null);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();

    // Register the test task generator into task manager
    _taskManager.registerTaskGenerator(new TestTaskGenerator(_taskManager.getClusterInfoProvider()));

    Map<String, Class<? extends PinotTaskExecutor>> taskExecutorsToRegister = new HashMap<>(1);
    taskExecutorsToRegister.put(TestTaskGenerator.TASK_TYPE, TestTaskExecutor.class);
    startMinions(NUM_MINIONS, taskExecutorsToRegister);
  }

  @Test
  public void testStopAndResumeTaskQueue() throws Exception {
    // Hold the task
    TestTaskExecutor.HOLD.set(true);

    // Should create the task queues and generate a task
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));
    Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(TestTaskGenerator.TASK_TYPE)));

    // Should generate one more task
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));

    // Should not generate more tasks
    Assert.assertFalse(_taskManager.scheduleTasks().containsKey(TestTaskGenerator.TASK_TYPE));

    // Wait at most 60 seconds for all tasks IN_PROGRESS
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@Nullable Void input) {
        Collection<TaskState> taskStates =
            _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
        Assert.assertEquals(taskStates.size(), 2);
        for (TaskState taskState : taskStates) {
          if (taskState != TaskState.IN_PROGRESS) {
            return false;
          }
        }
        return true;
      }
    }, 60_000L, "Failed to get all tasks IN_PROGRESS");

    // Stop the task queue
    _helixTaskResourceManager.stopTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks STOPPED
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        Collection<TaskState> taskStates =
            _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
        Assert.assertEquals(taskStates.size(), 2);
        for (TaskState taskState : taskStates) {
          if (taskState != TaskState.STOPPED) {
            return false;
          }
        }
        return true;
      }
    }, 60_000L, "Failed to get all tasks STOPPED");

    // Resume the task queue, and let the task complete
    _helixTaskResourceManager.resumeTaskQueue(TestTaskGenerator.TASK_TYPE);
    TestTaskExecutor.HOLD.set(false);

    // Wait at most 60 seconds for all tasks COMPLETED
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        Collection<TaskState> taskStates =
            _helixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values();
        Assert.assertEquals(taskStates.size(), 2);
        for (TaskState taskState : taskStates) {
          if (taskState != TaskState.COMPLETED) {
            return false;
          }
        }
        return true;
      }
    }, 60_000L, "Failed to get all tasks COMPLETED");

    // Delete the task queue
    _helixTaskResourceManager.deleteTaskQueue(TestTaskGenerator.TASK_TYPE);
  }

  @AfterClass
  public void tearDown() throws Exception {
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
      Assert.assertEquals(tableConfigs.size(), 2);

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
  }

  public static class TestTaskExecutor extends BaseTaskExecutor {
    private static final AtomicBoolean HOLD = new AtomicBoolean();

    @Override
    public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
      Assert.assertTrue(MINION_CONTEXT.getDataDir().exists());
      Assert.assertNotNull(MINION_CONTEXT.getMinionMetrics());
      Assert.assertNotNull(MINION_CONTEXT.getMinionVersion());

      Assert.assertTrue(pinotTaskConfig.getTaskType().equals(TestTaskGenerator.TASK_TYPE));
      Map<String, String> configs = pinotTaskConfig.getConfigs();
      Assert.assertTrue(configs.size() == 2);
      String offlineTableName = configs.get("tableName");
      Assert.assertEquals(TableNameBuilder.getTableTypeFromTableName(offlineTableName), TableType.OFFLINE);
      String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
      Assert.assertTrue(rawTableName.equals(TABLE_NAME_1) || rawTableName.equals(TABLE_NAME_2));
      Assert.assertEquals(configs.get("tableType"), TableType.OFFLINE.toString());

      do {
        if (_cancelled) {
          throw new TaskCancelledException("Task has been cancelled");
        }
      } while (HOLD.get());
    }
  }
}
