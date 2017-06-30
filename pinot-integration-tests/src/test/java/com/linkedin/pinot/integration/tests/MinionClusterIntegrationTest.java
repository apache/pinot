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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.minion.exception.FatalException;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.BaseTaskExecutor;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.util.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends HybridClusterIntegrationTest and add minions into the cluster.
 */
public class MinionClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final String RAW_TABLE_NAME = "mytable";
  private static final String OFFLINE_TABLE_NAME = "mytable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "mytable_REALTIME";
  private static final int NUM_MINIONS = 3;

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private PinotTaskManager _pinotTaskManager;

  @Override
  protected TableTaskConfig getTaskConfig() {
    TableTaskConfig tableTaskConfig = new TableTaskConfig();
    tableTaskConfig.setEnabledTaskTypes(Collections.singleton(TestTaskGenerator.TASK_TYPE));
    return tableTaskConfig;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // The parent setUp() sets up Zookeeper, Kafka, controller, broker and servers
    super.setUp();

    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
    _pinotHelixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _pinotTaskManager = _controllerStarter.getTaskManager();

    // Register the test task generator into task manager
    _pinotTaskManager.registerTaskGenerator(new TestTaskGenerator(_pinotTaskManager.getClusterInfoProvider()));
    _pinotTaskManager.ensureTaskQueuesExist();

    Map<String, Class<? extends PinotTaskExecutor>> taskExecutorsToRegister = new HashMap<>(1);
    taskExecutorsToRegister.put(TestTaskGenerator.TASK_TYPE, TestTaskExecutor.class);
    startMinions(NUM_MINIONS, taskExecutorsToRegister);
  }

  @Test
  public void testStopAndResumeTaskQueue()
      throws Exception {
    // Generate 4 tasks
    _pinotTaskManager.scheduleTasks();
    _pinotTaskManager.scheduleTasks();

    // Wait at most 60 seconds for all tasks showing up in the cluster
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        return _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).size() == 4;
      }
    }, 60_000L, "Failed to get all tasks showing up in the cluster");

    // Should not generate more tasks
    _pinotTaskManager.scheduleTasks();

    // Check if all tasks IN_PROGRESS
    Map<String, TaskState> taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
    Assert.assertEquals(taskStates.size(), 4);
    for (TaskState taskState : taskStates.values()) {
      Assert.assertEquals(taskState, TaskState.IN_PROGRESS);
    }

    // Stop the task queue
    _pinotHelixTaskResourceManager.stopTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks STOPPED
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        for (TaskState taskState : _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values()) {
          if (taskState != TaskState.STOPPED) {
            return false;
          }
        }
        return true;
      }
    }, 60_000L, "Failed to get all tasks STOPPED");

    // Resume the task queue
    _pinotHelixTaskResourceManager.resumeTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks COMPLETED
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        for (TaskState taskState : _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).values()) {
          if (taskState != TaskState.COMPLETED) {
            return false;
          }
        }
        return true;
      }
    }, 60_000L, "Failed to get all tasks COMPLETED");

    // Delete the task queue
    _pinotHelixTaskResourceManager.deleteTaskQueue(TestTaskGenerator.TASK_TYPE);
  }

  @Test
  public void testPinotHelixResourceManagerAPIs() {
    // Instance APIs
    Assert.assertEquals(_pinotHelixResourceManager.getAllInstances().size(), 6);
    Assert.assertEquals(_pinotHelixResourceManager.getOnlineInstanceList().size(), 6);
    Assert.assertEquals(_pinotHelixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), 0);
    Assert.assertEquals(_pinotHelixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 0);

    // Table APIs
    List<String> tableNames = _pinotHelixResourceManager.getAllTables();
    Assert.assertEquals(tableNames.size(), 2);
    Assert.assertTrue(tableNames.contains(OFFLINE_TABLE_NAME));
    Assert.assertTrue(tableNames.contains(REALTIME_TABLE_NAME));
    Assert.assertEquals(_pinotHelixResourceManager.getAllRawTables(), Collections.singletonList(RAW_TABLE_NAME));
    Assert.assertEquals(_pinotHelixResourceManager.getAllRealtimeTables(),
        Collections.singletonList(REALTIME_TABLE_NAME));

    // Tenant APIs
    Assert.assertEquals(_pinotHelixResourceManager.getAllBrokerTenantNames(), Collections.singleton("TestTenant"));
    Assert.assertEquals(_pinotHelixResourceManager.getAllServerTenantNames(), Collections.singleton("TestTenant"));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopMinion();

    super.tearDown();
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
      // Generate at most 4 tasks
      if (_clusterInfoProvider.getTaskStates(TASK_TYPE).size() >= 4) {
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
    @Override
    public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
      try {
        Preconditions.checkState(_minionContext.getDataDir().exists());
        Preconditions.checkNotNull(_minionContext.getMinionMetrics());

        Preconditions.checkArgument(pinotTaskConfig.getTaskType().equals(TestTaskGenerator.TASK_TYPE));
        Map<String, String> configs = pinotTaskConfig.getConfigs();
        Preconditions.checkArgument(configs.size() == 2);
        Preconditions.checkArgument(configs.containsKey("tableName"));
        Preconditions.checkArgument(configs.containsKey("tableType"));
        switch (configs.get("tableName")) {
          case OFFLINE_TABLE_NAME:
            Preconditions.checkArgument(configs.get("tableType").equals(TableType.OFFLINE.toString()));
            break;
          case REALTIME_TABLE_NAME:
            Preconditions.checkArgument(configs.get("tableType").equals(TableType.REALTIME.toString()));
            break;
          default:
            throw new IllegalArgumentException("Got unexpected table name: " + configs.get("tableName"));
        }
      } catch (IllegalArgumentException e) {
        throw new FatalException("Got unexpected task config: " + pinotTaskConfig, e);
      }

      Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

      if (_cancelled) {
        throw new TaskCancelledException("Task has been cancelled");
      }
    }
  }
}
