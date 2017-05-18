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

import com.clearspring.analytics.util.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.minion.exception.FatalException;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.BaseTaskExecutor;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MinionClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final String RAW_TABLE_NAME = "mytable";
  private static final String OFFLINE_TABLE_NAME = "mytable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "mytable_REALTIME";
  private static final int NUM_WORKERS = 3;

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private PinotTaskManager _pinotTaskManager;

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
    startMinions(NUM_WORKERS, taskExecutorsToRegister);
  }

  @Test
  public void testStopAndResumeTaskQueue()
      throws Exception {
    // Generate 4 tasks
    _pinotTaskManager.scheduleTasks();
    _pinotTaskManager.scheduleTasks();

    // Wait at most 60 seconds for all tasks showing up in the cluster
    long endTime = System.currentTimeMillis() + 60_000L;
    while ((System.currentTimeMillis() < endTime)
        && (_pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).size() != 4)) {
      Thread.sleep(100L);
    }
    Assert.assertEquals(_pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).size(), 4,
        "Not all tasks showed up within 60 seconds");

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
    endTime = System.currentTimeMillis() + 60_000L;
    int stoppedTaskCount = 0;
    while ((System.currentTimeMillis() < endTime) && (stoppedTaskCount != 4)) {
      Thread.sleep(100L);
      stoppedTaskCount = 0;
      taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
      for (TaskState taskState : taskStates.values()) {
        if (taskState == TaskState.STOPPED) {
          stoppedTaskCount++;
        }
      }
    }
    Assert.assertEquals(stoppedTaskCount, 4, "Not all tasks STOPPED within 60 seconds");

    // Resume the task queue
    _pinotHelixTaskResourceManager.resumeTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks COMPLETED
    endTime = System.currentTimeMillis() + 60_000L;
    int completedTaskCount = 0;
    while ((System.currentTimeMillis() < endTime) && (completedTaskCount != 4)) {
      Thread.sleep(100L);
      completedTaskCount = 0;
      taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
      for (TaskState taskState : taskStates.values()) {
        if (taskState == TaskState.COMPLETED) {
          completedTaskCount++;
        }
      }
    }
    Assert.assertEquals(completedTaskCount, 4, "Not all tasks COMPLETED within 60 seconds");

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
    public List<PinotTaskConfig> generateTasks(@Nonnull List<AbstractTableConfig> tableConfigs) {
      // Generate at most 4 tasks
      if (_clusterInfoProvider.getTaskStates(TASK_TYPE).size() < 4) {
        Map<String, String> config1 = new HashMap<>();
        config1.put("arg1", "foo1");
        config1.put("arg2", "bar1");
        Map<String, String> config2 = new HashMap<>();
        config2.put("arg1", "foo2");
        config2.put("arg2", "bar2");
        return Arrays.asList(new PinotTaskConfig(TASK_TYPE, config1), new PinotTaskConfig(TASK_TYPE, config2));
      } else {
        return Collections.emptyList();
      }
    }
  }

  public static class TestTaskExecutor extends BaseTaskExecutor {
    @Override
    public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
      try {
        Preconditions.checkArgument(pinotTaskConfig.getTaskType().equals(TestTaskGenerator.TASK_TYPE));
        Preconditions.checkArgument(pinotTaskConfig.getConfigs().size() == 2);
        Preconditions.checkArgument(pinotTaskConfig.getConfigs().containsKey("arg1"));
        Preconditions.checkArgument(pinotTaskConfig.getConfigs().containsKey("arg2"));
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
