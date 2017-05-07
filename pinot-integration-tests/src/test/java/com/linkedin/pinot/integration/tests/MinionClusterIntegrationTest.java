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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.minion.MinionStarter;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.BaseTaskExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MinionClusterIntegrationTest {
  private static final int ZK_PORT = 2191;
  private static final String ZK_ADDRESS = "localhost:" + ZK_PORT;
  private static final String HELIX_CLUSTER_NAME = "pinot-minion-test";
  private static final String CONTROLLER_ID = "controller";
  private static final int NUM_WORKERS = 3;

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private PinotTaskManager _pinotTaskManager;
  private MinionStarter[] _minionStarters = new MinionStarter[NUM_WORKERS];

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start a ZooKeeper instance.
    _zookeeperInstance = ZkStarter.startLocalZkServer(ZK_PORT);

    // Start a PinotHelixResourceManager
    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_ADDRESS, HELIX_CLUSTER_NAME, CONTROLLER_ID, null);
    _pinotHelixResourceManager.start();

    // Initialize a PinotTaskManager
    TaskDriver taskDriver = new TaskDriver(_pinotHelixResourceManager.getHelixZkManager());
    _pinotHelixTaskResourceManager = new PinotHelixTaskResourceManager(taskDriver);
    _pinotTaskManager = new PinotTaskManager(taskDriver, _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
    _pinotTaskManager.registerTaskGenerator(new TestTaskGenerator(_pinotTaskManager.getClusterInfoProvider()));
    _pinotTaskManager.ensureTaskQueuesExist();

    // Initialize minion starters.
    for (int i = 0; i < NUM_WORKERS; i++) {
      Configuration config = new PropertiesConfiguration();
      config.setProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY, "minion" + i);
      MinionStarter minionStarter = new MinionStarter(ZK_ADDRESS, HELIX_CLUSTER_NAME, config);
      minionStarter.registerTaskExecutorClass(TestTaskGenerator.TASK_TYPE, TestTaskExecutor.class);
      minionStarter.start();
      _minionStarters[i] = minionStarter;
    }
  }

  @Test
  public void testStopAndResumeJobQueue()
      throws Exception {
    // Generate 4 tasks
    _pinotTaskManager.execute();
    _pinotTaskManager.execute();

    // Wait at most 60 seconds for all tasks showing up in the cluster
    long endTime = System.currentTimeMillis() + 60_000L;
    while ((System.currentTimeMillis() < endTime) && (
        _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).size() != 4)) {
      Thread.sleep(100L);
    }
    Assert.assertEquals(_pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE).size(), 4);

    // Should not generate more tasks
    _pinotTaskManager.execute();

    // Check if all tasks IN_PROGRESS
    Map<String, TaskState> taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
    Assert.assertEquals(taskStates.size(), 4);
    for (TaskState taskState : taskStates.values()) {
      Assert.assertTrue(taskState.equals(TaskState.IN_PROGRESS));
    }

    // Stop the job queue
    _pinotHelixTaskResourceManager.stopTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks stopped
    endTime = System.currentTimeMillis() + 60_000L;
    int stoppedTaskCount = 0;
    while ((System.currentTimeMillis() < endTime) && (stoppedTaskCount != 4)) {
      Thread.sleep(100L);
      stoppedTaskCount = 0;
      taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
      for (TaskState taskState : taskStates.values()) {
        if (taskState.equals(TaskState.STOPPED)) {
          stoppedTaskCount++;
        }
      }
    }
    Assert.assertEquals(stoppedTaskCount, 4);

    // Resume the job queue
    _pinotHelixTaskResourceManager.resumeTaskQueue(TestTaskGenerator.TASK_TYPE);

    // Wait at most 60 seconds for all tasks completed
    int completedTaskCount = 0;
    while (completedTaskCount != 4) {
      Thread.sleep(100L);
      completedTaskCount = 0;
      taskStates = _pinotHelixTaskResourceManager.getTaskStates(TestTaskGenerator.TASK_TYPE);
      for (TaskState taskState : taskStates.values()) {
        if (taskState.equals(TaskState.COMPLETED)) {
          completedTaskCount++;
        }
      }
    }
    Assert.assertEquals(completedTaskCount, 4);

    // Delete the job queue
    _pinotHelixTaskResourceManager.deleteTaskQueue(TestTaskGenerator.TASK_TYPE);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    for (MinionStarter minionStarter : _minionStarters) {
      minionStarter.stop();
    }
    _pinotHelixResourceManager.stop();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
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
      Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

      if (_cancelled) {
        throw new TaskCancelledException("Task has been cancelled");
      }
    }
  }
}
