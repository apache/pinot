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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
  // Accessed by the plug-in classes
  public static final String TASK_TYPE = "TestTask";
  public static final String TABLE_NAME_1 = "testTable1";
  public static final String TABLE_NAME_2 = "testTable2";
  public static final String TABLE_NAME_3 = "testTable3";
  public static final int NUM_TASKS = 2;
  public static final int NUM_CONFIGS = 4;
  public static final AtomicBoolean HOLD = new AtomicBoolean();
  public static final AtomicBoolean TASK_START_NOTIFIED = new AtomicBoolean();
  public static final AtomicBoolean TASK_SUCCESS_NOTIFIED = new AtomicBoolean();
  public static final AtomicBoolean TASK_CANCELLED_NOTIFIED = new AtomicBoolean();
  public static final AtomicBoolean TASK_ERROR_NOTIFIED = new AtomicBoolean();

  private static final long STATE_TRANSITION_TIMEOUT_MS = 60_000L;  // 1 minute
  private static final long ZK_CALLBACK_TIMEOUT_MS = 30_000L;       // 30 seconds

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Set task timeout in cluster config
    PinotHelixResourceManager helixResourceManager = _controllerStarter.getHelixResourceManager();
    Map<String, String> properties = new HashMap<>();
    properties.put(TASK_TYPE + MinionConstants.TIMEOUT_MS_KEY_SUFFIX, Long.toString(600_000L));
    properties.put(TASK_TYPE + MinionConstants.MAX_ATTEMPTS_PER_TASK_KEY_SUFFIX, "2");

    helixResourceManager.getHelixAdmin().setConfig(
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
            .forCluster(helixResourceManager.getHelixClusterName()).build(), properties);

    // Add 3 offline tables, where 2 of them have TestTask enabled
    TableTaskConfig taskConfig = new TableTaskConfig(Collections.singletonMap(TASK_TYPE, Collections.emptyMap()));
    addTableConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_1).setTaskConfig(taskConfig).build());
    addTableConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_2).setTaskConfig(taskConfig).build());
    addTableConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_3).build());

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
  }

  @Test
  public void testTaskTimeout() {
    PinotTaskGenerator taskGenerator = _taskManager.getTaskGeneratorRegistry().getTaskGenerator(TASK_TYPE);
    assertNotNull(taskGenerator);
    assertEquals(taskGenerator.getTaskTimeoutMs(), 600_000L);
  }

  @Test
  public void testTaskMaxAttempts() {
    PinotTaskGenerator taskGenerator = _taskManager.getTaskGeneratorRegistry().getTaskGenerator(TASK_TYPE);
    assertNotNull(taskGenerator);
    assertEquals(taskGenerator.getMaxAttemptsPerTask(), 2);
  }

  private void verifyTaskCount(String task, int errors, int waiting, int running, int total) {
    // Wait for at most 10 seconds for Helix to generate the tasks
    TestUtils.waitForCondition((aVoid) -> {
      PinotHelixTaskResourceManager.TaskCount taskCount = _helixTaskResourceManager.getTaskCount(task);
      return taskCount.getError() == errors && taskCount.getWaiting() == waiting && taskCount.getRunning() == running
          && taskCount.getTotal() == total;
    }, 10_000L, "Failed to reach expected task count");
  }

  @Test
  public void testStopResumeDeleteTaskQueue() {
    // Hold the task
    HOLD.set(true);
    // No tasks before we start.
    assertEquals(_helixTaskResourceManager.getTasksInProgress(TASK_TYPE).size(), 0);
    verifyTaskCount("Task_" + TASK_TYPE + "_1624403781879", 0, 0, 0, 0);

    // Should create the task queues and generate a task
    String task1 = _taskManager.scheduleTasks().get(TASK_TYPE);
    assertNotNull(task1);
    assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(TASK_TYPE)));
    assertTrue(_helixTaskResourceManager.getTasksInProgress(TASK_TYPE).contains(task1));

    // Since we have two tables, two sub-tasks are generated -- one for each table.
    // The default concurrent sub-tasks per minion instance is 1, and we have one minion
    // instance spun up. So, one sub-tasks gets scheduled in a minion, and the other one
    // waits.
    verifyTaskCount(task1, 0, 1, 1, 2);
    // Should generate one more task, with two sub-tasks. Both of these sub-tasks will wait
    // since we have one minion instance that is still running one of the sub-tasks.
    String task2 = _taskManager.scheduleTask(TASK_TYPE);
    assertNotNull(task2);
    assertTrue(_helixTaskResourceManager.getTasksInProgress(TASK_TYPE).contains(task2));
    verifyTaskCount(task2, 0, 2, 0, 2);

    // Should not generate more tasks since SimpleMinionClusterIntegrationTests.NUM_TASKS is 2.
    // Our test task generator does not generate if there are already this many sub-tasks in the
    // running+waiting count already.
    assertNull(_taskManager.scheduleTasks().get(TASK_TYPE));
    assertNull(_taskManager.scheduleTask(TASK_TYPE));

    // Wait at most 60 seconds for all tasks IN_PROGRESS
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TASK_TYPE).values();
      assertEquals(taskStates.size(), NUM_TASKS);
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

    // Wait at most 30 seconds for ZK callback to update the controller gauges
    ControllerMetrics controllerMetrics = _controllerStarter.getControllerMetrics();
    String inProgressGauge = TASK_TYPE + "." + TaskState.IN_PROGRESS;
    String stoppedGauge = TASK_TYPE + "." + TaskState.STOPPED;
    String completedGauge = TASK_TYPE + "." + TaskState.COMPLETED;
    TestUtils.waitForCondition(
        input -> MetricValueUtils.getGlobalGaugeValue(controllerMetrics, inProgressGauge, ControllerGauge.TASK_STATUS)
            == NUM_TASKS
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, stoppedGauge, ControllerGauge.TASK_STATUS) == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, completedGauge, ControllerGauge.TASK_STATUS)
            == 0,
        ZK_CALLBACK_TIMEOUT_MS, "Failed to update the controller gauges");

    // Stop the task queue
    _helixTaskResourceManager.stopTaskQueue(TASK_TYPE);

    // Wait at most 60 seconds for all tasks STOPPED
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TASK_TYPE).values();
      assertEquals(taskStates.size(), NUM_TASKS);
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

    // Wait at most 30 seconds for ZK callback to update the controller gauges
    TestUtils.waitForCondition(
        input -> MetricValueUtils.getGlobalGaugeValue(controllerMetrics, inProgressGauge, ControllerGauge.TASK_STATUS)
            == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, stoppedGauge, ControllerGauge.TASK_STATUS)
            == NUM_TASKS
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, completedGauge, ControllerGauge.TASK_STATUS)
            == 0,
        ZK_CALLBACK_TIMEOUT_MS, "Failed to update the controller gauges");

    // Task deletion requires the task queue to be stopped,
    // so deleting task1 here before resuming the task queue.
    assertTrue(_helixTaskResourceManager.getTaskStates(TASK_TYPE).containsKey(task1));
    _helixTaskResourceManager.deleteTask(task1, false);
    // Resume the task queue, and let the task complete
    _helixTaskResourceManager.resumeTaskQueue(TASK_TYPE);
    HOLD.set(false);

    // Wait at most 60 seconds for all tasks COMPLETED
    TestUtils.waitForCondition(input -> {
      Collection<TaskState> taskStates = _helixTaskResourceManager.getTaskStates(TASK_TYPE).values();
      for (TaskState taskState : taskStates) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      // Task deletion happens eventually along with other state transitions.
      assertFalse(_helixTaskResourceManager.getTaskStates(TASK_TYPE).containsKey(task1));
      assertEquals(taskStates.size(), (NUM_TASKS - 1));
      assertTrue(TASK_START_NOTIFIED.get());
      assertTrue(TASK_SUCCESS_NOTIFIED.get());
      assertTrue(TASK_CANCELLED_NOTIFIED.get());
      assertFalse(TASK_ERROR_NOTIFIED.get());
      return true;
    }, STATE_TRANSITION_TIMEOUT_MS, "Failed to get all tasks COMPLETED");

    // Wait at most 30 seconds for ZK callback to update the controller gauges
    TestUtils.waitForCondition(
        input -> MetricValueUtils.getGlobalGaugeValue(controllerMetrics, inProgressGauge, ControllerGauge.TASK_STATUS)
            == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, stoppedGauge, ControllerGauge.TASK_STATUS) == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, completedGauge, ControllerGauge.TASK_STATUS)
            == (NUM_TASKS - 1),
        ZK_CALLBACK_TIMEOUT_MS, "Failed to update the controller gauges");

    // Delete the task queue
    _helixTaskResourceManager.deleteTaskQueue(TASK_TYPE, false);

    // Wait at most 60 seconds for task queue to be deleted
    TestUtils.waitForCondition(input -> !_helixTaskResourceManager.getTaskTypes().contains(TASK_TYPE),
        STATE_TRANSITION_TIMEOUT_MS, "Failed to delete the task queue");

    // Wait at most 30 seconds for ZK callback to update the controller gauges
    TestUtils.waitForCondition(
        input -> MetricValueUtils.getGlobalGaugeValue(controllerMetrics, inProgressGauge, ControllerGauge.TASK_STATUS)
            == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, stoppedGauge, ControllerGauge.TASK_STATUS) == 0
            && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, completedGauge, ControllerGauge.TASK_STATUS)
            == 0,
        ZK_CALLBACK_TIMEOUT_MS, "Failed to update the controller gauges");
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
}
