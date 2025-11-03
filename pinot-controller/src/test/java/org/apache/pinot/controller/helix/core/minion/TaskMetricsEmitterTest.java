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
package org.apache.pinot.controller.helix.core.minion;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.task.TaskPartitionState;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.plugin.metrics.yammer.YammerSettableGauge;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class TaskMetricsEmitterTest {
  private TaskMetricsEmitter _taskMetricsEmitter;
  private ControllerMetrics _controllerMetrics;
  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @BeforeMethod
  public void setUp() {
    // initialize PinotMetrics
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    _controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    _pinotHelixTaskResourceManager = Mockito.mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    LeadControllerManager leadControllerManager = Mockito.mock(LeadControllerManager.class);

    Mockito.when(_pinotHelixTaskResourceManager.getTaskMetadataLastUpdateTimeMs()).thenReturn(Map.of());
    Mockito.when(leadControllerManager.isLeaderForTable("TaskMetricsEmitter")).thenReturn(true);
    Mockito.when(pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(List.of());

    _taskMetricsEmitter = new TaskMetricsEmitter(pinotHelixResourceManager,
        _pinotHelixTaskResourceManager, leadControllerManager, new ControllerConf(), _controllerMetrics);
  }

  @Test
  public void noTaskTypeMetrics() {
    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of());
    _taskMetricsEmitter.runTask(null);
    Assert.assertEquals(metricsRegistry.allMetrics().size(), 1);
    Assert.assertTrue(metricsRegistry.allMetrics().containsKey(
        new YammerMetricName(ControllerMetrics.class, "pinot.controller.onlineMinionInstances")));
  }

  @Test
  public void taskType1ButNoInProgressTask() {
    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    String taskType = "taskType1";
    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of(taskType));
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType)).thenReturn(ImmutableSet.of());
    _taskMetricsEmitter.runTask(null);

    Assert.assertEquals(metricsRegistry.allMetrics().size(), 11);
    Assert.assertTrue(metricsRegistry.allMetrics().containsKey(
        new YammerMetricName(ControllerMetrics.class, "pinot.controller.onlineMinionInstances")));
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionTasksInProgress.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksRunning.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksWaiting.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksError.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksUnknown.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksDropped.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksTimedOut.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksAborted.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.percentMinionSubtasksInQueue.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.percentMinionSubtasksInError.taskType1"))
        .getMetric()).value(), 0L);
  }

  @Test
  public void oneSingleTaskTypeWithTwoTables() {
    String taskType = "taskType1";
    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of(taskType));
    String task11 = "task11";
    String task12 = "task12";
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of(task11, task12));

    String table1 = "table1_OFFLINE";
    String table2 = "table2_OFFLINE";
    PinotHelixTaskResourceManager.TaskCount taskCount1 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount1.addTaskState(TaskPartitionState.COMPLETED);
    PinotHelixTaskResourceManager.TaskCount taskCount2 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount2.addTaskState(TaskPartitionState.RUNNING);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(task11)).thenReturn(
        Map.of(table1, taskCount1, table2, taskCount2));
    taskCount1 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount1.addTaskState(null);
    taskCount2 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount2.addTaskState(TaskPartitionState.TASK_ERROR);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(task12)).thenReturn(
        Map.of(table1, taskCount1, table2, taskCount2));

    runAndAssertForTaskType1WithTwoTables();
  }

  @Test
  public void taskType1WithTwoTablesEmitMetricTwice() {
    oneSingleTaskTypeWithTwoTables();
    // the second run does not change anything
    runAndAssertForTaskType1WithTwoTables();
  }

  private void runAndAssertForTaskType1WithTwoTables() {
    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    _taskMetricsEmitter.runTask(null);
    Assert.assertEquals(metricsRegistry.allMetrics().size(), 29);

    Assert.assertTrue(metricsRegistry.allMetrics().containsKey(
        new YammerMetricName(ControllerMetrics.class, "pinot.controller.onlineMinionInstances")));
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionTasksInProgress.taskType1"))
        .getMetric()).value(), 2L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksRunning.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksWaiting.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksError.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.numMinionSubtasksDropped.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.percentMinionSubtasksInQueue.taskType1"))
        .getMetric()).value(), 50L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class, "pinot.controller.percentMinionSubtasksInError.taskType1"))
        .getMetric()).value(), 25L);

    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksRunning.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksWaiting.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksDropped.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.percentMinionSubtasksInQueue.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 50L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.percentMinionSubtasksInError.table1_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);

    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksRunning.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksWaiting.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksDropped.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.percentMinionSubtasksInQueue.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 50L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.percentMinionSubtasksInError.table2_OFFLINE.taskType1"))
        .getMetric()).value(), 50L);
  }

  @Test
  public void taskType2WithOneTable() {
    oneTaskTypeWithOneTable("taskType2", "task21", "task22", "table3_OFFLINE");
  }

  private void oneTaskTypeWithOneTable(String taskType, String taskName1, String taskName2, String tableName) {
    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of(taskType));
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of(taskName1, taskName2));

    PinotHelixTaskResourceManager.TaskCount taskCount = new PinotHelixTaskResourceManager.TaskCount();
    taskCount.addTaskState(TaskPartitionState.COMPLETED);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(taskName1))
        .thenReturn(Map.of(tableName, taskCount));
    taskCount = new PinotHelixTaskResourceManager.TaskCount();
    taskCount.addTaskState(null);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(taskName2))
        .thenReturn(Map.of(tableName, taskCount));

    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    _taskMetricsEmitter.runTask(null);
    Assert.assertEquals(metricsRegistry.allMetrics().size(), 20);

    Assert.assertTrue(metricsRegistry.allMetrics().containsKey(
        new YammerMetricName(ControllerMetrics.class, "pinot.controller.onlineMinionInstances")));
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionTasksInProgress.%s", taskType)))
        .getMetric()).value(), 2L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksRunning.%s", taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksWaiting.%s", taskType)))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksError.%s", taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksDropped.%s", taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.percentMinionSubtasksInQueue.%s", taskType)))
        .getMetric()).value(), 50L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.percentMinionSubtasksInError.%s", taskType)))
        .getMetric()).value(), 0L);

    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksRunning.%s.%s", tableName, taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksWaiting.%s.%s", tableName, taskType)))
        .getMetric()).value(), 1L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksError.%s.%s", tableName, taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.numMinionSubtasksDropped.%s.%s", tableName, taskType)))
        .getMetric()).value(), 0L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.percentMinionSubtasksInQueue.%s.%s", tableName, taskType)))
        .getMetric()).value(), 50L);
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                String.format("pinot.controller.percentMinionSubtasksInError.%s.%s", tableName, taskType)))
        .getMetric()).value(), 0L);
  }

  @Test
  public void removeOneTableFromMinionTasks() {
    oneSingleTaskTypeWithTwoTables();
    oneTaskTypeWithOneTable("taskType1", "task11", "task12", "table1_OFFLINE");
  }

  @Test
  public void addOneTableToMinionTasks() {
    oneTaskTypeWithOneTable("taskType1", "task11", "task12", "table1_OFFLINE");
    oneSingleTaskTypeWithTwoTables();
  }

  @Test
  public void removeTheTaskTypeFromMinionTasks() {
    oneSingleTaskTypeWithTwoTables();
    noTaskTypeMetrics();
  }

  @Test
  public void removeOldTaskTypeAddNewTaskType() {
    oneSingleTaskTypeWithTwoTables();
    taskType2WithOneTable();
  }

  /**
   * Test for previously in-progress tasks that completed between runs:
   * Tasks that were in-progress in the previous run but completed before the current run
   * should still have their metrics reported in the current run.
   *
   * Scenario:
   * - Run 1: Task "taskCompletedBetweenRuns" is in-progress with 1 error subtask
   * - Run 2: Task "taskCompletedBetweenRuns" has completed and is no longer in getTasksInProgress()
   *
   * Expected: Metrics for "taskCompletedBetweenRuns" should still be emitted in Run 2 by detecting it via
   * _previousInProgressTasks tracking. The emitter maintains state of tasks that were in-progress
   * in the previous execution cycle and includes completed tasks in the current cycle's metrics.
   */
  @Test
  public void testReportsPreviouslyInProgressTasksThatCompletedBetweenRuns() {
    String taskType = "SegmentGenerationAndPushTask";
    String taskName = "taskCompletedBetweenRuns";
    String tableName = "testTable_OFFLINE";

    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of(taskType));

    // Run 1: Task is in-progress with 1 error subtask
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of(taskName));

    // Ensure getTasksStartedAfter returns empty for this test (not relevant for this scenario)
    Mockito.when(_pinotHelixTaskResourceManager.getTasksStartedAfter(
        Mockito.eq(taskType), Mockito.anyLong()))
        .thenReturn(ImmutableSet.of());

    PinotHelixTaskResourceManager.TaskCount taskCount = new PinotHelixTaskResourceManager.TaskCount();
    taskCount.addTaskState(TaskPartitionState.TASK_ERROR);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(taskName))
        .thenReturn(Map.of(tableName, taskCount));

    _taskMetricsEmitter.runTask(null);

    // Verify metrics were emitted in Run 1
    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError." + taskType))
        .getMetric()).value(), 1L);

    // Run 2: Task has completed and is no longer in-progress
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of());  // Empty - task completed

    // The emitter should detect that taskCompletedBetweenRuns was in-progress before and include it in metrics
    // This is achieved by comparing _previousInProgressTasks with currentInProgressTasks
    _taskMetricsEmitter.runTask(null);

    // Expected: Metrics for the completed task should still be reported
    // The emitter tracks tasks that were in-progress in the previous cycle and includes
    // them in the current cycle even if they've completed, ensuring final metrics are captured
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError." + taskType))
        .getMetric()).value(), 1L,
        "Previously in-progress task that completed between runs should still be reported");
  }

  /**
   * Test for short-lived tasks that started and completed between runs:
   * Tasks that started AND completed between two collection runs should have their
   * metrics reported.
   *
   * Scenario:
   * - Run 1: No tasks in-progress
   * - Between runs: Task "taskShortLived" starts and completes (very short-lived)
   * - Run 2: No tasks in-progress (taskShortLived already completed)
   *
   * Expected: Metrics for "taskShortLived" should be emitted in Run 2 by detecting it via
   * getTasksStartedAfter() which uses WorkflowContext.getJobStartTimes() to find tasks that
   * started after the previous execution timestamp. The emitter filters out tasks that are
   * currently in-progress or were tracked in the previous cycle to avoid duplicates.
   */
  @Test
  public void testReportsTasksThatStartAndCompleteBetweenRuns() {
    String taskType = "SegmentGenerationAndPushTask";
    String taskName = "taskShortLived";
    String tableName = "testTable_OFFLINE";

    Mockito.when(_pinotHelixTaskResourceManager.getTaskTypes()).thenReturn(ImmutableSet.of(taskType));

    // Run 1: No tasks in-progress
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of());

    // Run 1: No tasks started after initial timestamp (empty on first run)
    Mockito.when(_pinotHelixTaskResourceManager.getTasksStartedAfter(
        Mockito.eq(taskType), Mockito.anyLong()))
        .thenReturn(ImmutableSet.of());

    _taskMetricsEmitter.runTask(null);

    // Verify no error metrics in Run 1
    PinotMetricsRegistry metricsRegistry = _controllerMetrics.getMetricsRegistry();
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError." + taskType))
        .getMetric()).value(), 0L);

    // Between Run 1 and Run 2: taskShortLived starts and completes with 1 error
    // This task has a job start time after Run 1's execution timestamp
    // The implementation uses getTasksStartedAfter() which internally calls
    // WorkflowContext.getJobStartTimes() to detect such tasks
    PinotHelixTaskResourceManager.TaskCount taskCount = new PinotHelixTaskResourceManager.TaskCount();
    taskCount.addTaskState(TaskPartitionState.TASK_ERROR);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(taskName))
        .thenReturn(Map.of(tableName, taskCount));

    // Run 2: Still no tasks in-progress (taskShortLived already completed)
    Mockito.when(_pinotHelixTaskResourceManager.getTasksInProgress(taskType))
        .thenReturn(ImmutableSet.of());

    // Mock getTasksStartedAfter to return taskShortLived (simulating it started after Run 1's timestamp)
    // The implementation filters this to ensure it's not currently in-progress and wasn't
    // in the previous cycle, then includes it in metrics collection
    Mockito.when(_pinotHelixTaskResourceManager.getTasksStartedAfter(
        Mockito.eq(taskType), Mockito.anyLong()))
        .thenReturn(ImmutableSet.of(taskName));

    _taskMetricsEmitter.runTask(null);

    // Expected: Metrics for taskShortLived should be reported by detecting it via getTasksStartedAfter()
    // The emitter:
    // 1. Calls getTasksStartedAfter(taskType, previousExecutionTimestamp) which uses
    //    WorkflowContext.getJobStartTimes() internally
    // 2. Filters out tasks that are currently in-progress or were in the previous cycle
    // 3. Includes remaining tasks (short-lived tasks) in metrics collection
    Assert.assertEquals(((YammerSettableGauge<?>) metricsRegistry.allMetrics().get(
            new YammerMetricName(ControllerMetrics.class,
                "pinot.controller.numMinionSubtasksError." + taskType))
        .getMetric()).value(), 1L, "Short-lived task that started and completed between runs should be reported");
  }
}
