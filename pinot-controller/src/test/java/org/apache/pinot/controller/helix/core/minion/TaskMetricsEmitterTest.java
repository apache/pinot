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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

    Mockito.when(_pinotHelixTaskResourceManager.getTaskMetadataLastUpdateTimeMs()).thenReturn(ImmutableMap.of());
    Mockito.when(leadControllerManager.isLeaderForTable("TaskMetricsEmitter")).thenReturn(true);
    Mockito.when(pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(ImmutableList.of());

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
        ImmutableMap.of(table1, taskCount1, table2, taskCount2));
    taskCount1 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount1.addTaskState(null);
    taskCount2 = new PinotHelixTaskResourceManager.TaskCount();
    taskCount2.addTaskState(TaskPartitionState.TASK_ERROR);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(task12)).thenReturn(
        ImmutableMap.of(table1, taskCount1, table2, taskCount2));

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
        .thenReturn(ImmutableMap.of(tableName, taskCount));
    taskCount = new PinotHelixTaskResourceManager.TaskCount();
    taskCount.addTaskState(null);
    Mockito.when(_pinotHelixTaskResourceManager.getTableTaskCount(taskName2))
        .thenReturn(ImmutableMap.of(tableName, taskCount));

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
}
