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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.TaskGeneratorMostRecentRunInfo;
import org.apache.pinot.common.minion.TaskManagerStatusCache;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.validation.ResourceUtilizationManager;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class PinotTaskManagerOnChangeTest {

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    _helixTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setProperty("controller.task.frequencyPeriod", "1h");
    controllerConf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_SIZE, -1);
    controllerConf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, 50);
    controllerConf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_WARNING_THRESHOLD, 5000);

    ControllerMetrics controllerMetrics = new ControllerMetrics(new NoopPinotMetricsRegistry());
    TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> statusCache = mock(TaskManagerStatusCache.class);
    Executor executor = mock(Executor.class);
    PoolingHttpClientConnectionManager connectionManager = mock(PoolingHttpClientConnectionManager.class);
    ResourceUtilizationManager resourceUtilizationManager = mock(ResourceUtilizationManager.class);

    _taskManager = new PinotTaskManager(
        _helixTaskResourceManager, helixResourceManager, leadControllerManager,
        controllerConf, controllerMetrics, statusCache, executor, connectionManager,
        resourceUtilizationManager);
  }

  @Test
  public void testOnChangeNullClusterConfigs() {
    _taskManager.onChange(new HashSet<>(), null);
    verifyNoInteractions(_helixTaskResourceManager);
  }

  @Test
  public void testOnChangeNullChangedConfigs() {
    _taskManager.onChange(null, Collections.emptyMap());
    verifyNoInteractions(_helixTaskResourceManager);
  }

  @Test
  public void testOnChangeEmptyClusterConfigs() {
    _taskManager.onChange(new HashSet<>(), Collections.emptyMap());
    verifyNoMoreInteractions(_helixTaskResourceManager);
  }

  @Test
  public void testOnChangeValidTaskExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "86400000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager).setTaskExpireTimeMs(86400000L);
  }

  @Test
  public void testOnChangeRejectsNonPositiveTaskExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "0");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager, never()).setTaskExpireTimeMs(anyLong());
  }

  @Test
  public void testOnChangeRejectsNegativeTaskExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "-100");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager, never()).setTaskExpireTimeMs(anyLong());
  }

  @Test
  public void testOnChangeInvalidTaskExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "not_a_number");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager, never()).setTaskExpireTimeMs(anyLong());
  }

  @Test
  public void testOnChangeValidTerminalStateExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS, "172800000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager).setTerminalStateExpireTimeMs(172800000L);
  }

  @Test
  public void testOnChangeRejectsNonPositiveTerminalStateExpireTime() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS, "-1");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager, never()).setTerminalStateExpireTimeMs(anyLong());
  }

  @Test
  public void testOnChangeValidMaxQueueSize() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_SIZE, "10000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_SIZE);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    assertEquals(_taskManager.getTaskQueueMaxSize(), 10000);
  }

  @Test
  public void testOnChangeMaxDeletesPerCycleValidValue() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, "100");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    assertEquals(_taskManager.getTaskQueueMaxDeletesPerCycle(), 100);
  }

  @Test
  public void testOnChangeMaxDeletesPerCycleClampedToCap() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, "9999");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    assertEquals(_taskManager.getTaskQueueMaxDeletesPerCycle(), PinotTaskManager.MAX_DELETES_PER_CYCLE_CAP);
  }

  @Test
  public void testOnChangeMaxDeletesPerCycleRejectsNonPositive() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, "0");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE);

    int before = _taskManager.getTaskQueueMaxDeletesPerCycle();
    _taskManager.onChange(changedConfigs, clusterConfigs);
    assertEquals(_taskManager.getTaskQueueMaxDeletesPerCycle(), before);
  }

  @Test
  public void testOnChangeValidQueueCapacity() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_CAPACITY, "5000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_CAPACITY);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    verify(_helixTaskResourceManager).setQueueCapacity(5000);
  }

  @Test
  public void testOnChangeValidWarningThreshold() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_WARNING_THRESHOLD, "8000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_WARNING_THRESHOLD);

    _taskManager.onChange(changedConfigs, clusterConfigs);
    assertEquals(_taskManager.getTaskQueueWarningThreshold(), 8000);
  }

  @Test
  public void testOnChangeGatesOnChangedConfigs() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "99000");
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS, "99000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);

    verify(_helixTaskResourceManager).setTaskExpireTimeMs(99000L);
    verify(_helixTaskResourceManager, never()).setTerminalStateExpireTimeMs(anyLong());
  }

  @Test
  public void testOnChangeMultipleChangedConfigsProcessesBoth() {
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, "10000");
    clusterConfigs.put(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS, "20000");

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS);
    changedConfigs.add(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS);

    _taskManager.onChange(changedConfigs, clusterConfigs);

    verify(_helixTaskResourceManager).setTaskExpireTimeMs(10000L);
    verify(_helixTaskResourceManager).setTerminalStateExpireTimeMs(20000L);
  }

  @Test
  public void testStartupClampsMaxDeletesPerCycle() {
    PinotHelixTaskResourceManager taskResMgr = mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager helixResMgr = mock(PinotHelixResourceManager.class);
    LeadControllerManager leadCtrlMgr = mock(LeadControllerManager.class);

    ControllerConf conf = new ControllerConf();
    conf.setProperty("controller.task.frequencyPeriod", "1h");
    conf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, 9999);

    ControllerMetrics metrics = new ControllerMetrics(new NoopPinotMetricsRegistry());

    @SuppressWarnings("unchecked")
    TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> cache = mock(TaskManagerStatusCache.class);

    PinotTaskManager mgr = new PinotTaskManager(
        taskResMgr, helixResMgr, leadCtrlMgr, conf, metrics, cache,
        mock(Executor.class), mock(PoolingHttpClientConnectionManager.class),
        mock(ResourceUtilizationManager.class));

    assertEquals(mgr.getTaskQueueMaxDeletesPerCycle(), PinotTaskManager.MAX_DELETES_PER_CYCLE_CAP);
  }

  // ==================== TASKS_TRACKED_FOR_TASK_TYPE metric tests ====================

  @Test
  public void testReportMetricsEmitsTaskQueueSize() {
    PinotHelixTaskResourceManager taskResMgr = mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager helixResMgr = mock(PinotHelixResourceManager.class);
    LeadControllerManager leadCtrlMgr = mock(LeadControllerManager.class);

    ControllerConf conf = new ControllerConf();
    conf.setProperty("controller.task.frequencyPeriod", "1h");

    ControllerMetrics metrics = spy(new ControllerMetrics(new NoopPinotMetricsRegistry()));

    @SuppressWarnings("unchecked")
    TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> cache = mock(TaskManagerStatusCache.class);

    PinotTaskManager mgr = new PinotTaskManager(
        taskResMgr, helixResMgr, leadCtrlMgr, conf, metrics, cache,
        mock(Executor.class), mock(PoolingHttpClientConnectionManager.class),
        mock(ResourceUtilizationManager.class));

    String taskType = "TestTask";
    Set<String> taskTypes = new HashSet<>();
    taskTypes.add(taskType);
    when(taskResMgr.getTaskTypes()).thenReturn(taskTypes);

    Map<String, TaskState> taskStates = new HashMap<>();
    taskStates.put("Task_TestTask_1", TaskState.COMPLETED);
    taskStates.put("Task_TestTask_2", TaskState.IN_PROGRESS);
    taskStates.put("Task_TestTask_3", TaskState.FAILED);
    when(taskResMgr.getTaskStates(taskType)).thenReturn(taskStates);

    mgr.reportMetrics(taskType);
    verify(metrics).setValueOfTableGauge(taskType, ControllerGauge.TASKS_TRACKED_FOR_TASK_TYPE, 3);
  }

  @Test
  public void testReportMetricsEmitsZeroForMissingTaskType() {
    PinotHelixTaskResourceManager taskResMgr = mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager helixResMgr = mock(PinotHelixResourceManager.class);
    LeadControllerManager leadCtrlMgr = mock(LeadControllerManager.class);

    ControllerConf conf = new ControllerConf();
    conf.setProperty("controller.task.frequencyPeriod", "1h");

    ControllerMetrics metrics = spy(new ControllerMetrics(new NoopPinotMetricsRegistry()));

    @SuppressWarnings("unchecked")
    TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> cache = mock(TaskManagerStatusCache.class);

    PinotTaskManager mgr = new PinotTaskManager(
        taskResMgr, helixResMgr, leadCtrlMgr, conf, metrics, cache,
        mock(Executor.class), mock(PoolingHttpClientConnectionManager.class),
        mock(ResourceUtilizationManager.class));

    String taskType = "MissingTask";
    when(taskResMgr.getTaskTypes()).thenReturn(Collections.emptySet());

    mgr.reportMetrics(taskType);
    verify(metrics).setValueOfTableGauge(taskType, ControllerGauge.TASKS_TRACKED_FOR_TASK_TYPE, 0);
  }
}
