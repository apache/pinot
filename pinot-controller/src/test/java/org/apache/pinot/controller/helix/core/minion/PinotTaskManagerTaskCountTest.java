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

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.task.TaskState;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.validation.ResourceUtilizationManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "stateless")
public class PinotTaskManagerTaskCountTest {
  private PinotTaskManager _pinotTaskManager;
  private PinotHelixTaskResourceManager _mockTaskResourceManager;
  private ControllerMetrics _mockControllerMetrics;

  @BeforeMethod
  public void setUp() {
    // Create mocks
    _mockTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    LeadControllerManager mockLeadControllerManager = mock(LeadControllerManager.class);
    _mockControllerMetrics = mock(ControllerMetrics.class);
    ControllerConf mockControllerConf = mock(ControllerConf.class);
    PoolingHttpClientConnectionManager mockConnectionManager = mock(PoolingHttpClientConnectionManager.class);
    ResourceUtilizationManager mockResourceUtilizationManager = mock(ResourceUtilizationManager.class);
    
    // Configure task resource manager
    when(_mockTaskResourceManager.getTaskTypes()).thenReturn(Arrays.asList("Task1", "Task2"));
    
    Map<String, TaskState> task1States = new HashMap<>();
    task1States.put("task1_1", TaskState.IN_PROGRESS);
    task1States.put("task1_2", TaskState.NOT_STARTED);
    task1States.put("task1_3", TaskState.COMPLETED);
    when(_mockTaskResourceManager.getTaskStates("Task1")).thenReturn(task1States);
    
    Map<String, TaskState> task2States = new HashMap<>();
    task2States.put("task2_1", TaskState.IN_PROGRESS);
    task2States.put("task2_2", TaskState.FAILED);
    when(_mockTaskResourceManager.getTaskStates("Task2")).thenReturn(task2States);
    
    // Create PinotTaskManager with mocks
    _pinotTaskManager = spy(new PinotTaskManager(_mockTaskResourceManager, mockResourceManager, mockLeadControllerManager,
        mockControllerConf, _mockControllerMetrics, null, null, mockConnectionManager, mockResourceUtilizationManager));
  }

  @Test
  public void testGetTotalPendingTaskCount() {
    // Call the method under test
    int totalPendingTasks = _pinotTaskManager.getTotalPendingTaskCount();
    
    // Verify the result
    assertEquals(totalPendingTasks, 3); // 2 IN_PROGRESS + 1 NOT_STARTED
    
    // Verify metrics were set
    verify(_mockControllerMetrics).setValueOfGlobalGauge(ControllerGauge.TOTAL_PENDING_MINION_TASKS, 3);
  }

  @Test
  public void testGetPendingTaskCount() {
    // Call the method under test for Task1
    int task1PendingCount = _pinotTaskManager.getPendingTaskCount("Task1");
    
    // Verify the result
    assertEquals(task1PendingCount, 2); // 1 IN_PROGRESS + 1 NOT_STARTED
    
    // Verify metrics were set
    verify(_mockControllerMetrics).setValueOfTableGauge("Task1", ControllerGauge.PENDING_MINION_TASKS_PER_TYPE, 2);
    
    // Call the method under test for Task2
    int task2PendingCount = _pinotTaskManager.getPendingTaskCount("Task2");
    
    // Verify the result
    assertEquals(task2PendingCount, 1); // 1 IN_PROGRESS
    
    // Verify metrics were set
    verify(_mockControllerMetrics).setValueOfTableGauge("Task2", ControllerGauge.PENDING_MINION_TASKS_PER_TYPE, 1);
  }

  @Test
  public void testReportMetricsUpdatesTaskCounts() {
    // Call reportMetrics with Task1
    _pinotTaskManager.reportMetrics("Task1");
    
    // Verify that task count methods were called
    verify(_pinotTaskManager).getPendingTaskCount("Task1");
    verify(_pinotTaskManager).getTotalPendingTaskCount();
    
    // Call reportMetrics with Task2
    _pinotTaskManager.reportMetrics("Task2");
    
    // Verify that task count methods were called again
    verify(_pinotTaskManager).getPendingTaskCount("Task2");
    verify(_pinotTaskManager, times(2)).getTotalPendingTaskCount();
  }
}