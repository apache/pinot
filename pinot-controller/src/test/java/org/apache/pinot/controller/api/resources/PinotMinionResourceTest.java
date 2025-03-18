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
package org.apache.pinot.controller.api.resources;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "stateless")
public class PinotMinionResourceTest {

  private PinotMinionResource _pinotMinionResource;
  private PinotHelixResourceManager _mockResourceManager;
  private PinotHelixTaskResourceManager _mockTaskResourceManager;
  private PinotTaskManager _mockTaskManager;

  @BeforeMethod
  public void setUp() {
    // Create mocks
    _mockResourceManager = mock(PinotHelixResourceManager.class);
    _mockTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
    _mockTaskManager = mock(PinotTaskManager.class);

    // Initialize resource
    _pinotMinionResource = new PinotMinionResource();
    _pinotMinionResource._pinotHelixResourceManager = _mockResourceManager;
    _pinotMinionResource._pinotHelixTaskResourceManager = _mockTaskResourceManager;
    _pinotMinionResource._pinotTaskManager = _mockTaskManager;
  }

  @Test
  public void testGetMinionInstances() {
    // Setup mock
    Set<String> minionInstances = new HashSet<>(Arrays.asList("Minion_1", "Minion_2", "Minion_3"));
    when(_mockResourceManager.getAllInstancesForMinion()).thenReturn(minionInstances);

    // Call method
    Set<String> result = _pinotMinionResource.getMinionInstances();

    // Verify result
    assertEquals(result, minionInstances);
    verify(_mockResourceManager).getAllInstancesForMinion();
  }

  @Test
  public void testGetTaggedMinionInstances() {
    // Setup mock for specific tag
    String testTag = "TestTag";
    Set<String> taggedInstances = new HashSet<>(Arrays.asList("Minion_1", "Minion_2"));
    when(_mockResourceManager.getInstancesWithTag(testTag)).thenReturn(taggedInstances);

    // Call method with specific tag
    Map<String, Set<String>> result = _pinotMinionResource.getTaggedMinionInstances(testTag);

    // Verify result
    assertEquals(result.size(), 1);
    assertEquals(result.get(testTag), taggedInstances);
    verify(_mockResourceManager).getInstancesWithTag(testTag);

    // Setup mock for all tags
    Map<String, Set<String>> allTaggedInstances = new HashMap<>();
    allTaggedInstances.put("Tag1", new HashSet<>(Arrays.asList("Minion_1", "Minion_2")));
    allTaggedInstances.put("Tag2", new HashSet<>(Collections.singletonList("Minion_3")));
    when(_mockResourceManager.getMinionInstancesWithTags()).thenReturn(allTaggedInstances);

    // Call method with empty tag
    Map<String, Set<String>> allResult = _pinotMinionResource.getTaggedMinionInstances("");

    // Verify result
    assertEquals(allResult, allTaggedInstances);
    verify(_mockResourceManager).getMinionInstancesWithTags();
  }

  @Test
  public void testGetPendingTaskCount() {
    // Setup mocks
    when(_mockTaskManager.getTotalPendingTaskCount()).thenReturn(5);
    when(_mockTaskResourceManager.getTaskTypes()).thenReturn(Arrays.asList("Task1", "Task2"));
    when(_mockTaskManager.getPendingTaskCount("Task1")).thenReturn(3);
    when(_mockTaskManager.getPendingTaskCount("Task2")).thenReturn(2);
    when(_mockResourceManager.getAllInstancesForMinion()).thenReturn(
        new HashSet<>(Arrays.asList("Minion_1", "Minion_2")));

    // Call method
    Map<String, Object> result = _pinotMinionResource.getPendingTaskCount();

    // Verify result
    assertEquals(result.get("totalPendingTasks"), 5);
    assertEquals(result.get("totalMinionInstances"), 2);
    assertEquals(result.get("tasksPerMinion"), 2.5); // 5 tasks / 2 minions = 2.5
    
    Map<String, Integer> pendingTasksByType = (Map<String, Integer>) result.get("pendingTasksByType");
    assertEquals(pendingTasksByType.get("Task1"), Integer.valueOf(3));
    assertEquals(pendingTasksByType.get("Task2"), Integer.valueOf(2));
    
    // Verify method calls
    verify(_mockTaskManager).getTotalPendingTaskCount();
    verify(_mockTaskResourceManager).getTaskTypes();
    verify(_mockTaskManager).getPendingTaskCount("Task1");
    verify(_mockTaskManager).getPendingTaskCount("Task2");
    verify(_mockResourceManager).getAllInstancesForMinion();
  }

  @Test
  public void testGetPendingTaskCountByType() {
    // Setup mocks
    String taskType = "Task1";
    when(_mockTaskManager.getPendingTaskCount(taskType)).thenReturn(3);
    
    Map<String, TaskState> taskStates = new HashMap<>();
    taskStates.put("task1", TaskState.IN_PROGRESS);
    taskStates.put("task2", TaskState.NOT_STARTED);
    taskStates.put("task3", TaskState.COMPLETED);
    taskStates.put("task4", TaskState.FAILED);
    when(_mockTaskResourceManager.getTaskStates(taskType)).thenReturn(taskStates);
    
    String taskQueueTag = "Task1_Queue";
    when(_mockTaskResourceManager.getTaskQueueTag(taskType)).thenReturn(taskQueueTag);
    
    Set<String> minionInstances = new HashSet<>(Arrays.asList("Minion_1", "Minion_2"));
    when(_mockResourceManager.getInstancesWithTag(taskQueueTag)).thenReturn(minionInstances);

    // Call method
    Map<String, Object> result = _pinotMinionResource.getPendingTaskCountByType(taskType);

    // Verify result
    assertEquals(result.get("pendingTasks"), 3);
    assertEquals(result.get("totalTasks"), 4);
    assertEquals(result.get("availableMinionInstances"), 2);
    assertEquals(result.get("tasksPerMinion"), 1.5); // 3 tasks / 2 minions = 1.5
    
    Map<TaskState, Integer> taskStateCount = (Map<TaskState, Integer>) result.get("taskStateCount");
    assertEquals(taskStateCount.get(TaskState.IN_PROGRESS), Integer.valueOf(1));
    assertEquals(taskStateCount.get(TaskState.NOT_STARTED), Integer.valueOf(1));
    assertEquals(taskStateCount.get(TaskState.COMPLETED), Integer.valueOf(1));
    assertEquals(taskStateCount.get(TaskState.FAILED), Integer.valueOf(1));
    
    // Verify method calls
    verify(_mockTaskManager).getPendingTaskCount(taskType);
    verify(_mockTaskResourceManager).getTaskStates(taskType);
    verify(_mockTaskResourceManager).getTaskQueueTag(taskType);
    verify(_mockResourceManager).getInstancesWithTag(taskQueueTag);
  }
}