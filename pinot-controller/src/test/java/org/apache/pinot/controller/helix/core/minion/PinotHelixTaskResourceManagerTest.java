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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.WorkflowContext;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotHelixTaskResourceManagerTest {
  @Test
  public void testGetSubtaskProgressNoWorker()
      throws Exception {
    TaskDriver taskDriver = mock(TaskDriver.class);
    when(taskDriver.getJobContext(anyString())).thenReturn(mock(JobContext.class));
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    CompletionServiceHelper.CompletionServiceResponse httpResp =
        new CompletionServiceHelper.CompletionServiceResponse();
    when(httpHelper.doMultiGetRequest(any(), any(), anyBoolean(), any(), anyInt())).thenReturn(httpResp);
    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), taskDriver);
    // No worker to run subtasks.
    Map<String, String> workerEndpoints = new HashMap<>();
    String taskName = "Task_SegmentGenerationAndPushTask_someone";
    String[] subtaskNames = new String[3];
    for (int i = 0; i < 3; i++) {
      subtaskNames[i] = taskName + "_" + i;
    }
    Map<String, Object> progress =
        mgr.getSubtaskProgress(taskName, StringUtils.join(subtaskNames, ','), httpHelper, workerEndpoints,
            Collections.emptyMap(), 1000);
    for (String subtaskName : subtaskNames) {
      assertEquals(progress.get(subtaskName), "No worker has run this subtask");
    }
  }

  @Test
  public void testGetSubtaskProgressNoResponse()
      throws Exception {
    TaskDriver taskDriver = mock(TaskDriver.class);
    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(anyString())).thenReturn(jobContext);
    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), taskDriver);
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    CompletionServiceHelper.CompletionServiceResponse httpResp =
        new CompletionServiceHelper.CompletionServiceResponse();
    when(httpHelper.doMultiGetRequest(any(), any(), anyBoolean(), any(), anyInt())).thenReturn(httpResp);
    // Three workers to run 3 subtasks but got no progress status from workers.
    httpResp._failedResponseCount = 3;
    String[] workers = new String[]{"worker0", "worker1", "worker2"};
    Map<String, String> workerEndpoints = new HashMap<>();
    for (String worker : workers) {
      workerEndpoints.put(worker, "http://" + worker + ":9000");
    }
    String taskName = "Task_SegmentGenerationAndPushTask_someone";
    String[] subtaskNames = new String[3];
    Set<Integer> subtaskIds = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      subtaskIds.add(i);
      subtaskNames[i] = taskName + "_" + i;
    }
    TaskPartitionState[] helixStates =
        new TaskPartitionState[]{TaskPartitionState.INIT, TaskPartitionState.RUNNING, TaskPartitionState.TASK_ERROR};
    when(jobContext.getTaskIdForPartition(anyInt())).thenReturn(subtaskNames[0], subtaskNames[1], subtaskNames[2]);
    when(jobContext.getAssignedParticipant(anyInt())).thenReturn(workers[0], workers[1], workers[2]);
    when(jobContext.getPartitionState(anyInt())).thenReturn(helixStates[0], helixStates[1], helixStates[2]);
    when(jobContext.getPartitionSet()).thenReturn(subtaskIds);
    Map<String, Object> progress =
        mgr.getSubtaskProgress(taskName, StringUtils.join(subtaskNames, ','), httpHelper, workerEndpoints,
            Collections.emptyMap(), 1000);
    for (int i = 0; i < 3; i++) {
      String taskProgress = (String) progress.get(subtaskNames[i]);
      assertTrue(taskProgress.contains(helixStates[i].name()), subtaskNames[i] + ":" + taskProgress);
    }
  }

  @Test
  public void testGetSubtaskProgressWithResponse()
      throws Exception {
    TaskDriver taskDriver = mock(TaskDriver.class);
    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(anyString())).thenReturn(jobContext);
    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), taskDriver);
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    CompletionServiceHelper.CompletionServiceResponse httpResp =
        new CompletionServiceHelper.CompletionServiceResponse();
    when(httpHelper.doMultiGetRequest(any(), any(), anyBoolean(), any(), anyInt())).thenReturn(httpResp);
    String[] workers = new String[]{"worker0", "worker1", "worker2"};
    Map<String, String> workerEndpoints = new HashMap<>();
    for (String worker : workers) {
      workerEndpoints.put(worker, "http://" + worker + ":9000");
    }
    String taskName = "Task_SegmentGenerationAndPushTask_someone";
    String[] subtaskNames = new String[3];
    Set<Integer> subtaskIds = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      subtaskIds.add(i);
      subtaskNames[i] = taskName + "_" + i;
      httpResp._httpResponses.put(workers[i],
          JsonUtils.objectToString(Collections.singletonMap(subtaskNames[i], "running on worker: " + i)));
    }
    TaskPartitionState[] helixStates =
        new TaskPartitionState[]{TaskPartitionState.INIT, TaskPartitionState.RUNNING, TaskPartitionState.TASK_ERROR};
    when(jobContext.getTaskIdForPartition(anyInt())).thenReturn(subtaskNames[0], subtaskNames[1], subtaskNames[2]);
    when(jobContext.getAssignedParticipant(anyInt())).thenReturn(workers[0], workers[1], workers[2]);
    when(jobContext.getPartitionState(anyInt())).thenReturn(helixStates[0], helixStates[1], helixStates[2]);
    when(jobContext.getPartitionSet()).thenReturn(subtaskIds);
    Map<String, Object> progress =
        mgr.getSubtaskProgress(taskName, StringUtils.join(subtaskNames, ','), httpHelper, workerEndpoints,
            Collections.emptyMap(), 1000);
    for (int i = 0; i < 3; i++) {
      String taskProgress = (String) progress.get(subtaskNames[i]);
      assertEquals(taskProgress, "running on worker: " + i);
    }
  }

  @Test
  public void testGetSubtaskProgressPending()
      throws Exception {
    TaskDriver taskDriver = mock(TaskDriver.class);
    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(anyString())).thenReturn(jobContext);
    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), taskDriver);
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    CompletionServiceHelper.CompletionServiceResponse httpResp =
        new CompletionServiceHelper.CompletionServiceResponse();
    when(httpHelper.doMultiGetRequest(any(), any(), anyBoolean(), any(), anyInt())).thenReturn(httpResp);
    String[] workers = new String[]{"worker0", "worker1", "worker2"};
    Map<String, String> workerEndpoints = new HashMap<>();
    for (String worker : workers) {
      workerEndpoints.put(worker, "http://" + worker + ":9000");
    }
    String taskName = "Task_SegmentGenerationAndPushTask_someone";
    String[] subtaskNames = new String[3];
    Set<Integer> subtaskIds = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      subtaskIds.add(i);
      subtaskNames[i] = taskName + "_" + i;
    }
    // Some subtasks are pending to be run.
    TaskPartitionState[] helixStates = new TaskPartitionState[]{TaskPartitionState.RUNNING, null, null};
    httpResp._httpResponses.put(workers[0],
        JsonUtils.objectToString(Collections.singletonMap(subtaskNames[0], "running on worker: 0")));
    when(jobContext.getTaskIdForPartition(anyInt())).thenReturn(subtaskNames[0], subtaskNames[1], subtaskNames[2]);
    when(jobContext.getAssignedParticipant(anyInt())).thenReturn(workers[0], null, null);
    when(jobContext.getPartitionState(anyInt())).thenReturn(helixStates[0], null, null);
    when(jobContext.getPartitionSet()).thenReturn(subtaskIds);
    Map<String, Object> progress =
        mgr.getSubtaskProgress(taskName, StringUtils.join(subtaskNames, ','), httpHelper, workerEndpoints,
            Collections.emptyMap(), 1000);
    String taskProgress = (String) progress.get(subtaskNames[0]);
    assertEquals(taskProgress, "running on worker: 0");
    taskProgress = (String) progress.get(subtaskNames[1]);
    assertEquals(taskProgress, "No worker has run this subtask");
    taskProgress = (String) progress.get(subtaskNames[2]);
    assertEquals(taskProgress, "No worker has run this subtask");
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressNoWorker()
      throws JsonProcessingException {
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), mock(TaskDriver.class));
    // No worker to run subtasks.
    Map<String, String> selectedMinionWorkerEndpoints = new HashMap<>();
    Map<String, Object> progress =
        mgr.getSubtaskOnWorkerProgress("IN_PROGRESS", httpHelper,
            selectedMinionWorkerEndpoints, Collections.emptyMap(), 1000);
    assertTrue(progress.isEmpty());
    verify(httpHelper, Mockito.never()).doMultiGetRequest(any(), any(), anyBoolean(), any(), anyInt());
  }

  @Test
  public void testGetSubtaskWithGivenStateProgress()
      throws IOException {
    CompletionServiceHelper httpHelper = mock(CompletionServiceHelper.class);
    CompletionServiceHelper.CompletionServiceResponse httpResp =
        new CompletionServiceHelper.CompletionServiceResponse();
    String taskIdPrefix = "Task_SegmentGenerationAndPushTask_someone";
    String workerIdPrefix = "worker";
    String[] subtaskIds = new String[6];
    String[] workerIds = new String[3];
    Map<String, String> selectedMinionWorkerEndpoints = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      workerIds[i] = workerIdPrefix + i;
      String workerEndpoint = "http://" + workerIds[i] + ":9000";
      selectedMinionWorkerEndpoints.put(workerIds[i], workerEndpoint);

      subtaskIds[2 * i] = taskIdPrefix + "_" + (2 * i);
      subtaskIds[2 * i + 1] = taskIdPrefix + "_" + (2 * i + 1);
      // Notice that for testing purpose, we map subtask names to empty strings. In reality, subtask names will be
      // mapped to jsonized org.apache.pinot.minion.event.MinionEventObserver
      httpResp._httpResponses.put(
          String.format("%s/tasks/subtask/state/progress?subTaskState=IN_PROGRESS", workerEndpoint),
          JsonUtils.objectToString(ImmutableMap.of(subtaskIds[2 * i], "", subtaskIds[2 * i + 1], "")));
    }
    httpResp._failedResponseCount = 1;
    ArgumentCaptor<List<String>> workerEndpointCaptor = ArgumentCaptor.forClass(List.class);
    when(httpHelper.doMultiGetRequest(workerEndpointCaptor.capture(), any(), anyBoolean(), any(), anyInt()))
        .thenReturn(httpResp);

    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), mock(TaskDriver.class));

    Map<String, Object> progress =
        mgr.getSubtaskOnWorkerProgress("IN_PROGRESS", httpHelper, selectedMinionWorkerEndpoints,
            Collections.emptyMap(), 1000);
    List<String> value = workerEndpointCaptor.getValue();
    Set<String> expectedWorkerUrls = selectedMinionWorkerEndpoints.values().stream()
        .map(workerEndpoint
            -> String.format("%s/tasks/subtask/state/progress?subTaskState=IN_PROGRESS", workerEndpoint))
        .collect(Collectors.toSet());
    assertEquals(new HashSet<>(value), expectedWorkerUrls);
    assertEquals(progress.size(), 3);
    for (int i = 0; i < 3; i++) {
      Object responseFromMinionWorker = progress.get(workerIds[i]);
      Map<String, Object> subtaskProgressMap = (Map<String, Object>) responseFromMinionWorker;
      assertEquals(subtaskProgressMap.size(), 2);
      assertTrue(subtaskProgressMap.containsKey(subtaskIds[2 * i]));
      assertTrue(subtaskProgressMap.containsKey(subtaskIds[2 * i + 1]));
    }
  }

  @Test
  public void testGetTableTaskCount() {
    String taskType = "TestTask";
    String taskName = "Task_TestTask_12345";
    String helixJobQueueName = PinotHelixTaskResourceManager.getHelixJobQueueName(taskType);
    String helixJobName = helixJobQueueName + "_" + taskName;

    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(helixJobQueueName)).thenReturn(workflowContext);

    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    when(jobContext.getPartitionSet()).thenReturn(ImmutableSet.of(0, 1));
    when(jobContext.getTaskIdForPartition(0)).thenReturn("taskId0");
    when(jobContext.getTaskIdForPartition(1)).thenReturn("taskId1");
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.RUNNING);
    when(jobContext.getPartitionState(1)).thenReturn(TaskPartitionState.COMPLETED);

    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);
    when(jobConfig.getTaskConfig("taskId0")).thenReturn(new TaskConfig("", new HashMap<>()));
    when(jobConfig.getTaskConfig("taskId1")).thenReturn(new TaskConfig("",
        new HashMap<>(ImmutableMap.of("tableName", "table1_OFFLINE"))));

    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), taskDriver);
    Map<String, PinotHelixTaskResourceManager.TaskCount> tableTaskCount = mgr.getTableTaskCount(taskName);
    assertEquals(tableTaskCount.size(), 2);

    PinotHelixTaskResourceManager.TaskCount taskCount = tableTaskCount.get("table1_OFFLINE");
    assertEquals(taskCount.getTotal(), 1);
    assertEquals(taskCount.getCompleted(), 1);
    assertEquals(taskCount.getRunning(), 0);
    assertEquals(taskCount.getWaiting(), 0);
    assertEquals(taskCount.getError(), 0);
    assertEquals(taskCount.getUnknown(), 0);
    taskCount = tableTaskCount.get("unknown");
    assertEquals(taskCount.getTotal(), 1);
    assertEquals(taskCount.getCompleted(), 0);
    assertEquals(taskCount.getRunning(), 1);
    assertEquals(taskCount.getWaiting(), 0);
    assertEquals(taskCount.getError(), 0);
    assertEquals(taskCount.getUnknown(), 0);
  }
}
