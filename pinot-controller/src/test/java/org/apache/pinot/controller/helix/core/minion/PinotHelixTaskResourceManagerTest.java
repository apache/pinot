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
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.TABLE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class PinotHelixTaskResourceManagerTest {

  @Test
  public void testGetSubtaskProgressNoWorker()
      throws Exception {
    TaskDriver taskDriver = mock(TaskDriver.class);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(anyString())).thenReturn(jobConfig);
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
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    for (String subtaskName : subtaskNames) {
      taskConfigMap.put(subtaskName, mock(TaskConfig.class));
    }
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
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
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(anyString())).thenReturn(jobConfig);
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
    Map<String, Integer> taskIdPartitionMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      String subtaskName = taskName + "_" + i;
      subtaskNames[i] = subtaskName;
      taskIdPartitionMap.put(subtaskName, i);
    }
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    for (String subtaskName : subtaskNames) {
      taskConfigMap.put(subtaskName, mock(TaskConfig.class));
    }
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    TaskPartitionState[] helixStates =
        new TaskPartitionState[]{TaskPartitionState.INIT, TaskPartitionState.RUNNING, TaskPartitionState.TASK_ERROR};
    when(jobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    when(jobContext.getAssignedParticipant(anyInt())).thenAnswer(
        invocation -> workers[(int) invocation.getArgument(0)]);
    when(jobContext.getPartitionState(anyInt())).thenAnswer(invocation -> helixStates[(int) invocation.getArgument(0)]);
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
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(anyString())).thenReturn(jobConfig);
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
    Map<String, Integer> taskIdPartitionMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      String subtaskName = taskName + "_" + i;
      subtaskNames[i] = subtaskName;
      taskIdPartitionMap.put(subtaskName, i);
      httpResp._httpResponses.put(workers[i],
          JsonUtils.objectToString(Collections.singletonMap(subtaskNames[i], "running on worker: " + i)));
    }
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    for (String subtaskName : subtaskNames) {
      taskConfigMap.put(subtaskName, mock(TaskConfig.class));
    }
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    TaskPartitionState[] helixStates =
        new TaskPartitionState[]{TaskPartitionState.INIT, TaskPartitionState.RUNNING, TaskPartitionState.TASK_ERROR};
    when(jobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    when(jobContext.getAssignedParticipant(anyInt())).thenAnswer(
        invocation -> workers[(int) invocation.getArgument(0)]);
    when(jobContext.getPartitionState(anyInt())).thenAnswer(invocation -> helixStates[(int) invocation.getArgument(0)]);
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
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(anyString())).thenReturn(jobConfig);
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
    Map<String, Integer> taskIdPartitionMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      String subtaskName = taskName + "_" + i;
      subtaskNames[i] = subtaskName;
      taskIdPartitionMap.put(subtaskName, i);
    }
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    for (String subtaskName : subtaskNames) {
      taskConfigMap.put(subtaskName, mock(TaskConfig.class));
    }
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    // Some subtasks are pending to be run
    httpResp._httpResponses.put(workers[0],
        JsonUtils.objectToString(Collections.singletonMap(subtaskNames[0], "running on worker: 0")));
    when(jobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    when(jobContext.getAssignedParticipant(0)).thenReturn(workers[0]);
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.RUNNING);
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
        mgr.getSubtaskOnWorkerProgress("IN_PROGRESS", httpHelper, selectedMinionWorkerEndpoints, Collections.emptyMap(),
            1000);
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
    when(httpHelper.doMultiGetRequest(workerEndpointCaptor.capture(), any(), anyBoolean(), any(), anyInt())).thenReturn(
        httpResp);

    PinotHelixTaskResourceManager mgr =
        new PinotHelixTaskResourceManager(mock(PinotHelixResourceManager.class), mock(TaskDriver.class));
    Map<String, Object> progress =
        mgr.getSubtaskOnWorkerProgress("IN_PROGRESS", httpHelper, selectedMinionWorkerEndpoints, Collections.emptyMap(),
            1000);
    List<String> value = workerEndpointCaptor.getValue();
    Set<String> expectedWorkerUrls = selectedMinionWorkerEndpoints.values().stream().map(
            workerEndpoint -> String.format("%s/tasks/subtask/state/progress?subTaskState=IN_PROGRESS", workerEndpoint))
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
    String taskName = "Task_TestTask_12345";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);
    TaskDriver taskDriver = mock(TaskDriver.class);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(anyString())).thenReturn(jobConfig);
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    taskConfigMap.put("taskId0", new TaskConfig("", new HashMap<>()));
    taskConfigMap.put("taskId1",
        new TaskConfig("", new HashMap<>(Collections.singletonMap("tableName", "table1_OFFLINE"))));
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    Map<String, Integer> taskIdPartitionMap = new HashMap<>();
    taskIdPartitionMap.put("taskId0", 0);
    taskIdPartitionMap.put("taskId1", 1);
    when(jobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    when(jobContext.getTaskIdForPartition(0)).thenReturn("taskId0");
    when(jobContext.getTaskIdForPartition(1)).thenReturn("taskId1");
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.RUNNING);
    when(jobContext.getPartitionState(1)).thenReturn(TaskPartitionState.COMPLETED);

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

  @Test
  public void testGetTasksDebugInfoByTableWithFiltering() {
    String taskType = "TestTask";
    String tableNameWithType = "testTable_OFFLINE";
    String otherTableNameWithType = "otherTable_OFFLINE";
    String taskName = "Task_TestTask_12345";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);

    // Mock TaskDriver and WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(workflowContext);

    // Mock job states
    Map<String, TaskState> helixJobStates = new HashMap<>();
    helixJobStates.put(helixJobName, TaskState.IN_PROGRESS);
    when(workflowContext.getJobStates()).thenReturn(helixJobStates);
    when(workflowContext.getJobState(helixJobName)).thenReturn(TaskState.IN_PROGRESS);

    // Mock JobContext and JobConfig
    JobContext jobContext = mock(JobContext.class);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);

    // Create task configs for different tables
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();

    // Task config for the target table
    Map<String, String> targetTableConfig = new HashMap<>();
    targetTableConfig.put(TABLE_NAME, tableNameWithType);
    targetTableConfig.put("taskId", "task1");
    TaskConfig targetTableTaskConfig = new TaskConfig("TestTask", targetTableConfig, "task1", null);
    taskConfigMap.put("task1", targetTableTaskConfig);

    // Task config for other table
    Map<String, String> otherTableConfig = new HashMap<>();
    otherTableConfig.put(TABLE_NAME, otherTableNameWithType);
    otherTableConfig.put("taskId", "task2");
    TaskConfig otherTableTaskConfig = new TaskConfig("TestTask", otherTableConfig, "task2", null);
    taskConfigMap.put("task2", otherTableTaskConfig);

    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    when(jobConfig.getTaskConfig("task1")).thenReturn(targetTableTaskConfig);
    when(jobConfig.getTaskConfig("task2")).thenReturn(otherTableTaskConfig);

    // Mock partition information
    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);
    partitionSet.add(1);
    when(jobContext.getPartitionSet()).thenReturn(partitionSet);
    when(jobContext.getTaskIdForPartition(0)).thenReturn("task1");
    when(jobContext.getTaskIdForPartition(1)).thenReturn("task2");
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.RUNNING);
    when(jobContext.getPartitionState(1)).thenReturn(TaskPartitionState.COMPLETED);
    when(jobContext.getAssignedParticipant(0)).thenReturn("worker1");
    when(jobContext.getAssignedParticipant(1)).thenReturn("worker2");
    when(jobContext.getPartitionInfo(0)).thenReturn("Running on worker1");
    when(jobContext.getPartitionInfo(1)).thenReturn("Completed on worker2");

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test filtering by table
    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> result =
        mgr.getTasksDebugInfoByTable(taskType, tableNameWithType, 0);

    // Verify that only tasks for the target table are returned
    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(taskName));

    PinotHelixTaskResourceManager.TaskDebugInfo taskDebugInfo = result.get(taskName);
    assertEquals(taskDebugInfo.getTaskState(), TaskState.IN_PROGRESS);

    // Verify that only subtasks for the target table are included
    List<PinotHelixTaskResourceManager.SubtaskDebugInfo> subtaskInfos = taskDebugInfo.getSubtaskInfos();
    assertEquals(subtaskInfos.size(), 1);
    assertEquals(subtaskInfos.get(0).getTaskId(), "task1");
    assertEquals(subtaskInfos.get(0).getState(), TaskPartitionState.RUNNING);

    // Verify the task config belongs to the target table
    PinotTaskConfig taskConfig = subtaskInfos.get(0).getTaskConfig();
    assertNotNull(taskConfig);
    assertEquals(taskConfig.getTableName(), tableNameWithType);
  }

  @Test
  public void testGetTasksDebugInfoByTableNoMatchingTable() {
    String taskType = "TestTask";
    String tableNameWithType = "testTable_OFFLINE";
    String otherTableNameWithType = "otherTable_OFFLINE";
    String taskName = "Task_TestTask_12345";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);

    // Mock TaskDriver and WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(workflowContext);

    // Mock job states
    Map<String, TaskState> helixJobStates = new HashMap<>();
    helixJobStates.put(helixJobName, TaskState.IN_PROGRESS);
    when(workflowContext.getJobStates()).thenReturn(helixJobStates);

    // Fix: Mock JobConfig to return an empty JobConfig (not null)
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);
    when(jobConfig.getTaskConfigMap()).thenReturn(new HashMap<>());

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test with no matching table
    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> result =
        mgr.getTasksDebugInfoByTable(taskType, tableNameWithType, 0);

    // Verify that no tasks are returned when no matching table is found
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetTasksDebugInfoByTableWithNullWorkflowContext() {
    String taskType = "TestTask";
    String tableNameWithType = "testTable_OFFLINE";

    // Mock TaskDriver with null WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(null);

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test with null workflow context
    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> result =
        mgr.getTasksDebugInfoByTable(taskType, tableNameWithType, 0);

    // Verify that empty result is returned
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetTaskDebugInfoWithTableFiltering() {
    String taskName = "Task_TestTask_12345";
    String taskType = "TestTask";
    String tableNameWithType = "testTable_OFFLINE";
    String otherTableNameWithType = "otherTable_OFFLINE";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);

    // Mock TaskDriver and WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(workflowContext);
    when(workflowContext.getJobState(helixJobName)).thenReturn(TaskState.IN_PROGRESS);

    // Mock JobContext and JobConfig
    JobContext jobContext = mock(JobContext.class);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);

    // Create task configs for different tables
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();

    // Task config for the target table
    Map<String, String> targetTableConfig = new HashMap<>();
    targetTableConfig.put(TABLE_NAME, tableNameWithType);
    targetTableConfig.put("taskId", "task1");
    TaskConfig targetTableTaskConfig = new TaskConfig("TestTask", targetTableConfig, "task1", null);
    taskConfigMap.put("task1", targetTableTaskConfig);

    // Task config for other table
    Map<String, String> otherTableConfig = new HashMap<>();
    otherTableConfig.put(TABLE_NAME, otherTableNameWithType);
    otherTableConfig.put("taskId", "task2");
    TaskConfig otherTableTaskConfig = new TaskConfig("TestTask", otherTableConfig, "task2", null);
    taskConfigMap.put("task2", otherTableTaskConfig);

    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);

    // Mock partition information
    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);
    partitionSet.add(1);
    when(jobContext.getPartitionSet()).thenReturn(partitionSet);
    when(jobContext.getTaskIdForPartition(0)).thenReturn("task1");
    when(jobContext.getTaskIdForPartition(1)).thenReturn("task2");
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.RUNNING);
    when(jobContext.getPartitionState(1)).thenReturn(TaskPartitionState.COMPLETED);
    when(jobContext.getAssignedParticipant(0)).thenReturn("worker1");
    when(jobContext.getAssignedParticipant(1)).thenReturn("worker2");
    when(jobContext.getPartitionInfo(0)).thenReturn("Running on worker1");
    when(jobContext.getPartitionInfo(1)).thenReturn("Completed on worker2");

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test getTaskDebugInfo with table filtering (should not filter when tableNameWithType is null)
    PinotHelixTaskResourceManager.TaskDebugInfo result = mgr.getTaskDebugInfo(taskName, 0);

    // Verify that all subtasks are included when no table filtering is applied
    assertEquals(result.getTaskState(), TaskState.IN_PROGRESS);
    List<PinotHelixTaskResourceManager.SubtaskDebugInfo> subtaskInfos = result.getSubtaskInfos();
    assertEquals(subtaskInfos.size(), 1); // Only running tasks are shown with verbosity = 0

    // Verify the subtask is included
    assertEquals(subtaskInfos.get(0).getTaskId(), "task1");
    assertEquals(subtaskInfos.get(0).getState(), TaskPartitionState.RUNNING);
  }

  @Test
  public void testGetTaskDebugInfoWithNullJobContext() {
    String taskName = "Task_TestTask_12345";
    String taskType = "TestTask";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);

    // Mock TaskDriver and WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(workflowContext);
    when(workflowContext.getJobState(helixJobName)).thenReturn(TaskState.IN_PROGRESS);

    // Mock null JobContext
    when(taskDriver.getJobContext(helixJobName)).thenReturn(null);

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test getTaskDebugInfo with null job context
    PinotHelixTaskResourceManager.TaskDebugInfo result = mgr.getTaskDebugInfo(taskName, 0);

    // Verify that basic task info is still returned even with null job context
    assertEquals(result.getTaskState(), TaskState.IN_PROGRESS);
    assertNull(result.getSubtaskInfos());
  }

  @Test
  public void testGetTasksDebugInfoByTableWithVerbosity() {
    String taskType = "TestTask";
    String tableNameWithType = "testTable_OFFLINE";
    String taskName = "Task_TestTask_12345";
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);

    // Mock TaskDriver and WorkflowContext
    TaskDriver taskDriver = mock(TaskDriver.class);
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType)))
        .thenReturn(workflowContext);

    // Mock job states
    Map<String, TaskState> helixJobStates = new HashMap<>();
    helixJobStates.put(helixJobName, TaskState.IN_PROGRESS);
    when(workflowContext.getJobStates()).thenReturn(helixJobStates);
    when(workflowContext.getJobState(helixJobName)).thenReturn(TaskState.IN_PROGRESS);

    // Mock JobContext and JobConfig
    JobContext jobContext = mock(JobContext.class);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);

    // Create task config for the target table
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    Map<String, String> targetTableConfig = new HashMap<>();
    targetTableConfig.put(TABLE_NAME, tableNameWithType);
    targetTableConfig.put("taskId", "task1");
    TaskConfig targetTableTaskConfig = new TaskConfig("TestTask", targetTableConfig, "task1", null);
    taskConfigMap.put("task1", targetTableTaskConfig);

    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    when(jobConfig.getTaskConfig("task1")).thenReturn(targetTableTaskConfig);

    // Mock partition information with completed state
    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);
    when(jobContext.getPartitionSet()).thenReturn(partitionSet);
    when(jobContext.getTaskIdForPartition(0)).thenReturn("task1");
    when(jobContext.getPartitionState(0)).thenReturn(TaskPartitionState.COMPLETED);
    when(jobContext.getAssignedParticipant(0)).thenReturn("worker1");
    when(jobContext.getPartitionInfo(0)).thenReturn("Completed on worker1");

    // Mock PinotHelixResourceManager
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);

    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(helixResourceManager, taskDriver);

    // Test with verbosity = 0 (should not show completed tasks)
    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> result =
        mgr.getTasksDebugInfoByTable(taskType, tableNameWithType, 0);

    assertEquals(result.size(), 1);
    PinotHelixTaskResourceManager.TaskDebugInfo taskDebugInfo = result.get(taskName);
    List<PinotHelixTaskResourceManager.SubtaskDebugInfo> subtaskInfos = taskDebugInfo.getSubtaskInfos();
    assertTrue(subtaskInfos == null || subtaskInfos.isEmpty()); // Completed tasks should be filtered out

    // Test with verbosity > 0 (should show all tasks including completed)
    result = mgr.getTasksDebugInfoByTable(taskType, tableNameWithType, 1);

    assertEquals(result.size(), 1);
    taskDebugInfo = result.get(taskName);
    subtaskInfos = taskDebugInfo.getSubtaskInfos();
    assertEquals(subtaskInfos.size(), 1); // Completed tasks should be included
    assertEquals(subtaskInfos.get(0).getState(), TaskPartitionState.COMPLETED);
  }

  @Test
  public void testGetTaskCountsWithSingleStateFilter() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";
    String taskName2 = "Task_TestTask_67890";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return our test task names
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    tasks.add(taskName2);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock workflow-level components for getTaskStates
    String helixJobQueueName = "TaskQueue_" + taskType;
    WorkflowConfig workflowConfig = mock(WorkflowConfig.class);
    when(taskDriver.getWorkflowConfig(helixJobQueueName)).thenReturn(workflowConfig);

    JobDag jobDag = mock(JobDag.class);
    Set<String> helixJobs = new HashSet<>();
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName1));
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName2));
    when(jobDag.getAllNodes()).thenReturn(helixJobs);
    when(workflowConfig.getJobDag()).thenReturn(jobDag);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(helixJobQueueName)).thenReturn(workflowContext);
    Map<String, TaskState> jobStatesMap = new HashMap<>();
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName1), TaskState.IN_PROGRESS);
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName2), TaskState.COMPLETED);
    when(workflowContext.getJobStates()).thenReturn(jobStatesMap);

    // Mock JobConfig and JobContext for both tasks
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);
    mockTaskJobConfigAndContext(taskDriver, taskName2, TaskPartitionState.COMPLETED);

    // Test filter by "IN_PROGRESS" - should only return taskName1
    Map<String, PinotHelixTaskResourceManager.TaskCount> inProgressTasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS", null);
    assertEquals(inProgressTasks.size(), 1);
    assertTrue(inProgressTasks.containsKey(taskName1));
    assertFalse(inProgressTasks.containsKey(taskName2));

    // Test filter by "COMPLETED" - should only return taskName2
    Map<String, PinotHelixTaskResourceManager.TaskCount> completedTasks =
        spyMgr.getTaskCounts(taskType, "COMPLETED", null);
    assertEquals(completedTasks.size(), 1);
    assertFalse(completedTasks.containsKey(taskName1));
    assertTrue(completedTasks.containsKey(taskName2));

    // Test filter by "FAILED" - should return no tasks
    Map<String, PinotHelixTaskResourceManager.TaskCount> failedTasks =
        spyMgr.getTaskCounts(taskType, "FAILED", null);
    assertEquals(failedTasks.size(), 0);
  }

  @Test
  public void testGetTaskCountsWithMultipleStatesFilter() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";
    String taskName2 = "Task_TestTask_67890";
    String taskName3 = "Task_TestTask_11111";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return our test task names
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    tasks.add(taskName2);
    tasks.add(taskName3);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock workflow-level components for getTaskStates
    String helixJobQueueName = "TaskQueue_" + taskType;
    WorkflowConfig workflowConfig = mock(WorkflowConfig.class);
    when(taskDriver.getWorkflowConfig(helixJobQueueName)).thenReturn(workflowConfig);

    JobDag jobDag = mock(JobDag.class);
    Set<String> helixJobs = new HashSet<>();
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName1));
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName2));
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName3));
    when(jobDag.getAllNodes()).thenReturn(helixJobs);
    when(workflowConfig.getJobDag()).thenReturn(jobDag);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(helixJobQueueName)).thenReturn(workflowContext);
    Map<String, TaskState> jobStatesMap = new HashMap<>();
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName1), TaskState.IN_PROGRESS);
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName2), TaskState.FAILED);
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName3), TaskState.COMPLETED);
    when(workflowContext.getJobStates()).thenReturn(jobStatesMap);

    // Mock JobConfig and JobContext for all tasks
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);
    mockTaskJobConfigAndContext(taskDriver, taskName2, TaskPartitionState.TASK_ERROR);
    mockTaskJobConfigAndContext(taskDriver, taskName3, TaskPartitionState.COMPLETED);

    // Test filter by "IN_PROGRESS,FAILED" - should return taskName1 and taskName2
    Map<String, PinotHelixTaskResourceManager.TaskCount> inProgressOrFailedTasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS,FAILED", null);
    assertEquals(inProgressOrFailedTasks.size(), 2);
    assertTrue(inProgressOrFailedTasks.containsKey(taskName1));
    assertTrue(inProgressOrFailedTasks.containsKey(taskName2));
    assertFalse(inProgressOrFailedTasks.containsKey(taskName3));

    // Test filter by "COMPLETED,IN_PROGRESS" - should return taskName1 and taskName3
    Map<String, PinotHelixTaskResourceManager.TaskCount> completedOrInProgressTasks =
        spyMgr.getTaskCounts(taskType, "COMPLETED,IN_PROGRESS", null);
    assertEquals(completedOrInProgressTasks.size(), 2);
    assertTrue(completedOrInProgressTasks.containsKey(taskName1));
    assertFalse(completedOrInProgressTasks.containsKey(taskName2));
    assertTrue(completedOrInProgressTasks.containsKey(taskName3));

    // Test filter by "NOT_STARTED,STOPPED" - should return no tasks
    Map<String, PinotHelixTaskResourceManager.TaskCount> notStartedOrStoppedTasks =
        spyMgr.getTaskCounts(taskType, "NOT_STARTED,STOPPED", null);
    assertEquals(notStartedOrStoppedTasks.size(), 0);

    // Test filter with spaces "IN_PROGRESS, FAILED, COMPLETED" - should return all three
    Map<String, PinotHelixTaskResourceManager.TaskCount> allTasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS, FAILED, COMPLETED", null);
    assertEquals(allTasks.size(), 3);
    assertTrue(allTasks.containsKey(taskName1));
    assertTrue(allTasks.containsKey(taskName2));
    assertTrue(allTasks.containsKey(taskName3));
  }

  @Test
  public void testGetTaskCountsWithInvalidState() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return a task
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock JobConfig and JobContext for the task
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);

    // Test with invalid single state
    try {
      spyMgr.getTaskCounts(taskType, "INVALID_STATE", null);
      fail("Expected IllegalArgumentException for invalid state");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid state: INVALID_STATE"));
    }

    // Test with mixed valid and invalid states
    try {
      spyMgr.getTaskCounts(taskType, "IN_PROGRESS,INVALID_STATE,COMPLETED", null);
      fail("Expected IllegalArgumentException for invalid state in multiple states");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid state: INVALID_STATE"));
    }
  }

  @Test
  public void testGetTaskCountsWithTableFilter() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";
    String taskName2 = "Task_TestTask_67890";
    String table1 = "table1_OFFLINE";
    String table2 = "table2_OFFLINE";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return our test task names
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    tasks.add(taskName2);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock JobConfig and JobContext for both tasks
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);
    mockTaskJobConfigAndContext(taskDriver, taskName2, TaskPartitionState.COMPLETED);

    // Mock subtask configs - taskName1 has subtasks for table1, taskName2 has subtasks for table2
    List<PinotTaskConfig> subtaskConfigs1 = new ArrayList<>();
    PinotTaskConfig taskConfig1 = mock(PinotTaskConfig.class);
    when(taskConfig1.getTableName()).thenReturn(table1);
    subtaskConfigs1.add(taskConfig1);
    when(spyMgr.getSubtaskConfigs(taskName1)).thenReturn(subtaskConfigs1);

    List<PinotTaskConfig> subtaskConfigs2 = new ArrayList<>();
    PinotTaskConfig taskConfig2 = mock(PinotTaskConfig.class);
    when(taskConfig2.getTableName()).thenReturn(table2);
    subtaskConfigs2.add(taskConfig2);
    when(spyMgr.getSubtaskConfigs(taskName2)).thenReturn(subtaskConfigs2);

    // Test filter by table1 - should only return taskName1
    Map<String, PinotHelixTaskResourceManager.TaskCount> table1Tasks =
        spyMgr.getTaskCounts(taskType, null, table1);
    assertEquals(table1Tasks.size(), 1);
    assertTrue(table1Tasks.containsKey(taskName1));
    assertFalse(table1Tasks.containsKey(taskName2));

    // Test filter by table2 - should only return taskName2
    Map<String, PinotHelixTaskResourceManager.TaskCount> table2Tasks =
        spyMgr.getTaskCounts(taskType, null, table2);
    assertEquals(table2Tasks.size(), 1);
    assertFalse(table2Tasks.containsKey(taskName1));
    assertTrue(table2Tasks.containsKey(taskName2));

    // Test filter by non-existent table - should return no tasks
    Map<String, PinotHelixTaskResourceManager.TaskCount> noTableTasks =
        spyMgr.getTaskCounts(taskType, null, "nonexistent_OFFLINE");
    assertEquals(noTableTasks.size(), 0);
  }

  @Test
  public void testGetTaskCountsWithStateAndTableFilter() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";
    String taskName2 = "Task_TestTask_67890";
    String taskName3 = "Task_TestTask_11111";
    String table1 = "table1_OFFLINE";
    String table2 = "table2_OFFLINE";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return our test task names
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    tasks.add(taskName2);
    tasks.add(taskName3);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock workflow-level components for getTaskStates
    String helixJobQueueName = "TaskQueue_" + taskType;
    WorkflowConfig workflowConfig = mock(WorkflowConfig.class);
    when(taskDriver.getWorkflowConfig(helixJobQueueName)).thenReturn(workflowConfig);

    JobDag jobDag = mock(JobDag.class);
    Set<String> helixJobs = new HashSet<>();
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName1));
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName2));
    helixJobs.add(PinotHelixTaskResourceManager.getHelixJobName(taskName3));
    when(jobDag.getAllNodes()).thenReturn(helixJobs);
    when(workflowConfig.getJobDag()).thenReturn(jobDag);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(taskDriver.getWorkflowContext(helixJobQueueName)).thenReturn(workflowContext);
    Map<String, TaskState> jobStatesMap = new HashMap<>();
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName1), TaskState.IN_PROGRESS); // table1
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName2), TaskState.COMPLETED);   // table1
    jobStatesMap.put(PinotHelixTaskResourceManager.getHelixJobName(taskName3), TaskState.IN_PROGRESS); // table2
    when(workflowContext.getJobStates()).thenReturn(jobStatesMap);

    // Mock JobConfig and JobContext - different states for each task
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);   // table1
    mockTaskJobConfigAndContext(taskDriver, taskName2, TaskPartitionState.COMPLETED); // table1
    mockTaskJobConfigAndContext(taskDriver, taskName3, TaskPartitionState.RUNNING);   // table2

    // Mock subtask configs - taskName1 and taskName2 for table1, taskName3 for table2
    List<PinotTaskConfig> subtaskConfigs1 = new ArrayList<>();
    PinotTaskConfig taskConfig1 = mock(PinotTaskConfig.class);
    when(taskConfig1.getTableName()).thenReturn(table1);
    subtaskConfigs1.add(taskConfig1);
    when(spyMgr.getSubtaskConfigs(taskName1)).thenReturn(subtaskConfigs1);

    List<PinotTaskConfig> subtaskConfigs2 = new ArrayList<>();
    PinotTaskConfig taskConfig2 = mock(PinotTaskConfig.class);
    when(taskConfig2.getTableName()).thenReturn(table1);
    subtaskConfigs2.add(taskConfig2);
    when(spyMgr.getSubtaskConfigs(taskName2)).thenReturn(subtaskConfigs2);

    List<PinotTaskConfig> subtaskConfigs3 = new ArrayList<>();
    PinotTaskConfig taskConfig3 = mock(PinotTaskConfig.class);
    when(taskConfig3.getTableName()).thenReturn(table2);
    subtaskConfigs3.add(taskConfig3);
    when(spyMgr.getSubtaskConfigs(taskName3)).thenReturn(subtaskConfigs3);

    // Test filter by running state and table1 - should only return taskName1
    Map<String, PinotHelixTaskResourceManager.TaskCount> runningTable1Tasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS", table1);
    assertEquals(runningTable1Tasks.size(), 1);
    assertTrue(runningTable1Tasks.containsKey(taskName1));
    assertFalse(runningTable1Tasks.containsKey(taskName2));
    assertFalse(runningTable1Tasks.containsKey(taskName3));

    // Test filter by completed state and table1 - should only return taskName2
    Map<String, PinotHelixTaskResourceManager.TaskCount> completedTable1Tasks =
        spyMgr.getTaskCounts(taskType, "COMPLETED", table1);
    assertEquals(completedTable1Tasks.size(), 1);
    assertFalse(completedTable1Tasks.containsKey(taskName1));
    assertTrue(completedTable1Tasks.containsKey(taskName2));
    assertFalse(completedTable1Tasks.containsKey(taskName3));

    // Test filter by running state and table2 - should only return taskName3
    Map<String, PinotHelixTaskResourceManager.TaskCount> runningTable2Tasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS", table2);
    assertEquals(runningTable2Tasks.size(), 1);
    assertFalse(runningTable2Tasks.containsKey(taskName1));
    assertFalse(runningTable2Tasks.containsKey(taskName2));
    assertTrue(runningTable2Tasks.containsKey(taskName3));

    // Test filter by completed state and table2 - should return no tasks
    Map<String, PinotHelixTaskResourceManager.TaskCount> completedTable2Tasks =
        spyMgr.getTaskCounts(taskType, "COMPLETED", table2);
    assertEquals(completedTable2Tasks.size(), 0);

    // Test filter by multiple states and table1 - should return taskName1 and taskName2
    Map<String, PinotHelixTaskResourceManager.TaskCount> multiStateTable1Tasks =
        spyMgr.getTaskCounts(taskType, "IN_PROGRESS,COMPLETED", table1);
    assertEquals(multiStateTable1Tasks.size(), 2);
    assertTrue(multiStateTable1Tasks.containsKey(taskName1));
    assertTrue(multiStateTable1Tasks.containsKey(taskName2));
    assertFalse(multiStateTable1Tasks.containsKey(taskName3));
  }

  @Test
  public void testGetTaskCountsWithTableFilterEdgeCases() {
    String taskType = "TestTask";
    String taskName1 = "Task_TestTask_12345";

    TaskDriver taskDriver = mock(TaskDriver.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    PinotHelixTaskResourceManager mgr = new PinotHelixTaskResourceManager(pinotHelixResourceManager, taskDriver);

    // Mock getTasks to return our test task name
    Set<String> tasks = new HashSet<>();
    tasks.add(taskName1);
    PinotHelixTaskResourceManager spyMgr = Mockito.spy(mgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);

    // Mock JobConfig and JobContext
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);

    // Test with task that has null table name
    List<PinotTaskConfig> subtaskConfigsNullTable = new ArrayList<>();
    PinotTaskConfig taskConfigNullTable = mock(PinotTaskConfig.class);
    when(taskConfigNullTable.getTableName()).thenReturn(null);
    subtaskConfigsNullTable.add(taskConfigNullTable);
    when(spyMgr.getSubtaskConfigs(taskName1)).thenReturn(subtaskConfigsNullTable);

    Map<String, PinotHelixTaskResourceManager.TaskCount> nullTableTasks =
        spyMgr.getTaskCounts(taskType, null, "anyTable_OFFLINE");
    assertEquals(nullTableTasks.size(), 0);

    // Test with task that throws exception when getting subtask configs
    when(spyMgr.getSubtaskConfigs(taskName1)).thenThrow(new RuntimeException("Test exception"));

    Map<String, PinotHelixTaskResourceManager.TaskCount> exceptionTasks =
        spyMgr.getTaskCounts(taskType, null, "anyTable_OFFLINE");
    assertEquals(exceptionTasks.size(), 0);

    // Reset the mock to avoid exception in subsequent calls
    Mockito.reset(spyMgr);
    when(spyMgr.getTasks(taskType)).thenReturn(tasks);
    mockTaskJobConfigAndContext(taskDriver, taskName1, TaskPartitionState.RUNNING);

    // Test with null state and null table (should return all tasks like original method)
    when(spyMgr.getSubtaskConfigs(taskName1)).thenReturn(new ArrayList<>());
    Map<String, PinotHelixTaskResourceManager.TaskCount> allTasks =
        spyMgr.getTaskCounts(taskType, null, null);
    assertEquals(allTasks.size(), 1);
    assertTrue(allTasks.containsKey(taskName1));
  }

  /**
   * Helper method to mock JobConfig and JobContext for a task
   */
  private void mockTaskJobConfigAndContext(TaskDriver taskDriver, String taskName, TaskPartitionState state) {
    String helixJobName = PinotHelixTaskResourceManager.getHelixJobName(taskName);
    JobConfig jobConfig = mock(JobConfig.class);
    when(taskDriver.getJobConfig(helixJobName)).thenReturn(jobConfig);
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    taskConfigMap.put("taskId0", new TaskConfig("", new HashMap<>()));
    when(jobConfig.getTaskConfigMap()).thenReturn(taskConfigMap);
    JobContext jobContext = mock(JobContext.class);
    when(taskDriver.getJobContext(helixJobName)).thenReturn(jobContext);
    Map<String, Integer> taskIdPartitionMap = new HashMap<>();
    taskIdPartitionMap.put("taskId0", 0);
    when(jobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    when(jobContext.getPartitionState(0)).thenReturn(state);
  }
}
