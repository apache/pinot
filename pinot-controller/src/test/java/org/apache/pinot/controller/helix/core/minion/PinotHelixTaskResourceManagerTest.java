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
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
    String[] workers = new String[]{"worker01", "worker02", "worker03"};
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
    String[] workers = new String[]{"worker01", "worker02", "worker03"};
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
}
