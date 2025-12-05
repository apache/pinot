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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PinotTaskRestletResourceTest {
  @Mock
  PinotHelixResourceManager _pinotHelixResourceManager;
  @Mock
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  @Mock
  ControllerConf _controllerConf;
  @Mock
  UriInfo _uriInfo;

  @InjectMocks
  PinotTaskRestletResource _pinotTaskRestletResource;

  @BeforeMethod
  public void init()
      throws URISyntaxException {
    MockitoAnnotations.openMocks(this);
    when(_uriInfo.getRequestUri()).thenReturn(new URI("http://localhost:9000"));
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWhenMinionWorkerIdsAreNotSpecified()
      throws JsonProcessingException {
    Map<String, String> minionWorkerEndpoints
        = invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints(null);
    assertEquals(minionWorkerEndpoints,
        Map.of("minion1", "http://minion1:9514", "minion2", "http://minion2:9514"));
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWhenAllMinionWorkerIdsAreSpecified()
      throws JsonProcessingException {
    // use minion worker ids with spaces ensure they will be trimmed.
    Map<String, String> minionWorkerEndpoints
        = invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints(" minion1 , minion2 ");
    assertEquals(minionWorkerEndpoints,
        Map.of("minion1", "http://minion1:9514", "minion2", "http://minion2:9514"));
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWhenOneMinionWorkerIdIsSpecified()
      throws JsonProcessingException {
    Map<String, String> minionWorkerEndpoints
        = invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints("minion1");
    assertEquals(minionWorkerEndpoints,
        Map.of("minion1", "http://minion1:9514"));
  }

  private Map<String, String> invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints(
      String minionWorkerIds)
      throws JsonProcessingException {
    InstanceConfig minion1 = createInstanceConfig("minion1", "minion1", "9514");
    InstanceConfig minion2 = createInstanceConfig("minion2", "minion2", "9514");
    when(_pinotHelixResourceManager.getAllMinionInstanceConfigs()).thenReturn(List.of(minion1, minion2));
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    ArgumentCaptor<Map<String, String>> minionWorkerEndpointsCaptor = ArgumentCaptor.forClass(Map.class);
    when(_pinotHelixTaskResourceManager.getSubtaskOnWorkerProgress(anyString(), any(), any(),
        minionWorkerEndpointsCaptor.capture(), anyMap(), anyInt()))
        .thenReturn(Collections.emptyMap());
    String progress =
        _pinotTaskRestletResource.getSubtaskOnWorkerProgress(httpHeaders, "IN_PROGRESS", minionWorkerIds);
    assertEquals(progress, "{}");
    return minionWorkerEndpointsCaptor.getValue();
  }

  private InstanceConfig createInstanceConfig(String instanceId, String hostName, String port) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    instanceConfig.setHostName(hostName);
    instanceConfig.setPort(port);
    return instanceConfig;
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWithException()
      throws JsonProcessingException {
    when(_pinotHelixResourceManager.getAllMinionInstanceConfigs()).thenReturn(Collections.emptyList());
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(_pinotHelixTaskResourceManager
        .getSubtaskOnWorkerProgress(anyString(), any(), any(), anyMap(), anyMap(), anyInt()))
        .thenThrow(new RuntimeException());
    assertThrows(ControllerApplicationException.class,
        () -> _pinotTaskRestletResource.getSubtaskOnWorkerProgress(httpHeaders, "IN_PROGRESS", null));
  }

  @Test
  public void testGetTasksSummaryWithNoTasks() {
    // Create an empty TaskSummaryResponse
    PinotHelixTaskResourceManager.TaskSummaryResponse emptyResponse =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();

    when(_pinotHelixTaskResourceManager.getTasksSummary()).thenReturn(emptyResponse);

    PinotHelixTaskResourceManager.TaskSummaryResponse response = _pinotTaskRestletResource.getTasksSummary();

    assertNotNull(response);
    assertEquals(response.getTotalRunningTasks(), 0);
    assertEquals(response.getTotalWaitingTasks(), 0);
    assertEquals(response.getTotalInProgressTaskTypes(), 0);
    assertTrue(response.getTaskTypeBreakdown().isEmpty());
  }

  @Test
  public void testGetTasksSummaryWithMultipleTaskTypes() {
    // Create a TaskSummaryResponse with multiple task types
    PinotHelixTaskResourceManager.TaskSummaryResponse response =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();
    response.setTotalRunningTasks(150);
    response.setTotalWaitingTasks(50);
    response.setTotalInProgressTaskTypes(2);

    List<PinotHelixTaskResourceManager.TaskTypeBreakdown> breakdownList = new ArrayList<>();
    breakdownList.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "SegmentGenerationAndPushTask", 100, 30));
    breakdownList.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "RealtimeToOfflineSegmentsTask", 50, 20));
    response.setTaskTypeBreakdown(breakdownList);

    when(_pinotHelixTaskResourceManager.getTasksSummary()).thenReturn(response);

    PinotHelixTaskResourceManager.TaskSummaryResponse actualResponse = _pinotTaskRestletResource.getTasksSummary();

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getTotalRunningTasks(), 150);
    assertEquals(actualResponse.getTotalWaitingTasks(), 50);
    assertEquals(actualResponse.getTotalInProgressTaskTypes(), 2);
    assertEquals(actualResponse.getTaskTypeBreakdown().size(), 2);

    // Verify the first task type breakdown
    PinotHelixTaskResourceManager.TaskTypeBreakdown breakdown1 = actualResponse.getTaskTypeBreakdown().get(0);
    assertEquals(breakdown1.getTaskType(), "SegmentGenerationAndPushTask");
    assertEquals(breakdown1.getRunningCount(), 100);
    assertEquals(breakdown1.getWaitingCount(), 30);

    // Verify the second task type breakdown
    PinotHelixTaskResourceManager.TaskTypeBreakdown breakdown2 = actualResponse.getTaskTypeBreakdown().get(1);
    assertEquals(breakdown2.getTaskType(), "RealtimeToOfflineSegmentsTask");
    assertEquals(breakdown2.getRunningCount(), 50);
    assertEquals(breakdown2.getWaitingCount(), 20);
  }

  @Test
  public void testGetTasksSummaryWithOnlyRunningTasks() {
    // Create a TaskSummaryResponse with only running tasks, no waiting tasks
    PinotHelixTaskResourceManager.TaskSummaryResponse response =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();
    response.setTotalRunningTasks(3);
    response.setTotalWaitingTasks(0);
    response.setTotalInProgressTaskTypes(1);

    List<PinotHelixTaskResourceManager.TaskTypeBreakdown> breakdownList = new ArrayList<>();
    breakdownList.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "MergeRollupTask", 3, 0));
    response.setTaskTypeBreakdown(breakdownList);

    when(_pinotHelixTaskResourceManager.getTasksSummary()).thenReturn(response);

    PinotHelixTaskResourceManager.TaskSummaryResponse actualResponse = _pinotTaskRestletResource.getTasksSummary();

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getTotalRunningTasks(), 3);
    assertEquals(actualResponse.getTotalWaitingTasks(), 0);
    assertEquals(actualResponse.getTotalInProgressTaskTypes(), 1);
    assertEquals(actualResponse.getTaskTypeBreakdown().size(), 1);

    PinotHelixTaskResourceManager.TaskTypeBreakdown breakdown = actualResponse.getTaskTypeBreakdown().get(0);
    assertEquals(breakdown.getTaskType(), "MergeRollupTask");
    assertEquals(breakdown.getRunningCount(), 3);
    assertEquals(breakdown.getWaitingCount(), 0);
  }
}
