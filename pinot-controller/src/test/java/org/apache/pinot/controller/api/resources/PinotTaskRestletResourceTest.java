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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
  @Mock
  Executor _executor;
  @Mock
  HttpClientConnectionManager _connectionManager;

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
    when(_controllerConf.getMinionAdminRequestTimeoutSeconds()).thenReturn(10);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> minionWorkerEndpointsCaptor = ArgumentCaptor.forClass(Map.class);
    when(_pinotHelixTaskResourceManager.getSubtaskOnWorkerProgress(anyString(), any(), any(),
        minionWorkerEndpointsCaptor.capture(), anyMap(), anyInt()))
        .thenReturn(Map.of());
    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    _pinotTaskRestletResource.getSubtaskOnWorkerProgress(httpHeaders, "IN_PROGRESS", minionWorkerIds, asyncResponse);

    verify(asyncResponse).resume(responseCaptor.capture());
    assertEquals(responseCaptor.getValue(), "{}");
    return minionWorkerEndpointsCaptor.getValue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTaskConfigResponsesRedactCredentials() {
    String taskType = "SegmentGenerationAndPushTask";
    PinotTaskConfig stored = taskConfigWithCredentials(taskType);
    when(_pinotHelixTaskResourceManager.getSubtaskConfigs("task1")).thenReturn(List.of(stored));
    when(_pinotHelixTaskResourceManager.getSubtaskConfigs("task1", null)).thenReturn(Map.of("task1_0", stored));
    when(_pinotHelixTaskResourceManager.getTaskDebugInfo("task1", null, 1)).thenReturn(debugInfo(stored));
    when(_pinotHelixTaskResourceManager.getTasksDebugInfo(taskType, 1)).thenReturn(Map.of("task1", debugInfo(stored)));
    when(_pinotHelixTaskResourceManager.getTasksDebugInfoByTable(taskType, "myTable_OFFLINE", 1))
        .thenReturn(Map.of("task1", debugInfo(stored)));
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);

    List<PinotTaskConfig> taskConfigs = (List<PinotTaskConfig>) resumedWith(
        response -> _pinotTaskRestletResource.getTaskConfigs("task1", response));
    assertEquals(taskConfigs.size(), 1);
    assertRedacted(taskConfigs.get(0), taskType);

    Map<String, PinotTaskConfig> subtaskConfigs = (Map<String, PinotTaskConfig>) resumedWith(
        response -> _pinotTaskRestletResource.getSubtaskConfigs("task1", null, response));
    assertRedacted(subtaskConfigs.get("task1_0"), taskType);

    PinotHelixTaskResourceManager.TaskDebugInfo taskDebugInfo = (PinotHelixTaskResourceManager.TaskDebugInfo)
        resumedWith(response -> _pinotTaskRestletResource.getTaskDebugInfo("task1", 1, null, httpHeaders, response));
    assertRedacted(taskDebugInfo.getSubtaskInfos().get(0).getTaskConfig(), taskType);

    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> tasksDebugInfo =
        (Map<String, PinotHelixTaskResourceManager.TaskDebugInfo>) resumedWith(
            response -> _pinotTaskRestletResource.getTasksDebugInfo(taskType, 1, response));
    assertRedacted(tasksDebugInfo.get("task1").getSubtaskInfos().get(0).getTaskConfig(), taskType);

    Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> tableDebugInfo =
        (Map<String, PinotHelixTaskResourceManager.TaskDebugInfo>) resumedWith(
            response -> _pinotTaskRestletResource.getTasksDebugInfo(taskType, "myTable_OFFLINE", 1, httpHeaders,
                response));
    assertRedacted(tableDebugInfo.get("task1").getSubtaskInfos().get(0).getTaskConfig(), taskType);

    // The stored task config, which minions execute, keeps its credentials.
    assertEquals(stored.getConfigs().get("authToken"), "Basic task-auth-secret");
    assertEquals(stored.getConfigs().get("input.fs.prop.secretKey"), "task-fs-secret");
  }

  private static PinotTaskConfig taskConfigWithCredentials(String taskType) {
    Map<String, String> configs = new HashMap<>();
    configs.put("tableName", "myTable_OFFLINE");
    configs.put("authToken", "Basic task-auth-secret");
    configs.put("input.fs.prop.secretKey", "task-fs-secret");
    configs.put("inputDirURI", "https://store.example/input?X-Amz-Signature=task-signature&region=us-west-2");
    return new PinotTaskConfig(taskType, configs);
  }

  private static PinotHelixTaskResourceManager.TaskDebugInfo debugInfo(PinotTaskConfig taskConfig) {
    PinotHelixTaskResourceManager.SubtaskDebugInfo subtaskInfo = new PinotHelixTaskResourceManager.SubtaskDebugInfo();
    subtaskInfo.setTaskConfig(taskConfig);
    PinotHelixTaskResourceManager.TaskDebugInfo debugInfo = new PinotHelixTaskResourceManager.TaskDebugInfo();
    debugInfo.addSubtaskInfo(subtaskInfo);
    return debugInfo;
  }

  private static void assertRedacted(PinotTaskConfig taskConfig, String taskType) {
    String marker = TableConfigRedactionUtils.REDACTION_MARKER;
    Map<String, String> configs = taskConfig.getConfigs();
    assertEquals(taskConfig.getTaskType(), taskType);
    assertEquals(taskConfig.getTableName(), "myTable_OFFLINE");
    assertEquals(configs.get("authToken"), marker);
    assertEquals(configs.get("input.fs.prop.secretKey"), marker);
    assertEquals(configs.get("inputDirURI"),
        "https://store.example/input?X-Amz-Signature=" + marker + "&region=us-west-2");
  }

  /// Runs an asynchronous endpoint and returns the entity it resumed the response with.
  private static Object resumedWith(Consumer<AsyncResponse> endpoint) {
    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    endpoint.accept(asyncResponse);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(asyncResponse).resume(responseCaptor.capture());
    return responseCaptor.getValue();
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
    when(_pinotHelixResourceManager.getAllMinionInstanceConfigs()).thenReturn(List.of());
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(_controllerConf.getMinionAdminRequestTimeoutSeconds()).thenReturn(10);
    when(_pinotHelixTaskResourceManager
        .getSubtaskOnWorkerProgress(anyString(), any(), any(), anyMap(), anyMap(), anyInt()))
        .thenThrow(new RuntimeException());
    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    ArgumentCaptor<Throwable> responseCaptor = ArgumentCaptor.forClass(Throwable.class);
    _pinotTaskRestletResource.getSubtaskOnWorkerProgress(httpHeaders, "IN_PROGRESS", null, asyncResponse);

    verify(asyncResponse).resume(responseCaptor.capture());
    assertTrue(responseCaptor.getValue() instanceof ControllerApplicationException);
  }

  @Test
  public void testGetTasksSummaryWithNoTasks() {
    // Create an empty TaskSummaryResponse
    PinotHelixTaskResourceManager.TaskSummaryResponse emptyResponse =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();

    when(_pinotHelixTaskResourceManager.getTasksSummary(null)).thenReturn(emptyResponse);

    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    _pinotTaskRestletResource.getTasksSummary(null, asyncResponse);

    verify(asyncResponse).resume(responseCaptor.capture());

    PinotHelixTaskResourceManager.TaskSummaryResponse response =
        (PinotHelixTaskResourceManager.TaskSummaryResponse) responseCaptor.getValue();
    assertNotNull(response);
    assertEquals(response.getTotalRunningTasks(), 0);
    assertEquals(response.getTotalWaitingTasks(), 0);
    assertTrue(response.getTaskBreakdown().isEmpty());
  }

  @Test
  public void testGetTasksSummaryWithMultipleTenants() {
    // Create a TaskSummaryResponse with multiple tenants
    PinotHelixTaskResourceManager.TaskSummaryResponse response =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();
    response.setTotalRunningTasks(150);
    response.setTotalWaitingTasks(50);

    // Tenant 1: defaultTenant
    List<PinotHelixTaskResourceManager.TaskTypeBreakdown> tenant1TaskTypes = new ArrayList<>();
    tenant1TaskTypes.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "SegmentGenerationAndPushTask", 100, 30));
    PinotHelixTaskResourceManager.TenantTaskBreakdown tenant1 =
        new PinotHelixTaskResourceManager.TenantTaskBreakdown("defaultTenant", 100, 30, tenant1TaskTypes);

    // Tenant 2: tenant2
    List<PinotHelixTaskResourceManager.TaskTypeBreakdown> tenant2TaskTypes = new ArrayList<>();
    tenant2TaskTypes.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "RealtimeToOfflineSegmentsTask", 50, 20));
    PinotHelixTaskResourceManager.TenantTaskBreakdown tenant2 =
        new PinotHelixTaskResourceManager.TenantTaskBreakdown("tenant2", 50, 20, tenant2TaskTypes);

    List<PinotHelixTaskResourceManager.TenantTaskBreakdown> taskBreakdown = new ArrayList<>();
    taskBreakdown.add(tenant1);
    taskBreakdown.add(tenant2);
    response.setTaskBreakdown(taskBreakdown);

    when(_pinotHelixTaskResourceManager.getTasksSummary(null)).thenReturn(response);

    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    _pinotTaskRestletResource.getTasksSummary(null, asyncResponse);

    verify(asyncResponse).resume(responseCaptor.capture());

    PinotHelixTaskResourceManager.TaskSummaryResponse actualResponse =
        (PinotHelixTaskResourceManager.TaskSummaryResponse) responseCaptor.getValue();
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getTotalRunningTasks(), 150);
    assertEquals(actualResponse.getTotalWaitingTasks(), 50);
    assertEquals(actualResponse.getTaskBreakdown().size(), 2);

    // Verify first tenant breakdown
    PinotHelixTaskResourceManager.TenantTaskBreakdown tenantBreakdown1 = actualResponse.getTaskBreakdown().get(0);
    assertEquals(tenantBreakdown1.getTenant(), "defaultTenant");
    assertEquals(tenantBreakdown1.getRunningTasks(), 100);
    assertEquals(tenantBreakdown1.getWaitingTasks(), 30);
    assertEquals(tenantBreakdown1.getTaskTypeBreakdown().size(), 1);
    assertEquals(tenantBreakdown1.getTaskTypeBreakdown().get(0).getTaskType(), "SegmentGenerationAndPushTask");

    // Verify second tenant breakdown
    PinotHelixTaskResourceManager.TenantTaskBreakdown tenantBreakdown2 = actualResponse.getTaskBreakdown().get(1);
    assertEquals(tenantBreakdown2.getTenant(), "tenant2");
    assertEquals(tenantBreakdown2.getRunningTasks(), 50);
    assertEquals(tenantBreakdown2.getWaitingTasks(), 20);
    assertEquals(tenantBreakdown2.getTaskTypeBreakdown().size(), 1);
    assertEquals(tenantBreakdown2.getTaskTypeBreakdown().get(0).getTaskType(), "RealtimeToOfflineSegmentsTask");
  }

  @Test
  public void testGetTasksSummaryWithTenantFilter() {
    // Create a TaskSummaryResponse filtered to a specific tenant
    PinotHelixTaskResourceManager.TaskSummaryResponse response =
        new PinotHelixTaskResourceManager.TaskSummaryResponse();
    response.setTotalRunningTasks(100);
    response.setTotalWaitingTasks(30);

    List<PinotHelixTaskResourceManager.TaskTypeBreakdown> taskTypes = new ArrayList<>();
    taskTypes.add(new PinotHelixTaskResourceManager.TaskTypeBreakdown(
        "SegmentGenerationAndPushTask", 100, 30));

    PinotHelixTaskResourceManager.TenantTaskBreakdown tenantBreakdown =
        new PinotHelixTaskResourceManager.TenantTaskBreakdown("defaultTenant", 100, 30, taskTypes);

    List<PinotHelixTaskResourceManager.TenantTaskBreakdown> taskBreakdown = new ArrayList<>();
    taskBreakdown.add(tenantBreakdown);
    response.setTaskBreakdown(taskBreakdown);

    when(_pinotHelixTaskResourceManager.getTasksSummary("defaultTenant")).thenReturn(response);

    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    _pinotTaskRestletResource.getTasksSummary("defaultTenant", asyncResponse);

    verify(asyncResponse).resume(responseCaptor.capture());

    PinotHelixTaskResourceManager.TaskSummaryResponse actualResponse =
        (PinotHelixTaskResourceManager.TaskSummaryResponse) responseCaptor.getValue();
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getTotalRunningTasks(), 100);
    assertEquals(actualResponse.getTotalWaitingTasks(), 30);
    assertEquals(actualResponse.getTaskBreakdown().size(), 1);

    PinotHelixTaskResourceManager.TenantTaskBreakdown tenant = actualResponse.getTaskBreakdown().get(0);
    assertEquals(tenant.getTenant(), "defaultTenant");
    assertEquals(tenant.getRunningTasks(), 100);
    assertEquals(tenant.getWaitingTasks(), 30);
    assertEquals(tenant.getTaskTypeBreakdown().size(), 1);
    assertEquals(tenant.getTaskTypeBreakdown().get(0).getTaskType(), "SegmentGenerationAndPushTask");
  }
}
