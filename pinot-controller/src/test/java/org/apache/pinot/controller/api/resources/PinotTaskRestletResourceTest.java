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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
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
import static org.testng.Assert.assertThrows;


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
        ImmutableMap.of("minion1", "http://minion1:9514", "minion2", "http://minion2:9514"));
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWhenAllMinionWorkerIdsAreSpecified()
      throws JsonProcessingException {
    // use minion worker ids with spaces ensure they will be trimmed.
    Map<String, String> minionWorkerEndpoints
        = invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints(" minion1 , minion2 ");
    assertEquals(minionWorkerEndpoints,
        ImmutableMap.of("minion1", "http://minion1:9514", "minion2", "http://minion2:9514"));
  }

  @Test
  public void testGetSubtaskWithGivenStateProgressWhenOneMinionWorkerIdIsSpecified()
      throws JsonProcessingException {
    Map<String, String> minionWorkerEndpoints
        = invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints("minion1");
    assertEquals(minionWorkerEndpoints,
        ImmutableMap.of("minion1", "http://minion1:9514"));
  }

  private Map<String, String> invokeGetSubtaskWithGivenStateProgressAndReturnCapturedMinionWorkerEndpoints(
      String minionWorkerIds)
      throws JsonProcessingException {
    InstanceConfig minion1 = createInstanceConfig("minion1", "minion1", "9514");
    InstanceConfig minion2 = createInstanceConfig("minion2", "minion2", "9514");
    when(_pinotHelixResourceManager.getAllMinionInstanceConfigs()).thenReturn(ImmutableList.of(minion1, minion2));
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
}
