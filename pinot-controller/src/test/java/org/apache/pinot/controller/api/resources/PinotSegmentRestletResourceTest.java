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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotSegmentRestletResourceTest {
  @Mock
  PinotHelixResourceManager _pinotHelixResourceManager;

  @InjectMocks
  PinotSegmentRestletResource _pinotSegmentRestletResource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGetServerToSegments() {
    String tableName = "testTable";
    Map<String, List<String>> fullServerToSegmentsMap = new HashMap<>();
    fullServerToSegmentsMap.put("svr01", new ArrayList<>(List.of("seg01", "seg02")));
    fullServerToSegmentsMap.put("svr02", new ArrayList<>(List.of("seg02", "seg03")));
    fullServerToSegmentsMap.put("svr03", new ArrayList<>(List.of("seg03", "seg01")));
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, null)).thenReturn(fullServerToSegmentsMap);
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, "svr02")).thenReturn(
        Map.of("svr02", new ArrayList<>(List.of("seg02", "seg03"))));
    when(_pinotHelixResourceManager.getServers(tableName, "seg01")).thenReturn(Set.of("svr01", "svr03"));

    // Get all servers and all their segments.
    Map<String, List<String>> serverToSegmentsMap =
        _pinotSegmentRestletResource.getServerToSegments(tableName, null, null);
    assertEquals(serverToSegmentsMap, fullServerToSegmentsMap);

    // Get all segments on svr02.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, null, "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg02", "seg03")));

    // Get all servers with seg01.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01", null);
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01"), "svr03", List.of("seg01")));

    // Simply map the provided server to the provided segments.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01", "svr01");
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01")));
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "anySegment", "anyServer");
    assertEquals(serverToSegmentsMap, Map.of("anyServer", List.of("anySegment")));
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01,seg02", "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg01", "seg02")));
    try {
      _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01,seg02", null);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Only one segment is expected but got: [seg01, seg02]"));
    }
  }
}
