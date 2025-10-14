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


public class PinotTableReloadResourceTest {
  @Mock
  PinotHelixResourceManager _pinotHelixResourceManager;

  @InjectMocks
  PinotTableReloadResource _resource;

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
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, null, true)).thenReturn(fullServerToSegmentsMap);
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, "svr02", true)).thenReturn(
        Map.of("svr02", new ArrayList<>(List.of("seg02", "seg03"))));
    when(_pinotHelixResourceManager.getServers(tableName, "seg01")).thenReturn(Set.of("svr01", "svr03"));

    // Get all servers and all their segments.
    Map<String, List<String>> serverToSegmentsMap = _resource.getServerToSegments(tableName, null, null);
    assertEquals(serverToSegmentsMap, fullServerToSegmentsMap);

    // Get all segments on svr02.
    serverToSegmentsMap = _resource.getServerToSegments(tableName, null, "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg02", "seg03")));

    // Get all servers with seg01.
    serverToSegmentsMap = _resource.getServerToSegments(tableName, "seg01", null);
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01"), "svr03", List.of("seg01")));

    // Simply map the provided server to the provided segments.
    serverToSegmentsMap = _resource.getServerToSegments(tableName, "seg01", "svr01");
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01")));
    serverToSegmentsMap = _resource.getServerToSegments(tableName, "anySegment", "anyServer");
    assertEquals(serverToSegmentsMap, Map.of("anyServer", List.of("anySegment")));
    serverToSegmentsMap = _resource.getServerToSegments(tableName, "seg01|seg02", "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg01", "seg02")));
    try {
      _resource.getServerToSegments(tableName, "seg01,seg02", null);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Only one segment is expected but got: [seg01, seg02]"));
    }
  }
}
