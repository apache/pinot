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
package org.apache.pinot.broker.routing;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for {@link MultiClusterRoutingManager}.
 * Tests verify multi-cluster routing behavior including local/remote cluster coordination,
 * routing table combination, and exception handling.
 */
public class MultiClusterRoutingManagerTest {
  private static final String TEST_TABLE = "testTable_OFFLINE";
  private static final long REQUEST_ID = 12345L;
  private static final int DEFAULT_SERVER_PORT = 9000;

  // Test constants for routing table metrics
  private static final int LOCAL_PRUNED_SEGMENTS = 5;
  private static final int REMOTE_PRUNED_SEGMENTS = 3;
  private static final int COMBINED_PRUNED_SEGMENTS = LOCAL_PRUNED_SEGMENTS + REMOTE_PRUNED_SEGMENTS;

  @Mock
  private BrokerRoutingManager _localClusterRoutingManager;

  @Mock
  private RemoteClusterBrokerRoutingManager _remoteClusterRoutingManager1;

  @Mock
  private RemoteClusterBrokerRoutingManager _remoteClusterRoutingManager2;

  private MultiClusterRoutingManager _multiClusterRoutingManager;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    List<RemoteClusterBrokerRoutingManager> remoteClusterManagers = Arrays.asList(
        _remoteClusterRoutingManager1, _remoteClusterRoutingManager2);
    _multiClusterRoutingManager = new MultiClusterRoutingManager(
        _localClusterRoutingManager, remoteClusterManagers);
  }

  @Test
  public void testRoutingExistsInLocal() {
    when(_localClusterRoutingManager.routingExists(TEST_TABLE)).thenReturn(true);

    boolean exists = _multiClusterRoutingManager.routingExists(TEST_TABLE);

    assertTrue(exists);
    verify(_localClusterRoutingManager).routingExists(TEST_TABLE);
    // Should not check remote if local returns true
    verify(_remoteClusterRoutingManager1, never()).routingExists(anyString());
  }

  @Test
  public void testRoutingExistsInRemote() {
    when(_localClusterRoutingManager.routingExists(TEST_TABLE)).thenReturn(false);
    when(_remoteClusterRoutingManager1.routingExists(TEST_TABLE)).thenReturn(true);

    boolean exists = _multiClusterRoutingManager.routingExists(TEST_TABLE);

    assertTrue(exists);
    verify(_localClusterRoutingManager).routingExists(TEST_TABLE);
    verify(_remoteClusterRoutingManager1).routingExists(TEST_TABLE);
  }

  @Test
  public void testRoutingExistsNowhere() {
    when(_localClusterRoutingManager.routingExists(TEST_TABLE)).thenReturn(false);
    when(_remoteClusterRoutingManager1.routingExists(TEST_TABLE)).thenReturn(false);
    when(_remoteClusterRoutingManager2.routingExists(TEST_TABLE)).thenReturn(false);

    boolean exists = _multiClusterRoutingManager.routingExists(TEST_TABLE);

    assertFalse(exists);
  }

  @Test
  public void testIsTableDisabled() {
    when(_localClusterRoutingManager.isTableDisabled(TEST_TABLE)).thenReturn(true);

    boolean disabled = _multiClusterRoutingManager.isTableDisabled(TEST_TABLE);

    assertTrue(disabled);
  }

  @Test
  public void testGetRoutingTableFromLocalOnly() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    Map<ServerInstance, SegmentsToQuery> serverMap = createMockServerMap("server1", Arrays.asList("seg1", "seg2"));
    RoutingTable localTable = new RoutingTable(serverMap, Collections.emptyList(), 0);

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 1);
    assertEquals(result.getNumPrunedSegments(), 0);
  }

  @Test
  public void testGetRoutingTableCombinesLocalAndRemote() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);

    Map<ServerInstance, SegmentsToQuery> localServerMap = createMockServerMap("server1", Arrays.asList("seg1"));
    RoutingTable localTable = new RoutingTable(localServerMap, Collections.emptyList(), LOCAL_PRUNED_SEGMENTS);

    Map<ServerInstance, SegmentsToQuery> remoteServerMap = createMockServerMap("server2", Arrays.asList("seg2"));
    RoutingTable remoteTable =
        new RoutingTable(remoteServerMap, Arrays.asList("unavailable1"), REMOTE_PRUNED_SEGMENTS);

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(remoteTable);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 2);
    assertEquals(result.getUnavailableSegments().size(), 1);
    assertEquals(result.getNumPrunedSegments(), COMBINED_PRUNED_SEGMENTS);
  }

  @Test
  public void testGetRoutingTableCombinesDifferentServers() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    ServerInstance localServer = createMockServerInstance("localServer");
    ServerInstance remoteServer = createMockServerInstance("remoteServer");

    RoutingTable localTable = createRoutingTable(localServer,
        Arrays.asList("seg1", "seg2"), Arrays.asList("opt1"));
    RoutingTable remoteTable = createRoutingTable(remoteServer,
        Arrays.asList("seg3"), Arrays.asList("opt2"));

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(remoteTable);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 2);

    int totalSegments = getTotalSegmentCount(result);
    assertEquals(totalSegments, 3, "Should have 3 segments total (seg1, seg2, seg3)");
  }

  @Test
  public void testGetRoutingTableHandlesRemoteException() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    Map<ServerInstance, SegmentsToQuery> localServerMap = createMockServerMap("server1", Arrays.asList("seg1"));
    RoutingTable localTable = new RoutingTable(localServerMap, Collections.emptyList(), 0);

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenThrow(new RuntimeException("Remote cluster error"));
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 1);
  }

  @Test
  public void testGetRoutingTableReturnsNullWhenEmpty() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNull(result);
  }

  @Test
  public void testGetRoutingTableWithBrokerRequestOnly() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    Map<ServerInstance, SegmentsToQuery> serverMap = createMockServerMap("server1", Arrays.asList("seg1"));
    RoutingTable localTable = new RoutingTable(serverMap, Collections.emptyList(), 0);

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, REQUEST_ID);

    assertNotNull(result);
    verify(_localClusterRoutingManager).getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);
  }

  @Test
  public void testGetTimeBoundaryInfoFromLocal() {
    TimeBoundaryInfo timeBoundaryInfo = mock(TimeBoundaryInfo.class);
    when(_localClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE)).thenReturn(timeBoundaryInfo);

    TimeBoundaryInfo result = _multiClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result, timeBoundaryInfo);
    verify(_remoteClusterRoutingManager1, never()).getTimeBoundaryInfo(anyString());
  }

  @Test
  public void testGetTimeBoundaryInfoFromRemote() {
    TimeBoundaryInfo timeBoundaryInfo = mock(TimeBoundaryInfo.class);
    when(_localClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getTimeBoundaryInfo(TEST_TABLE)).thenReturn(timeBoundaryInfo);

    TimeBoundaryInfo result = _multiClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result, timeBoundaryInfo);
  }

  @Test
  public void testGetEnabledServerInstanceMap() {
    ServerInstance server1 = createMockServerInstance("server1");
    ServerInstance server2 = createMockServerInstance("server2");
    ServerInstance server3 = createMockServerInstance("server3");

    Map<String, ServerInstance> localMap = new HashMap<>();
    localMap.put("server1", server1);

    Map<String, ServerInstance> remoteMap1 = new HashMap<>();
    remoteMap1.put("server2", server2);

    Map<String, ServerInstance> remoteMap2 = new HashMap<>();
    remoteMap2.put("server3", server3);

    when(_localClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(localMap);
    when(_remoteClusterRoutingManager1.getEnabledServerInstanceMap()).thenReturn(remoteMap1);
    when(_remoteClusterRoutingManager2.getEnabledServerInstanceMap()).thenReturn(remoteMap2);

    Map<String, ServerInstance> result = _multiClusterRoutingManager.getEnabledServerInstanceMap();

    assertEquals(result.size(), 3);
    assertTrue(result.containsKey("server1"));
    assertTrue(result.containsKey("server2"));
    assertTrue(result.containsKey("server3"));
  }

  @Test
  public void testGetTablePartitionInfo() {
    TablePartitionInfo partitionInfo = mock(TablePartitionInfo.class);
    when(_localClusterRoutingManager.getTablePartitionInfo(TEST_TABLE)).thenReturn(partitionInfo);

    TablePartitionInfo result = _multiClusterRoutingManager.getTablePartitionInfo(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result, partitionInfo);
  }

  @Test
  public void testGetServingInstancesFromLocal() {
    Set<String> localInstances = new HashSet<>(Arrays.asList("server1", "server2"));
    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(localInstances);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE)).thenReturn(null);
    when(_remoteClusterRoutingManager2.getServingInstances(TEST_TABLE)).thenReturn(null);

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("server1"));
    assertTrue(result.contains("server2"));
  }

  @Test
  public void testGetServingInstancesCombinesAll() {
    Set<String> localInstances = createInstanceSet("server1");
    Set<String> remoteCluster1Instances = createInstanceSet("server2");
    Set<String> remoteCluster2Instances = createInstanceSet("server3");

    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(localInstances);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE)).thenReturn(remoteCluster1Instances);
    when(_remoteClusterRoutingManager2.getServingInstances(TEST_TABLE)).thenReturn(remoteCluster2Instances);

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertTrue(result.contains("server1"));
    assertTrue(result.contains("server2"));
    assertTrue(result.contains("server3"));
  }

  @Test
  public void testGetServingInstancesHandlesException() {
    Set<String> localInstances = new HashSet<>(Arrays.asList("server1"));
    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(localInstances);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE))
        .thenThrow(new RuntimeException("Error"));
    when(_remoteClusterRoutingManager2.getServingInstances(TEST_TABLE)).thenReturn(null);

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertTrue(result.contains("server1"));
  }

  @Test
  public void testGetServingInstancesReturnsNullWhenEmpty() {
    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE)).thenReturn(null);
    when(_remoteClusterRoutingManager2.getServingInstances(TEST_TABLE)).thenReturn(null);

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNull(result);
  }

  @Test
  public void testGetSegments() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    List<String> localSegments = Arrays.asList("seg1", "seg2");
    List<String> remoteSegments1 = Arrays.asList("seg3");
    List<String> remoteSegments2 = Arrays.asList("seg4", "seg5");

    when(_localClusterRoutingManager.getSegments(brokerRequest)).thenReturn(localSegments);
    when(_remoteClusterRoutingManager1.getSegments(brokerRequest)).thenReturn(remoteSegments1);
    when(_remoteClusterRoutingManager2.getSegments(brokerRequest)).thenReturn(remoteSegments2);

    List<String> result = _multiClusterRoutingManager.getSegments(brokerRequest);

    assertNotNull(result);
    assertEquals(result.size(), 5);
    assertTrue(result.containsAll(Arrays.asList("seg1", "seg2", "seg3", "seg4", "seg5")));
  }

  @Test
  public void testGetSegmentsReturnsNullWhenAllNull() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    when(_localClusterRoutingManager.getSegments(brokerRequest)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getSegments(brokerRequest)).thenReturn(null);
    when(_remoteClusterRoutingManager2.getSegments(brokerRequest)).thenReturn(null);

    List<String> result = _multiClusterRoutingManager.getSegments(brokerRequest);

    assertNull(result);
  }

  @Test
  public void testGetSegmentsHandlesNullLocal() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    List<String> remoteSegments = Arrays.asList("seg1");
    when(_localClusterRoutingManager.getSegments(brokerRequest)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getSegments(brokerRequest)).thenReturn(remoteSegments);
    when(_remoteClusterRoutingManager2.getSegments(brokerRequest)).thenReturn(null);

    List<String> result = _multiClusterRoutingManager.getSegments(brokerRequest);

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertTrue(result.contains("seg1"));
  }

  @Test
  public void testGetSegmentsHandlesException() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    List<String> localSegments = Arrays.asList("seg1");
    when(_localClusterRoutingManager.getSegments(brokerRequest)).thenReturn(localSegments);
    when(_remoteClusterRoutingManager1.getSegments(brokerRequest))
        .thenThrow(new RuntimeException("Error"));
    when(_remoteClusterRoutingManager2.getSegments(brokerRequest)).thenReturn(Arrays.asList("seg2"));

    List<String> result = _multiClusterRoutingManager.getSegments(brokerRequest);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("seg1"));
    assertTrue(result.contains("seg2"));
  }

  @Test
  public void testGetTablePartitionReplicatedServersInfo() {
    TablePartitionReplicatedServersInfo info = mock(TablePartitionReplicatedServersInfo.class);
    when(_localClusterRoutingManager.getTablePartitionReplicatedServersInfo(TEST_TABLE)).thenReturn(info);

    TablePartitionReplicatedServersInfo result =
        _multiClusterRoutingManager.getTablePartitionReplicatedServersInfo(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result, info);
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a mock BrokerRequest with the specified table name.
   */
  private BrokerRequest createMockBrokerRequest(String tableName) {
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    QuerySource querySource = mock(QuerySource.class);
    when(querySource.getTableName()).thenReturn(tableName);
    when(brokerRequest.getQuerySource()).thenReturn(querySource);
    return brokerRequest;
  }

  /**
   * Creates a server-to-segments mapping for a single server.
   */
  private Map<ServerInstance, SegmentsToQuery> createMockServerMap(String serverName, List<String> segments) {
    Map<ServerInstance, SegmentsToQuery> serverMap = new HashMap<>();
    ServerInstance server = createMockServerInstance(serverName);
    SegmentsToQuery segmentsToQuery = new SegmentsToQuery(segments, Collections.emptyList());
    serverMap.put(server, segmentsToQuery);
    return serverMap;
  }

  /**
   * Creates a mock ServerInstance with the specified instance name.
   */
  private ServerInstance createMockServerInstance(String instanceName) {
    ServerInstance server = mock(ServerInstance.class);
    when(server.getInstanceId()).thenReturn(instanceName);
    when(server.getHostname()).thenReturn("localhost");
    when(server.getPort()).thenReturn(DEFAULT_SERVER_PORT);
    return server;
  }

  /**
   * Creates a RoutingTable for a single server with the specified segments.
   */
  private RoutingTable createRoutingTable(ServerInstance server, List<String> segments,
      List<String> optionalSegments) {
    Map<ServerInstance, SegmentsToQuery> serverMap = new HashMap<>();
    SegmentsToQuery segmentsToQuery = new SegmentsToQuery(segments, optionalSegments);
    serverMap.put(server, segmentsToQuery);
    return new RoutingTable(serverMap, Collections.emptyList(), 0);
  }

  /**
   * Calculates the total number of segments across all servers in a RoutingTable.
   */
  private int getTotalSegmentCount(RoutingTable routingTable) {
    return routingTable.getServerInstanceToSegmentsMap().values().stream()
        .mapToInt(s -> s.getSegments().size())
        .sum();
  }

  /**
   * Creates a Set containing the specified server instances.
   */
  private Set<String> createInstanceSet(String... instances) {
    return new HashSet<>(Arrays.asList(instances));
  }
}
