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
package org.apache.pinot.broker.routing.manager;

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
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link MultiClusterRoutingManager}.
 */
public class MultiClusterRoutingManagerTest {
  private static final String TEST_TABLE = "testTable_OFFLINE";
  private static final long REQUEST_ID = 12345L;

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
  public void testRoutingExistsShortCircuits() {
    when(_localClusterRoutingManager.routingExists(TEST_TABLE)).thenReturn(true);

    boolean exists = _multiClusterRoutingManager.routingExists(TEST_TABLE);

    assertTrue(exists);
    verify(_remoteClusterRoutingManager1, never()).routingExists(anyString());
  }

  @Test
  public void testRoutingExistsChecksRemote() {
    when(_localClusterRoutingManager.routingExists(TEST_TABLE)).thenReturn(false);
    when(_remoteClusterRoutingManager1.routingExists(TEST_TABLE)).thenReturn(false);
    when(_remoteClusterRoutingManager2.routingExists(TEST_TABLE)).thenReturn(true);

    boolean exists = _multiClusterRoutingManager.routingExists(TEST_TABLE);

    assertTrue(exists);
  }

  @Test
  public void testGetRoutingTableCombinesLocalAndRemote() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);

    RoutingTable localTable = createRoutingTable("localServer", Arrays.asList("seg1"));
    RoutingTable remoteTable = createRoutingTable("remoteServer", Arrays.asList("seg2"));

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(remoteTable);
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 2);
  }

  @Test
  public void testGetRoutingTableHandlesRemoteException() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    RoutingTable localTable = createRoutingTable("localServer", Arrays.asList("seg1"));

    when(_localClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(localTable);
    when(_remoteClusterRoutingManager1.getRoutingTable(any(), anyString(), anyLong()))
        .thenThrow(new RuntimeException("Remote error"));
    when(_remoteClusterRoutingManager2.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID))
        .thenReturn(null);

    RoutingTable result = _multiClusterRoutingManager.getRoutingTable(brokerRequest, TEST_TABLE, REQUEST_ID);

    assertNotNull(result);
    assertEquals(result.getServerInstanceToSegmentsMap().size(), 1);
  }

  @Test
  public void testGetRoutingTableReturnsNullWhenAllNull() {
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
  public void testGetTimeBoundaryInfoFindsFirst() {
    TimeBoundaryInfo timeBoundaryInfo = mock(TimeBoundaryInfo.class);
    when(_localClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getTimeBoundaryInfo(TEST_TABLE)).thenReturn(timeBoundaryInfo);

    TimeBoundaryInfo result = _multiClusterRoutingManager.getTimeBoundaryInfo(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result, timeBoundaryInfo);
    verify(_remoteClusterRoutingManager2, never()).getTimeBoundaryInfo(anyString());
  }

  @Test
  public void testGetServingInstancesCombinesAll() {
    Set<String> localInstances = new HashSet<>(Arrays.asList("server1"));
    Set<String> remoteInstances = new HashSet<>(Arrays.asList("server2"));

    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(localInstances);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE)).thenReturn(remoteInstances);
    when(_remoteClusterRoutingManager2.getServingInstances(TEST_TABLE)).thenReturn(null);

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("server1"));
    assertTrue(result.contains("server2"));
  }

  @Test
  public void testGetServingInstancesHandlesException() {
    Set<String> localInstances = new HashSet<>(Arrays.asList("server1"));
    when(_localClusterRoutingManager.getServingInstances(TEST_TABLE)).thenReturn(localInstances);
    when(_remoteClusterRoutingManager1.getServingInstances(TEST_TABLE))
        .thenThrow(new RuntimeException("Error"));

    Set<String> result = _multiClusterRoutingManager.getServingInstances(TEST_TABLE);

    assertNotNull(result);
    assertEquals(result.size(), 1);
  }

  @Test
  public void testGetSegmentsHandlesNulls() {
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
  public void testGetSegmentsReturnsNullWhenAllNull() {
    BrokerRequest brokerRequest = createMockBrokerRequest(TEST_TABLE);
    when(_localClusterRoutingManager.getSegments(brokerRequest)).thenReturn(null);
    when(_remoteClusterRoutingManager1.getSegments(brokerRequest)).thenReturn(null);
    when(_remoteClusterRoutingManager2.getSegments(brokerRequest)).thenReturn(null);

    List<String> result = _multiClusterRoutingManager.getSegments(brokerRequest);

    assertNull(result);
  }

  @Test
  public void testGetEnabledServerInstanceMapCombinesAll() {
    ServerInstance server1 = createMockServerInstance("server1");
    ServerInstance server2 = createMockServerInstance("server2");

    Map<String, ServerInstance> localMap = new HashMap<>();
    localMap.put("server1", server1);

    Map<String, ServerInstance> remoteMap = new HashMap<>();
    remoteMap.put("server2", server2);

    when(_localClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(localMap);
    when(_remoteClusterRoutingManager1.getEnabledServerInstanceMap()).thenReturn(remoteMap);
    when(_remoteClusterRoutingManager2.getEnabledServerInstanceMap()).thenReturn(new HashMap<>());

    Map<String, ServerInstance> result = _multiClusterRoutingManager.getEnabledServerInstanceMap();

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("server1"));
    assertTrue(result.containsKey("server2"));
  }

  // Helper methods

  private BrokerRequest createMockBrokerRequest(String tableName) {
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    QuerySource querySource = mock(QuerySource.class);
    when(querySource.getTableName()).thenReturn(tableName);
    when(brokerRequest.getQuerySource()).thenReturn(querySource);
    return brokerRequest;
  }

  private RoutingTable createRoutingTable(String serverName, List<String> segments) {
    Map<ServerInstance, SegmentsToQuery> serverMap = new HashMap<>();
    ServerInstance server = createMockServerInstance(serverName);
    SegmentsToQuery segmentsToQuery = new SegmentsToQuery(segments, Collections.emptyList());
    serverMap.put(server, segmentsToQuery);
    return new RoutingTable(serverMap, Collections.emptyList(), 0);
  }

  private ServerInstance createMockServerInstance(String instanceName) {
    ServerInstance server = mock(ServerInstance.class);
    when(server.getInstanceId()).thenReturn(instanceName);
    return server;
  }
}
