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
package org.apache.pinot.core.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LogicalTableRouteProviderCalculateRouteTest extends BaseTableRouteTest {

  private void assertTableRoute(String tableName, String logicalTableName,
      Map<String, Set<String>> expectedOfflineRoutingTable, Map<String, Set<String>> expectedRealtimeRoutingTable,
      boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, logicalTableName);
    BrokerRequestPair brokerRequestPair =
        getBrokerRequestPair(logicalTableName, routeInfo.hasOffline(), routeInfo.hasRealtime(),
            routeInfo.getOfflineTableName(), routeInfo.getRealtimeTableName());

    _logicalTableRouteProvider.calculateRoutes(routeInfo, _routingManager, brokerRequestPair._offlineBrokerRequest,
        brokerRequestPair._realtimeBrokerRequest, 0);
    LogicalTableRouteInfo logicalTableRouteInfo = (LogicalTableRouteInfo) routeInfo;

    if (isOfflineExpected) {
      assertNotNull(logicalTableRouteInfo.getOfflineTables());
      assertEquals(logicalTableRouteInfo.getOfflineTables().size(), 1);
      Map<ServerInstance, SegmentsToQuery> offlineRoutingTable =
          logicalTableRouteInfo.getOfflineTables().get(0).getOfflineRoutingTable();
      assertNotNull(offlineRoutingTable);
      assertRoutingTableEqual(offlineRoutingTable, expectedOfflineRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getOfflineExecutionServers().isEmpty());
    }

    if (isRealtimeExpected) {
      assertNotNull(logicalTableRouteInfo.getRealtimeTables());
      assertEquals(logicalTableRouteInfo.getRealtimeTables().size(), 1);
      Map<ServerInstance, SegmentsToQuery> realtimeRoutingTable =
          logicalTableRouteInfo.getRealtimeTables().get(0).getRealtimeRoutingTable();
      assertNotNull(realtimeRoutingTable);
      assertRoutingTableEqual(realtimeRoutingTable, expectedRealtimeRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getRealtimeExecutionServers().isEmpty());
    }

    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
      // Check requestMap
      Map<ServerRoutingInstance, InstanceRequest> requestMap = routeInfo.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }

    if (routeInfo.isHybrid()) {
      assertNotNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should not be null for hybrid table");
    } else {
      assertNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should be null for non-hybrid table");
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testOfflineTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "offlineTableAndRouteProvider", expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testRealtimeTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "realtimeTableAndRouteProvider", null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testHybridTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "hybridTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testRouteNotExists(String tableName) {
    assertTableRoute(tableName, "routeNotExistsProvider", null, null, false, false);
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "partiallyDisabledTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "disabledTableProvider")
  void testDisabledTable(String tableName) {
    assertTableRoute(tableName, "disabledTableProvider", null, null, false, false);
  }

  @Test
  void testGetRequestMapWithTlsEnabled() {
    int server1NettyPort = 1;
    int server2NettyPort = 2;
    int server1TlsPort = 8091;
    int server2TlsPort = 8092;

    ServerInstance server1 = createServerInstanceWithTls("localhost", server1NettyPort, server1TlsPort);
    ServerInstance server2 = createServerInstanceWithTls("localhost", server2NettyPort, server2TlsPort);

    // Create offline physical table with routing table
    ImplicitHybridTableRouteInfo offlinePhysical = new ImplicitHybridTableRouteInfo();
    offlinePhysical.setOfflineTableName("physical_OFFLINE");
    offlinePhysical.setOfflineRoutingTable(Map.of(
        server1, new SegmentsToQuery(new ArrayList<>(List.of("seg1")), null),
        server2, new SegmentsToQuery(new ArrayList<>(List.of("seg2")), null)));

    // Create realtime physical table with routing table
    ImplicitHybridTableRouteInfo realtimePhysical = new ImplicitHybridTableRouteInfo();
    realtimePhysical.setRealtimeTableName("physical_REALTIME");
    realtimePhysical.setRealtimeRoutingTable(Map.of(
        server1, new SegmentsToQuery(new ArrayList<>(List.of("seg3")), null)));

    // Build logical table route info
    LogicalTableRouteInfo logicalRouteInfo = new LogicalTableRouteInfo();
    logicalRouteInfo.setLogicalTableName("testLogicalTable");
    logicalRouteInfo.setOfflineTables(List.of(offlinePhysical));
    logicalRouteInfo.setRealtimeTables(List.of(realtimePhysical));

    BrokerRequestPair pair = getBrokerRequestPair("testLogicalTable", true, true,
        "testLogicalTable_OFFLINE", "testLogicalTable_REALTIME");
    logicalRouteInfo.setOfflineBrokerRequest(pair._offlineBrokerRequest);
    logicalRouteInfo.setRealtimeBrokerRequest(pair._realtimeBrokerRequest);

    // Verify preferTls=true uses TLS port and enables TLS
    Map<ServerRoutingInstance, InstanceRequest> tlsRequestMap = logicalRouteInfo.getRequestMap(0, "broker", true);
    assertNotNull(tlsRequestMap);
    assertEquals(tlsRequestMap.size(), 3); // 2 offline + 1 realtime
    for (ServerRoutingInstance routingInstance : tlsRequestMap.keySet()) {
      assertTrue(routingInstance.isTlsEnabled(), "TLS should be enabled when preferTls=true");
      int port = routingInstance.getPort();
      assertTrue(port == server1TlsPort || port == server2TlsPort,
          "Port should be TLS port, got: " + port);
    }

    // Verify preferTls=false uses regular port without TLS
    Map<ServerRoutingInstance, InstanceRequest> nonTlsRequestMap = logicalRouteInfo.getRequestMap(0, "broker", false);
    assertNotNull(nonTlsRequestMap);
    assertEquals(nonTlsRequestMap.size(), 3);
    for (ServerRoutingInstance routingInstance : nonTlsRequestMap.keySet()) {
      assertFalse(routingInstance.isTlsEnabled(), "TLS should be disabled when preferTls=false");
      int port = routingInstance.getPort();
      assertTrue(port == server1NettyPort || port == server2NettyPort,
          "Port should be regular netty port, got: " + port);
    }
  }

  private static ServerInstance createServerInstanceWithTls(String hostname, int nettyPort, int tlsPort) {
    String server = String.format("%s%s_%d", CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE, hostname, nettyPort);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.NETTY_TLS_PORT_KEY, String.valueOf(tlsPort));
    return new ServerInstance(instanceConfig);
  }
}
