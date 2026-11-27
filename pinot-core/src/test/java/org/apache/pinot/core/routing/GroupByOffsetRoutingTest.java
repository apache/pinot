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

import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests the query sent to a server for single-stage GROUP BY pagination.
public class GroupByOffsetRoutingTest {
  @Test
  public void testOfflineAndHybridRoutes() {
    ServerInstance server = new ServerInstance(InstanceConfig.toInstanceConfig("Server_localhost_1"));
    BrokerRequest offline = groupByRequest("test_OFFLINE");
    BrokerRequest realtime = groupByRequest("test_REALTIME");
    Map<ServerInstance, SegmentsToQuery> segments = Map.of(server, new SegmentsToQuery(List.of("seg"), null));

    TableRouteInfo offlineRoute = new ImplicitHybridTableRouteInfo(offline, null, segments, null);
    assertRequests(offlineRoute, 1);
    TableRouteInfo hybridRoute = new ImplicitHybridTableRouteInfo(offline, realtime, segments, segments);
    assertRequests(hybridRoute, 2);
    assertOriginalRequest(offline);
    assertOriginalRequest(realtime);
  }

  @Test
  public void testLogicalRoute() {
    ServerInstance server = new ServerInstance(InstanceConfig.toInstanceConfig("Server_localhost_1"));
    BrokerRequest offline = groupByRequest("logical_OFFLINE");
    BrokerRequest realtime = groupByRequest("logical_REALTIME");
    Map<ServerInstance, SegmentsToQuery> segments = Map.of(server, new SegmentsToQuery(List.of("seg"), null));

    ImplicitHybridTableRouteInfo physicalRoute = new ImplicitHybridTableRouteInfo(offline, realtime, segments,
        segments);
    physicalRoute.setOfflineTableName("physical_OFFLINE");
    physicalRoute.setRealtimeTableName("physical_REALTIME");
    LogicalTableRouteInfo logicalRoute = new LogicalTableRouteInfo();
    logicalRoute.setOfflineTables(List.of(physicalRoute));
    logicalRoute.setRealtimeTables(List.of(physicalRoute));
    logicalRoute.setOfflineBrokerRequest(offline);
    logicalRoute.setRealtimeBrokerRequest(realtime);

    assertRequests(logicalRoute, 2);
    assertOriginalRequest(offline);
    assertOriginalRequest(realtime);
  }

  private static BrokerRequest groupByRequest(String tableName) {
    return CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(
        "SELECT k, COUNT(*) AS c FROM " + tableName + " GROUP BY k ORDER BY c DESC LIMIT 10 OFFSET 1000"));
  }

  private static void assertRequests(TableRouteInfo route, int expectedSize) {
    Map<ServerRoutingInstance, InstanceRequest> requests = route.getRequestMap(1, "broker", false);
    assertEquals(requests.size(), expectedSize);
    for (Map.Entry<ServerRoutingInstance, InstanceRequest> entry : requests.entrySet()) {
      PinotQuery query = entry.getValue().getQuery().getPinotQuery();
      BrokerRequest original = entry.getKey().getTableType() == TableType.OFFLINE
          ? route.getOfflineBrokerRequest() : route.getRealtimeBrokerRequest();
      assertEquals(query.getDataSource(), original.getPinotQuery().getDataSource());
      assertEquals(query.getLimit(), 1010);
      assertEquals(query.getOffset(), 0);
    }
  }

  private static void assertOriginalRequest(BrokerRequest request) {
    assertEquals(request.getPinotQuery().getLimit(), 10);
    assertEquals(request.getPinotQuery().getOffset(), 1000);
  }
}
