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
package org.apache.pinot.core.transport;

import com.google.common.util.concurrent.Futures;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class QueryRoutingTest {
  private static final int TEST_PORT = 12345;
  private static final ServerInstance SERVER_INSTANCE = new ServerInstance("localhost", TEST_PORT);
  private static final ServerRoutingInstance OFFLINE_SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
  private static final ServerRoutingInstance REALTIME_SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.REALTIME, ServerInstance.RoutingType.NETTY);
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
  private static final Map<ServerInstance, List<String>> ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE, Collections.emptyList());

  private QueryRouter _queryRouter;
  private ServerRoutingStatsManager _serverRoutingStatsManager;
  int _requestCount;

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(properties);
    _serverRoutingStatsManager = new ServerRoutingStatsManager(cfg);
    _serverRoutingStatsManager.init();
    _queryRouter = new QueryRouter("testBroker", mock(BrokerMetrics.class), _serverRoutingStatsManager);
    _requestCount = 0;
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes) {
    InstanceRequestHandler handler = new InstanceRequestHandler("server01", new PinotConfiguration(),
        mockQueryScheduler(responseDelayMs, responseBytes), mock(ServerMetrics.class), mock(AccessControl.class));
    return new QueryServer(TEST_PORT, null, handler);
  }

  private QueryScheduler mockQueryScheduler(int responseDelayMs, byte[] responseBytes) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(invocation -> {
      Thread.sleep(responseDelayMs);
      return Futures.immediateFuture(responseBytes);
    });
    return queryScheduler;
  }

  @Test
  public void testValidResponse()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 600_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    // 2 requests - query submit and query response.
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // REALTIME only
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Hybrid
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, BROKER_REQUEST, ROUTING_TABLE,
            1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testInvalidResponse()
      throws Exception {
    long requestId = 123;
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(0, new byte[0]);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testNonMatchingRequestId()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testServerDown()
      throws Exception {
    long requestId = 123;
    // To avoid flakyness, set timeoutMs to 2000 msec. For some test runs, it can take up to
    // 1400 msec to mark request as failed.
    long timeoutMs = 2000L;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(500, responseBytes);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, timeoutMs);

    // Shut down the server before getting the response
    queryServer.shutDown();

    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    assertTrue(System.currentTimeMillis() - startTimeMs < timeoutMs);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Submit query after server is down
    startTimeMs = System.currentTimeMillis();
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, timeoutMs);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getSubmitDelayMs(), -1);
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    assertTrue(System.currentTimeMillis() - startTimeMs < timeoutMs);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  private void waitForStatsUpdate(long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (_serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 5L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }

  @AfterClass
  public void tearDown() {
    _queryRouter.shutDown();
  }

  private QueryServer startServer(String instanceName, int port, byte[] responseBytes) {
    InstanceRequestHandler handler =
        new InstanceRequestHandler(instanceName, new PinotConfiguration(), mockQueryScheduler(0, responseBytes),
            mock(ServerMetrics.class), mock(AccessControl.class));
    return new QueryServer(port, null, handler);
  }

  @Test
  public void testValidResponseWithShortCircuit()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    QueryServer qs1 = startServer("server01", 1111, responseBytes);
    QueryServer qs2 = startServer("server02", 1112, responseBytes);
    QueryServer qs3 = startServer("server03", 1113, responseBytes);
    QueryServer qs4 = startServer("server04", 1114, responseBytes);
    QueryServer qs5 = startServer("server05", 1115, responseBytes);

    qs1.start();
    qs2.start();
    qs3.start();
    qs4.start();
    qs5.start();

    ServerInstance serverInstance1 = new ServerInstance("localhost", 1111);
    ServerInstance serverInstance2 = new ServerInstance("localhost", 1112);
    ServerInstance serverInstance3 = new ServerInstance("localhost", 1113);
    ServerInstance serverInstance4 = new ServerInstance("localhost", 1114);
    ServerInstance serverInstance5 = new ServerInstance("localhost", 1115);

    Map<ServerInstance, List<String>> routingTable = new HashMap<>();
    routingTable.put(serverInstance1, Collections.emptyList());
    routingTable.put(serverInstance2, Collections.emptyList());
    routingTable.put(serverInstance3, Collections.emptyList());
    routingTable.put(serverInstance4, Collections.emptyList());
    routingTable.put(serverInstance5, Collections.emptyList());

    {
      // Offline Only

      String query = "SELECT * FROM testTable LIMIT 2";
      BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

      AsyncQueryResponse asyncQueryResponse =
          _queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, 600_000L);
      Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses(queryContext);
      assertEquals(response.size(), 5);

      ServerRoutingInstance offlineServerRoutingInstance =
          serverInstance1.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      assertTrue(response.containsKey(offlineServerRoutingInstance));
      ServerResponse serverResponse = response.get(offlineServerRoutingInstance);
      assertNotNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseSize(), responseBytes.length);

      offlineServerRoutingInstance =
          serverInstance2.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      assertTrue(response.containsKey(offlineServerRoutingInstance));
      serverResponse = response.get(offlineServerRoutingInstance);
      assertNotNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseSize(), responseBytes.length);

      offlineServerRoutingInstance =
          serverInstance3.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      assertTrue(response.containsKey(offlineServerRoutingInstance));
      serverResponse = response.get(offlineServerRoutingInstance);
      assertNotNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseSize(), responseBytes.length);

      offlineServerRoutingInstance =
          serverInstance4.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      assertTrue(response.containsKey(offlineServerRoutingInstance));
      serverResponse = response.get(offlineServerRoutingInstance);
      assertNotNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseSize(), responseBytes.length);

      offlineServerRoutingInstance =
          serverInstance5.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      assertTrue(response.containsKey(offlineServerRoutingInstance));
      serverResponse = response.get(offlineServerRoutingInstance);
      assertNotNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseSize(), responseBytes.length);

      _requestCount += 10;
      waitForStatsUpdate(_requestCount);
      assertEquals(
          _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance1.getInstanceId()).intValue(), 0);
      assertEquals(
          _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance2.getInstanceId()).intValue(), 0);
      assertEquals(
          _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance3.getInstanceId()).intValue(), 0);
      assertEquals(
          _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance4.getInstanceId()).intValue(), 0);
      assertEquals(
          _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance5.getInstanceId()).intValue(), 0);
    }

    // Shut down the server
    qs1.shutDown();
    qs2.shutDown();
    qs3.shutDown();
    qs4.shutDown();
    qs5.shutDown();
  }
}
