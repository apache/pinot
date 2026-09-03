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
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class QueryRoutingTest {
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");

  private QueryRouter _queryRouter;
  private ServerRoutingStatsManager _serverRoutingStatsManager;
  int _requestCount;
  private QueryServer _queryServer;
  private ServerInstance _serverInstance;
  private ServerRoutingInstance _offlineServerRoutingInstance;
  private ServerRoutingInstance _realtimeServerRoutingInstance;
  private Map<ServerInstance, SegmentsToQuery> _routingTable;

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(properties);
    _serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, mock(BrokerMetrics.class));
    _serverRoutingStatsManager.init();
    _queryRouter = new QueryRouter("testBroker", null, null, _serverRoutingStatsManager,
        ThreadAccountantUtils.getNoOpAccountant());
    _requestCount = 0;
  }

  @AfterMethod
  void shutdownServer() {
    try {
      if (_queryServer != null && _queryServer.getChannel() != null) {
        // shutDown() blocks on channel.close().sync(), so port is released when this returns
        _queryServer.shutDown();
      }
    } finally {
      clearTestFixtures();
    }
  }

  @AfterMethod
  void deregisterServerMetrics() {
    ServerMetrics.deregister();
  }

  private void clearTestFixtures() {
    _queryServer = null;
    _serverInstance = null;
    _offlineServerRoutingInstance = null;
    _realtimeServerRoutingInstance = null;
    _routingTable = null;
  }

  private void initializeTestFixtures(int port) {
    _serverInstance = new ServerInstance("localhost", port);
    _offlineServerRoutingInstance =
        _serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    _realtimeServerRoutingInstance =
        _serverInstance.toServerRoutingInstance(TableType.REALTIME, ServerInstance.RoutingType.NETTY);
    _routingTable = Map.of(_serverInstance,
        new SegmentsToQuery(List.of(), List.of()));
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes) {
    return getQueryServer(responseDelayMs, responseBytes, 0);
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes, int port) {
    InstanceRequestHandler handler = new InstanceRequestHandler("server01", new PinotConfiguration(),
        mockQueryScheduler(responseDelayMs, responseBytes), mock(AccessControl.class),
        ThreadAccountantUtils.getNoOpAccountant());
    return new QueryServer(port, null, handler);
  }

  /// Starts the query server and returns the actual bound port. Uses port 0 to let the OS assign a free port,
  /// avoiding TOCTOU race conditions. [QueryServer#start()] blocks on `bind().sync()`, so the
  /// server is ready to accept connections when this method returns.
  private int startAndGetPort(QueryServer server) {
    server.start();
    return ((InetSocketAddress) server.getChannel().localAddress()).getPort();
  }

  private QueryScheduler mockQueryScheduler(int responseDelayMs, byte[] responseBytes) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(invocation -> {
      Thread.sleep(responseDelayMs);
      return Futures.immediateFuture(responseBytes);
    });
    return queryScheduler;
  }

  private QueryRouter newIsolatedQueryRouter() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    ServerRoutingStatsManager statsManager =
        new ServerRoutingStatsManager(new PinotConfiguration(properties), mock(BrokerMetrics.class));
    statsManager.init();
    return new QueryRouter("testBroker", null, null, statsManager, ThreadAccountantUtils.getNoOpAccountant());
  }

  @Test
  public void testValidResponse()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race, then initialize test fixtures from actual port
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 600_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    // 2 requests - query submit and query response.
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // REALTIME only
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, _routingTable, 1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_realtimeServerRoutingInstance));
    serverResponse = response.get(_realtimeServerRoutingInstance);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Hybrid
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, BROKER_REQUEST, _routingTable,
            1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    serverResponse = response.get(_offlineServerRoutingInstance);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    assertTrue(response.containsKey(_realtimeServerRoutingInstance));
    serverResponse = response.get(_realtimeServerRoutingInstance);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  @Test
  public void testInvalidResponse()
      throws Exception {
    long requestId = 123;

    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, new byte[0]);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    // A request left unanswered until the deadline produces a timeout and nothing else: the send succeeded and the
    // channel was never torn down, so no failed server is reported. This is why the broker's failure detector cannot
    // rely on getFailedServer() alone -- see FailureDetector#notifyServerNotResponded.
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.TIMED_OUT);
    assertNull(asyncQueryResponse.getException());
    assertNull(asyncQueryResponse.getFailedServer());
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  @Test
  public void testLatencyForQueryServerException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.SERVER_TABLE_MISSING, "Test error message");
    byte[] responseBytes = dataTable.toBytes();
    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    // Send a query with ServerSide exception and check if the latency is set to timeout value.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // This means that no queries were run before this test. So we can just make sure that latencyAfter is equal to
      //666.334.
      // This corresponds to the EWMA value when a latency timeout value of 1000 is set. Latency set to timeout value
      //when server side exception occurs.
      double serverEWMALatency = 666.334;
      // Leaving an error budget of 2%
      double delta = 13.32;
      assertEquals(latencyAfter, serverEWMALatency, delta);
    } else {
      assertTrue(latencyAfter > latencyBefore, latencyAfter + " should be greater than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForClientException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.QUERY_CANCELLATION, "Test error message");
    byte[] responseBytes = dataTable.toBytes();
    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    // Send a query with client side errors.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);

    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // Latency for the server with client side exception is assigned as serverResponse.getResponseDelayMs() and the
      //calculated
      // EWMLatency for the server will be less than serverResponse.getResponseDelayMs()
      assertTrue(latencyAfter <= serverResponse.getResponseDelayMs());
    } else {
      assertTrue(latencyAfter < latencyBefore, latencyAfter + " should be lesser than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForMultipleExceptions()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.QUERY_CANCELLATION, "Test cancellation error message");
    dataTable.addException(QueryErrorCode.SERVER_TABLE_MISSING, "Test table missing error message");
    byte[] responseBytes = dataTable.toBytes();
    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    // Send a query with multiple exceptions. Make sure that the latency is set to timeout value even if a single
    //server-side exception is seen.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // This means that no queries where run before this test. So we can just make sure that latencyAfter is equal
      //to 666.334.
      // This corresponds to the EWMA value when a latency timeout value of 1000 is set.
      double serverEWMALatency = 666.334;
      // Leaving an error budget of 2%
      double delta = 13.32;
      assertEquals(latencyAfter, serverEWMALatency, delta);
    } else {
      assertTrue(latencyAfter > latencyBefore, latencyAfter + " should be greater than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForNoException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    // Send a valid query and get latency
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // Latency for the server with no exceptions is assigned as serverResponse.getResponseDelayMs() and the calculated
      // EWMLatency for the server will be less than serverResponse.getResponseDelayMs()
      assertTrue(latencyAfter <= serverResponse.getResponseDelayMs());
    } else {
      assertTrue(latencyAfter < latencyBefore, latencyAfter + " should be lesser than " + latencyBefore);
    }
  }

  @Test
  public void testNonMatchingRequestId()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(0, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, _routingTable, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(_offlineServerRoutingInstance));
    ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
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

    // Start the server on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(500, responseBytes);
    initializeTestFixtures(startAndGetPort(_queryServer));
    String serverId = _serverInstance.getInstanceId();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, _routingTable, null, null, timeoutMs);

    // Shut down the server before getting the response
    _queryServer.shutDown();

    try {
      assertFalse(_queryServer.getChannel().isOpen());
      assertFalse(_queryServer.getChannel().isActive());

      Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
      assertEquals(response.size(), 1);
      assertTrue(response.containsKey(_offlineServerRoutingInstance));
      ServerResponse serverResponse = response.get(_offlineServerRoutingInstance);
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
          _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, _routingTable, null, null, timeoutMs);
      response = asyncQueryResponse.getFinalResponses();
      assertEquals(response.size(), 1);
      assertTrue(response.containsKey(_offlineServerRoutingInstance));
      serverResponse = response.get(_offlineServerRoutingInstance);
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
    } finally {
      // To be sure we don't close it again on the @AfterMethod method
      clearTestFixtures();
    }
  }

  @Test
  public void testSkipUnavailableServer()
      throws Exception {
    long requestId = 123;
    DataSchema dataSchema =
        new DataSchema(new String[]{"column1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    builder.startRow();
    builder.setColumn(0, "value1");
    builder.finishRow();
    DataTable dataTableSuccess = builder.build();
    Map<String, String> dataTableMetadata = dataTableSuccess.getMetadata();
    dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] successResponseBytes = dataTableSuccess.toBytes();

    // Start server1 on port 0 (OS-assigned) to avoid TOCTOU race
    _queryServer = getQueryServer(500, successResponseBytes, 0);
    int port1 = startAndGetPort(_queryServer);

    ServerInstance serverInstance1 = new ServerInstance("localhost", port1);
    // For server2 (unavailable server), use a reserved .invalid hostname so connection setup fails
    // deterministically without depending on a transiently free port.
    ServerInstance serverInstance2 = new ServerInstance("unavailable.invalid", port1);
    ServerRoutingInstance serverRoutingInstance1 =
        serverInstance1.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance serverRoutingInstance2 =
        serverInstance2.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    Map<ServerInstance, SegmentsToQuery> routingTable =
        Map.of(serverInstance1, new SegmentsToQuery(List.of(), List.of()),
            serverInstance2, new SegmentsToQuery(List.of(), List.of()));

    // Submit the query with skipUnavailableServers=true, the single started server should return a valid response
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable");
    long startTime = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, 10_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    ServerResponse serverResponse1 = response.get(serverRoutingInstance1);
    ServerResponse serverResponse2 = response.get(serverRoutingInstance2);
    assertNotNull(serverResponse1.getDataTable());
    assertNull(serverResponse2.getDataTable());
    assertTrue(serverResponse1.getResponseDelayMs() > 500);   // > response delay set by getQueryServer
    assertTrue(serverResponse2.getResponseDelayMs() < 100);   // connection refused, no delay
    assertTrue(System.currentTimeMillis() - startTime > 500); // > response delay set by getQueryServer
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance1.getInstanceId()).intValue(), 0);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance2.getInstanceId()).intValue(), 0);

    // Submit the same query without skipUnavailableServers, the servers should not return any response
    brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    startTime = System.currentTimeMillis();
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, 10_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    serverResponse1 = response.get(serverRoutingInstance1);
    serverResponse2 = response.get(serverRoutingInstance2);
    assertNull(serverResponse1.getDataTable());
    assertNull(serverResponse2.getDataTable());
    assertTrue(serverResponse1.getResponseDelayMs() < 100);
    assertTrue(serverResponse2.getResponseDelayMs() < 100);
    assertTrue(System.currentTimeMillis() - startTime < 100);
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance1.getInstanceId()).intValue(), 0);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance2.getInstanceId()).intValue(), 0);
  }

  @Test
  public void testSkipUnavailableServerChannelInactive()
      throws Exception {
    long requestId = 123;
    DataSchema dataSchema =
        new DataSchema(new String[]{"column1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    builder.startRow();
    builder.setColumn(0, "value1");
    builder.finishRow();
    DataTable dataTableSuccess = builder.build();
    dataTableSuccess.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] successResponseBytes = dataTableSuccess.toBytes();

    // The healthy server responds after a delay; the second server has a warm channel established during submitQuery
    // and its channel then goes inactive mid-flight (Path 2) before it can respond.
    long healthyDelayMs = 1000L;
    long timeoutMs = 10_000L;
    _queryServer = getQueryServer((int) healthyDelayMs, successResponseBytes, 0);
    int healthyPort = startAndGetPort(_queryServer);
    // Long delay so this server never actually responds during the test
    QueryServer unavailableServer = getQueryServer(60_000, successResponseBytes, 0);
    int unavailablePort = startAndGetPort(unavailableServer);
    // Isolated broker
    QueryRouter queryRouter = newIsolatedQueryRouter();

    try {
      ServerInstance healthyInstance = new ServerInstance("localhost", healthyPort);
      ServerInstance unavailableInstance = new ServerInstance("localhost", unavailablePort);
      ServerRoutingInstance healthyRoutingInstance =
          healthyInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      ServerRoutingInstance unavailableRoutingInstance =
          unavailableInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      Map<ServerInstance, SegmentsToQuery> routingTable =
          Map.of(healthyInstance, new SegmentsToQuery(List.of(), List.of()),
              unavailableInstance, new SegmentsToQuery(List.of(), List.of()));

      BrokerRequest brokerRequest =
          CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable");
      long startTime = System.currentTimeMillis();
      AsyncQueryResponse asyncQueryResponse =
          queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, timeoutMs);
      // Confirm the request was actually dispatched to the unavailable server (its channel went live) so this is a
      // genuine Path 2 (channel dies AFTER dispatch), not a send-time failure. The server is up, so the write succeeds
      // within milliseconds.
      ServerResponse unavailableDispatch = asyncQueryResponse.getCurrentResponses().get(unavailableRoutingInstance);
      TestUtils.waitForCondition(aVoid -> unavailableDispatch.getRequestSentDelayMs() >= 0, 10L, 5000L,
          "Request was not dispatched to the unavailable server");
      // Drive the mid-flight channel-inactive event through the real broker inbound handler
      new DataTableHandler(queryRouter, ThreadAccountantUtils.getNoOpAccountant(), unavailableRoutingInstance)
          .channelInactive(mock(ChannelHandlerContext.class));

      Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
      long elapsed = System.currentTimeMillis() - startTime;

      assertEquals(response.size(), 2);
      ServerResponse healthyResponse = response.get(healthyRoutingInstance);
      ServerResponse unavailableResponse = response.get(unavailableRoutingInstance);
      // The healthy server returned data; the unavailable server did not.
      assertNotNull(healthyResponse.getDataTable());
      assertNull(unavailableResponse.getDataTable());
      // No BROKER_REQUEST_SEND (425): the query degraded to partial results instead of failing.
      assertNull(asyncQueryResponse.getException());
      assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
      // The down server is still recorded so the failure detector can quarantine it from routing.
      assertEquals(asyncQueryResponse.getFailedServer(), unavailableRoutingInstance);
      // We waited for the healthy server (the latch was not force-drained) and returned well before the timeout.
      // If the bug were present, markQueryFailed would force-drain the latch and return almost immediately.
      assertTrue(elapsed >= healthyDelayMs, "Expected to wait for the healthy server, elapsed=" + elapsed);
      assertTrue(elapsed < timeoutMs, "Expected to return before timeout, elapsed=" + elapsed);
    } finally {
      unavailableServer.shutDown();
      queryRouter.shutDown();
    }
  }

  /// Control for [#testSkipUnavailableServerChannelInactive]: the same channel-inactive-mid-flight scenario but
  /// WITHOUT `skipUnavailableServers` must still fail the whole query
  @Test
  public void testChannelInactiveWithoutSkipFailsQuery()
      throws Exception {
    long requestId = 123;
    DataSchema dataSchema =
        new DataSchema(new String[]{"column1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    builder.startRow();
    builder.setColumn(0, "value1");
    builder.finishRow();
    DataTable dataTableSuccess = builder.build();
    dataTableSuccess.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] successResponseBytes = dataTableSuccess.toBytes();

    long timeoutMs = 10_000L;
    // Healthy server delay is set well above the deterministic fail-fast path
    long healthyDelayMs = 4000L;
    _queryServer = getQueryServer((int) healthyDelayMs, successResponseBytes, 0);
    int healthyPort = startAndGetPort(_queryServer);
    // Long delay so this server never actually responds during the test
    QueryServer unavailableServer = getQueryServer(60_000, successResponseBytes, 0);
    int unavailablePort = startAndGetPort(unavailableServer);
    // Isolated broker
    QueryRouter queryRouter = newIsolatedQueryRouter();

    try {
      ServerInstance healthyInstance = new ServerInstance("localhost", healthyPort);
      ServerInstance unavailableInstance = new ServerInstance("localhost", unavailablePort);
      ServerRoutingInstance unavailableRoutingInstance =
          unavailableInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
      Map<ServerInstance, SegmentsToQuery> routingTable =
          Map.of(healthyInstance, new SegmentsToQuery(List.of(), List.of()),
              unavailableInstance, new SegmentsToQuery(List.of(), List.of()));

      // No skipUnavailableServers option set.
      long startTime = System.currentTimeMillis();
      AsyncQueryResponse asyncQueryResponse =
          queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, routingTable, null, null, timeoutMs);
      // Confirm the request was actually dispatched to the unavailable server (its channel went live) so this is a
      // genuine Path 2 (channel dies AFTER dispatch), not a send-time failure.
      ServerResponse unavailableDispatch = asyncQueryResponse.getCurrentResponses().get(unavailableRoutingInstance);
      TestUtils.waitForCondition(aVoid -> unavailableDispatch.getRequestSentDelayMs() >= 0, 10L, 5000L,
          "Request was not dispatched to the unavailable server");
      // Drive the mid-flight channel-inactive event through the real broker inbound handler, deterministically (see the
      // rationale in testSkipUnavailableServerChannelInactive).
      new DataTableHandler(queryRouter, ThreadAccountantUtils.getNoOpAccountant(), unavailableRoutingInstance)
          .channelInactive(mock(ChannelHandlerContext.class));

      Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
      long elapsed = System.currentTimeMillis() - startTime;

      assertEquals(response.size(), 2);
      // Without the flag the query fails: the exception is set (becomes a 425 downstream) and status is FAILED.
      assertNotNull(asyncQueryResponse.getException());
      assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.FAILED);
      assertEquals(asyncQueryResponse.getFailedServer(), unavailableRoutingInstance);
      // The latch is force-drained, so the query fails fast — it returns before the healthy server's delay elapses
      // rather than waiting it out (and well before the timeout).
      assertTrue(elapsed < healthyDelayMs, "Expected to fail fast before the healthy server responded, elapsed="
          + elapsed);
    } finally {
      unavailableServer.shutDown();
      queryRouter.shutDown();
    }
  }

  @Test
  public void testChannelActiveInactiveEmitPerServerTaggedMeters() {
    PinotMetricUtils.init(new PinotConfiguration());
    // register() is a compareAndSet against NOOP with no deregister, so another test class may already have registered
    // a real instance. Either way, read from whatever DataTableHandler will read via BrokerMetrics.get().
    BrokerMetrics.register(new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    BrokerMetrics brokerMetrics = BrokerMetrics.get();
    PinotMetricsRegistry registry = brokerMetrics.getMetricsRegistry();

    ServerInstance serverInstance = new ServerInstance("localhost", 12345);
    ServerRoutingInstance routingInstance =
        serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    String shortName = routingInstance.getShortName();
    DataTableHandler handler =
        new DataTableHandler(_queryRouter, ThreadAccountantUtils.getNoOpAccountant(), routingInstance);

    long activeBefore = taggedMeterCount(registry, BrokerMeter.NETTY_CONNECTION_CHANNEL_ACTIVE, shortName);
    handler.channelActive(mock(ChannelHandlerContext.class));
    assertEquals(taggedMeterCount(registry, BrokerMeter.NETTY_CONNECTION_CHANNEL_ACTIVE, shortName), activeBefore + 1);

    // channelInactive also calls markServerUnavailable, but no query is in flight on _queryRouter so that is a no-op.
    long inactiveBefore = taggedMeterCount(registry, BrokerMeter.NETTY_CONNECTION_CHANNEL_INACTIVE, shortName);
    handler.channelInactive(mock(ChannelHandlerContext.class));
    assertEquals(taggedMeterCount(registry, BrokerMeter.NETTY_CONNECTION_CHANNEL_INACTIVE, shortName),
        inactiveBefore + 1);
  }

  private static long taggedMeterCount(PinotMetricsRegistry registry, BrokerMeter meter, String tag) {
    String fullName = CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX + meter.getMeterName() + "." + tag;
    return PinotMetricUtils.makePinotMeter(registry,
        PinotMetricUtils.makePinotMetricName(BrokerMetrics.class, fullName), meter.getUnit(), TimeUnit.SECONDS).count();
  }

  /// A server that answers queries answers a ping over the channel they use. Before any query there is no channel to
  /// ping over, and the ping must not open one either: that would make a server that only serves multi-stage queries
  /// look queried over this path.
  @Test
  public void testPingIsAnsweredByLiveServer()
      throws Exception {
    long requestId = 123;
    _queryServer = getQueryServer(0, dataTableBytesWithRequestId(requestId));
    initializeTestFixtures(startAndGetPort(_queryServer));
    QueryRouter queryRouter = newIsolatedQueryRouter();
    try {
      assertFalse(queryRouter.ping(_serverInstance, 5_000L));
      assertFalse(queryRouter.hasChannel(_serverInstance));

      assertNotNull(queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 5_000L)
          .getFinalResponses().get(_offlineServerRoutingInstance).getDataTable());
      assertTrue(queryRouter.ping(_serverInstance, 5_000L));
    } finally {
      queryRouter.shutDown();
    }
  }

  /// The property that keeps busy servers in routing: the ping is answered from the server's network thread, so it
  /// comes back at once even while a query to that server is stuck.
  @Test
  public void testPingIsAnsweredPromptlyWhileTheQueryEngineIsStuck() {
    _queryServer = QueryServerTestUtils.newQueryServer(invocation -> SettableFuture.<byte[]>create());
    initializeTestFixtures(startAndGetPort(_queryServer));
    QueryRouter queryRouter = newIsolatedQueryRouter();
    try {
      AsyncQueryResponse stuckQuery =
          queryRouter.submitQuery(123, "testTable", BROKER_REQUEST, _routingTable, null, null, 60_000L);

      long startTimeMs = System.currentTimeMillis();
      assertTrue(queryRouter.ping(_serverInstance, 5_000L));
      assertTrue(System.currentTimeMillis() - startTimeMs < 2_000L,
          "A server whose query engine is stuck must still answer a ping promptly");
      assertEquals(stuckQuery.getStatus(), QueryResponse.Status.IN_PROGRESS);
    } finally {
      queryRouter.shutDown();
    }
  }

  /// A frozen JVM or a vanished node: the kernel (or nothing at all) completes the TCP handshake, but nothing ever
  /// reads the request. Modeled by a listening socket that is never accepted from. The unanswered ping must also close
  /// the channel, or every later attempt would write into the same possibly half-open connection.
  @Test
  public void testPingGoesUnansweredByFrozenServer()
      throws Exception {
    long timeoutMs = 1_000L;
    QueryRouter queryRouter = newIsolatedQueryRouter();
    try (ServerSocket frozenServer = new ServerSocket(0)) {
      initializeTestFixtures(frozenServer.getLocalPort());
      // Opens the channel, and is never answered.
      AsyncQueryResponse query =
          queryRouter.submitQuery(123, "testTable", BROKER_REQUEST, _routingTable, null, null, 60_000L);

      long startTimeMs = System.currentTimeMillis();
      assertFalse(queryRouter.ping(_serverInstance, timeoutMs));
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      assertTrue(elapsedMs >= timeoutMs && elapsedMs < timeoutMs + 2_000L,
          "A ping to a frozen server must fail after the ping timeout, not sooner or much later; took " + elapsedMs);
      // Closing the channel also ends the query waiting on it, instead of leaving it to its own timeout.
      TestUtils.waitForCondition(aVoid -> query.getStatus() != QueryResponse.Status.IN_PROGRESS, 5_000L,
          "The query on the closed channel did not end");

      // Only now pick up the connection: past the query and the ping, the broker must have closed its end.
      frozenServer.setSoTimeout(5_000);
      try (Socket connection = frozenServer.accept()) {
        connection.setSoTimeout(5_000);
        InputStream inputStream = connection.getInputStream();
        while (inputStream.read() != -1) {
          // Skip the requests.
        }
      } catch (SocketTimeoutException e) {
        fail("The channel to a server that left a ping unanswered must be closed");
      }
    } finally {
      queryRouter.shutDown();
    }
  }

  /// Every channel queries use has to answer. A server whose OFFLINE channel answers while its REALTIME channel stays
  /// silent -- left half-open, say, when the server came back under a new address -- fails the ping, as REALTIME
  /// queries would time out. Only the silent channel is closed: the one that answered still carries queries.
  @Test
  public void testPingFailsWhenAnyChannelIsSilent()
      throws Exception {
    long requestId = 123;
    _queryServer = getQueryServer(0, dataTableBytesWithRequestId(requestId));
    initializeTestFixtures(startAndGetPort(_queryServer));
    QueryRouter queryRouter = newIsolatedQueryRouter();
    EmbeddedChannel silentChannel = new EmbeddedChannel();
    try {
      assertNotNull(queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, _routingTable, null, null, 5_000L)
          .getFinalResponses().get(_offlineServerRoutingInstance).getDataTable());
      ServerChannels serverChannels = queryRouter.getServerChannels();
      ServerChannels.ServerChannel realtimeChannel =
          serverChannels.getOrCreateServerChannel(_realtimeServerRoutingInstance);
      realtimeChannel._openedByQuery = true;
      realtimeChannel.setChannel(silentChannel);

      assertFalse(queryRouter.ping(_serverInstance, 1_000L));
      assertFalse(silentChannel.isOpen(), "The silent channel must be closed");
      // A close runs on the channel's event loop, so give a wrong one time to land.
      assertFalse(serverChannels.getOrCreateServerChannel(_offlineServerRoutingInstance)._channel.closeFuture()
          .await(500, TimeUnit.MILLISECONDS), "The channel that answered must stay open");
    } finally {
      silentChannel.finishAndReleaseAll();
      queryRouter.shutDown();
    }
  }

  /// Each table type has its own channel to a server. A server queried only for REALTIME tables -- as in the incident
  /// this guards -- has no OFFLINE channel, and must still be found and pinged over the channel queries use.
  @Test
  public void testRealtimeOnlyServerIsFoundAndPinged()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    _queryServer = getQueryServer(0, dataTable.toBytes());
    initializeTestFixtures(startAndGetPort(_queryServer));
    assertFalse(_queryRouter.hasChannel(_serverInstance));

    _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, _routingTable, 5_000L)
        .getFinalResponses();
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);

    assertTrue(_queryRouter.hasChannel(_serverInstance),
        "A REALTIME channel must count, or a REALTIME-only server is never checked");
    assertTrue(_queryRouter.ping(_serverInstance, 5_000L));
  }

  private static byte[] dataTableBytesWithRequestId(long requestId)
      throws IOException {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    return dataTable.toBytes();
  }

  private void waitForStatsUpdate(long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (_serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 5L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }
}
