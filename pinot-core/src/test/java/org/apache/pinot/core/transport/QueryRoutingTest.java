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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.BrokerRequestIdUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
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
  private static final ServerRoutingInstance SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(ServerInstance.RoutingType.NETTY);
  private static final String TABLE_NAME = "testTable";
  private static final BrokerRequest OFFLINE_BROKER_REQUEST = CalciteSqlCompiler.compileToBrokerRequest(
      "SELECT * FROM " + TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME));
  private static final BrokerRequest REALTIME_BROKER_REQUEST = CalciteSqlCompiler.compileToBrokerRequest(
      "SELECT * FROM " + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME));

  private static final ServerQueryRoutingContext OFFLINE_BROKER_REQ_CONTEXT =
      new ServerQueryRoutingContext(OFFLINE_BROKER_REQUEST, Pair.of(Collections.emptyList(), Collections.emptyList()),
          SERVER_ROUTING_INSTANCE);
  private static final ServerQueryRoutingContext REALTIME_BROKER_REQ_CONTEXT =
      new ServerQueryRoutingContext(REALTIME_BROKER_REQUEST, Pair.of(Collections.emptyList(), Collections.emptyList()),
          SERVER_ROUTING_INSTANCE);
  private static final Map<ServerInstance, List<ServerQueryRoutingContext>> OFFLINE_ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE, List.of(OFFLINE_BROKER_REQ_CONTEXT));
  private static final Map<ServerInstance, List<ServerQueryRoutingContext>> REALTIME_ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE, List.of(REALTIME_BROKER_REQ_CONTEXT));
  private static final Map<ServerInstance, List<ServerQueryRoutingContext>> HYBRID_ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE, List.of(OFFLINE_BROKER_REQ_CONTEXT, REALTIME_BROKER_REQ_CONTEXT));

  private QueryRouter _queryRouter;
  private ServerRoutingStatsManager _serverRoutingStatsManager;
  int _requestCount;

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(properties);
    _serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, mock(BrokerMetrics.class));
    _serverRoutingStatsManager.init();
    _queryRouter = new QueryRouter("testBroker", mock(BrokerMetrics.class), _serverRoutingStatsManager);
    _requestCount = 0;
  }

  @AfterMethod
  void deregisterServerMetrics() {
    ServerMetrics.deregister();
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes) {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    InstanceRequestHandler handler = new InstanceRequestHandler("server01", new PinotConfiguration(),
        mockQueryScheduler(responseDelayMs, responseBytes), serverMetrics, mock(AccessControl.class));
    ServerMetrics.register(serverMetrics);
    return new QueryServer(TEST_PORT, null, handler);
  }

  private QueryServer getQueryServer(int responseDelayMs, DataTable offlineDataTable, DataTable realtimeDataTable,
      int port) {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    InstanceRequestHandler handler = new InstanceRequestHandler("server01", new PinotConfiguration(),
        mockQueryScheduler(responseDelayMs, offlineDataTable, realtimeDataTable), serverMetrics,
        mock(AccessControl.class));
    ServerMetrics.register(serverMetrics);
    return new QueryServer(port, null, handler);
  }

  private QueryServer getQueryServer(int responseDelayMs, DataTable offlineDataTable, DataTable realtimeDataTable) {
    return getQueryServer(responseDelayMs, offlineDataTable, realtimeDataTable, TEST_PORT);
  }

  private QueryScheduler mockQueryScheduler(int responseDelayMs, byte[] responseBytes) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(invocation -> {
      Thread.sleep(responseDelayMs);
      return Futures.immediateFuture(responseBytes);
    });
    return queryScheduler;
  }

  private QueryScheduler mockQueryScheduler(int responseDelayMs, DataTable offlineDataTable,
      DataTable realtimeDataTable) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(invocation -> {
      Thread.sleep(responseDelayMs);
      // TODO(egalpin): verify if this arg is tablename
      String tableName = String.valueOf(((ServerQueryRequest) invocation.getArguments()[0]).getTableName());
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableName.equals(realtimeDataTable.getMetadata().get(MetadataKey.TABLE.getName())) || tableType.equals(
          DataTableUtils.inferTableType(realtimeDataTable))) {
        return Futures.immediateFuture(realtimeDataTable.toBytes());
      } else if (tableName.equals(offlineDataTable.getMetadata().get(MetadataKey.TABLE.getName())) || tableType.equals(
          DataTableUtils.inferTableType(offlineDataTable))) {
        return Futures.immediateFuture(offlineDataTable.toBytes());
      }
      return Futures.immediateFuture(new byte[0]);
    });
    return queryScheduler;
  }

  @Test
  public void testValidResponse()
      throws Exception {
    long requestId = 1230;
    DataTable offlineDataTable = DataTableBuilderFactory.getEmptyDataTable();
    offlineDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(),
        Long.toString(BrokerRequestIdUtils.getOfflineRequestId(requestId)));
    offlineDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), OFFLINE_BROKER_REQUEST.getQuerySource().getTableName());
    byte[] offlineResponseBytes = offlineDataTable.toBytes();

    DataTable realtimeDataTable = DataTableBuilderFactory.getEmptyDataTable();
    realtimeDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(),
        Long.toString(BrokerRequestIdUtils.getRealtimeRequestId(requestId)));
    realtimeDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), REALTIME_BROKER_REQUEST.getQuerySource().getTableName());
    byte[] realtimeResponseBytes = realtimeDataTable.toBytes();

    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(0, offlineDataTable, realtimeDataTable);
    queryServer.start();

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 600_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), offlineResponseBytes.length);
    // 2 requests - query submit and query response.
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // REALTIME only
    asyncQueryResponse = _queryRouter.submitQuery(requestId, "testTable", REALTIME_ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(TableType.REALTIME, DataTableUtils.inferTableType(serverResponse.getDataTable()));
    assertEquals(serverResponse.getResponseSize(), realtimeResponseBytes.length);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Hybrid
    asyncQueryResponse = _queryRouter.submitQuery(requestId, "testTable", HYBRID_ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 2);

    int accountedFor = 0;
    for (ServerResponse serverResponse1 : response.get(SERVER_ROUTING_INSTANCE)) {
      assertNotNull(serverResponse1.getDataTable());
      if (serverResponse1.getDataTable().getMetadata().get(MetadataKey.TABLE.getName())
          .equals(offlineDataTable.getMetadata().get(MetadataKey.TABLE.getName()))) {
        assertEquals(serverResponse1.getResponseSize(), offlineResponseBytes.length);
        accountedFor++;
      } else if (serverResponse1.getDataTable().getMetadata().get(MetadataKey.TABLE.getName())
          .equals(realtimeDataTable.getMetadata().get(MetadataKey.TABLE.getName()))) {
        assertEquals(serverResponse1.getResponseSize(), realtimeResponseBytes.length);
        accountedFor++;
      }
    }
    assertEquals(accountedFor, 2, "Hybrid should have created 1 realtime and 1 offline request/response");
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
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
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
  public void testLatencyForQueryServerException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.SERVER_TABLE_MISSING_ERROR, exception);
    dataTable.addException(processingException);
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // Send a query with ServerSide exception and check if the latency is set to timeout value.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));

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

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testLatencyForClientException()
      throws Exception {
    long requestId = 1230;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_CANCELLATION_ERROR, exception);
    dataTable.addException(processingException);
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // Send a query with client side errors.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);

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

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testLatencyForMultipleExceptions()
      throws Exception {
    long requestId = 1230;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_CANCELLATION_ERROR, exception);
    ProcessingException processingServerException =
        QueryException.getException(QueryException.SERVER_TABLE_MISSING_ERROR, exception);
    dataTable.addException(processingServerException);
    dataTable.addException(processingException);
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // Send a query with multiple exceptions. Make sure that the latency is set to timeout value even if a single
    //server-side exception is seen.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));

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

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testLatencyForNoException()
      throws Exception {
    long requestId = 1230;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // Send a valid query and get latency
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);

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

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testNonMatchingRequestId()
      throws Exception {
    long requestId = 1230;
    DataTable offlineDataTable = DataTableBuilderFactory.getEmptyDataTable();
    offlineDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    offlineDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), OFFLINE_BROKER_REQUEST.getQuerySource().getTableName());

    DataTable realtimeDataTable = DataTableBuilderFactory.getEmptyDataTable();
    realtimeDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    realtimeDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), REALTIME_BROKER_REQUEST.getQuerySource().getTableName());
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(0, offlineDataTable, realtimeDataTable);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", OFFLINE_ROUTING_TABLE, 1_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
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
    long requestId = 1230;
    // To avoid flakyness, set timeoutMs to 2000 msec. For some test runs, it can take up to
    // 1400 msec to mark request as failed.
    long timeoutMs = 2000L;
    DataTable offlineDataTable = DataTableBuilderFactory.getEmptyDataTable();
    offlineDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    offlineDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), OFFLINE_BROKER_REQUEST.getQuerySource().getTableName());

    DataTable realtimeDataTable = DataTableBuilderFactory.getEmptyDataTable();
    realtimeDataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    realtimeDataTable.getMetadata()
        .put(MetadataKey.TABLE.getName(), REALTIME_BROKER_REQUEST.getQuerySource().getTableName());

    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    QueryServer queryServer = getQueryServer(500, offlineDataTable, realtimeDataTable);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", OFFLINE_ROUTING_TABLE, timeoutMs);

    // Shut down the server before getting the response
    queryServer.shutDown();

    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    ServerResponse serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
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
    asyncQueryResponse = _queryRouter.submitQuery(requestId + 1, "testTable", OFFLINE_ROUTING_TABLE, timeoutMs);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(SERVER_ROUTING_INSTANCE));
    assertEquals(response.get(SERVER_ROUTING_INSTANCE).size(), 1);
    serverResponse = response.get(SERVER_ROUTING_INSTANCE).get(0);
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

  @Test
  public void testSkipUnavailableServer()
      throws IOException, InterruptedException {
    // Using a different port is a hack to avoid resource conflict with other tests, ideally queryServer.shutdown()
    // should ensure there is no possibility of resource conflict.
    int port = 12346;
    ServerInstance serverInstance1 = new ServerInstance("localhost", port);
    ServerInstance serverInstance2 = new ServerInstance("localhost", port + 1);
    ServerRoutingInstance serverRoutingInstance1 =
        serverInstance1.toServerRoutingInstance(ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance serverRoutingInstance2 =
        serverInstance2.toServerRoutingInstance(ServerInstance.RoutingType.NETTY);
//    Map<ServerInstance, Pair<List<String>, List<String>>> routingTable =
//        Map.of(serverInstance1, Pair.of(Collections.emptyList(), Collections.emptyList()), serverInstance2,
//            Pair.of(Collections.emptyList(), Collections.emptyList()));

    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable_OFFLINE");

    ServerQueryRoutingContext offlineBrokerRequestContext1 =
        new ServerQueryRoutingContext(brokerRequest, Pair.of(Collections.emptyList(), Collections.emptyList()),
            serverRoutingInstance1);
    ServerQueryRoutingContext offlineBrokerRequestContext2 =
        new ServerQueryRoutingContext(brokerRequest, Pair.of(Collections.emptyList(), Collections.emptyList()),
            serverRoutingInstance2);
    Map<ServerInstance, List<ServerQueryRoutingContext>> routingTable =
        Map.of(serverInstance1, List.of(offlineBrokerRequestContext1), serverInstance2,
            List.of(offlineBrokerRequestContext2));

    long requestId = 1230;
    DataSchema dataSchema =
        new DataSchema(new String[]{"column1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    builder.startRow();
    builder.setColumn(0, "value1");
    builder.finishRow();
    DataTable dataTableSuccess = builder.build();
    Map<String, String> dataTableMetadata = dataTableSuccess.getMetadata();
    dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTableMetadata.put(MetadataKey.TABLE.getName(), brokerRequest.getQuerySource().getTableName());

    // Only start a single QueryServer, on port from serverInstance1
    QueryServer queryServer = getQueryServer(500, dataTableSuccess, dataTableSuccess, port);
    queryServer.start();

    // Submit the query with skipUnavailableServers=true, the single started server should return a valid response
    long startTime = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse = _queryRouter.submitQuery(requestId, "testTable", routingTable, 10_000L);
    Map<ServerRoutingInstance, List<ServerResponse>> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    ServerResponse serverResponse1 = response.get(serverRoutingInstance1).get(0);
    ServerResponse serverResponse2 = response.get(serverRoutingInstance2).get(0);
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

    queryServer.shutDown();

    // Submit the same query without skipUnavailableServers, the servers should not return any response
    brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable_OFFLINE");
    dataTableMetadata.put(MetadataKey.TABLE.getName(), brokerRequest.getQuerySource().getTableName());

    offlineBrokerRequestContext1 =
        new ServerQueryRoutingContext(brokerRequest, Pair.of(Collections.emptyList(), Collections.emptyList()),
            serverRoutingInstance1);
    offlineBrokerRequestContext2 =
        new ServerQueryRoutingContext(brokerRequest, Pair.of(Collections.emptyList(), Collections.emptyList()),
            serverRoutingInstance2);
    routingTable = Map.of(serverInstance1, List.of(offlineBrokerRequestContext1), serverInstance2,
        List.of(offlineBrokerRequestContext2));

    // Start a new query server with updated data table
    queryServer = getQueryServer(500, dataTableSuccess, dataTableSuccess, port);
    queryServer.start();
    startTime = System.currentTimeMillis();

    asyncQueryResponse = _queryRouter.submitQuery(requestId, "testTable", routingTable, 10_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    serverResponse1 = response.get(serverRoutingInstance1).get(0);
    serverResponse2 = response.get(serverRoutingInstance2).get(0);
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
    queryServer.shutDown();
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
}
