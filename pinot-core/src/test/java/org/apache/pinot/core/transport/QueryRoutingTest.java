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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableType;
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
      SERVER_INSTANCE.toServerRoutingInstance(TableType.OFFLINE);
  private static final ServerRoutingInstance REALTIME_SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.REALTIME);
  private static final BrokerRequest BROKER_REQUEST =
      new Pql2Compiler().compileToBrokerRequest("SELECT * FROM testTable");
  private static final Map<ServerInstance, List<String>> ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE, Collections.emptyList());

  private QueryRouter _queryRouter;

  @BeforeClass
  public void setUp() {
    _queryRouter = new QueryRouter("testBroker", mock(BrokerMetrics.class));
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes) {
    return new QueryServer(TEST_PORT, mockQueryScheduler(responseDelayMs, responseBytes), mock(ServerMetrics.class));
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
  public void testValidResponse() throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilder.getEmptyDataTable();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // REALTIME only
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // Hybrid
    asyncQueryResponse = _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, BROKER_REQUEST,
        ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testInvalidResponse() throws Exception {
    long requestId = 123;

    // Start the server
    QueryServer queryServer = getQueryServer(0, new byte[0]);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testNonMatchingRequestId() throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilder.getEmptyDataTable();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    QueryServer queryServer = getQueryServer(0, responseBytes);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);

    // Shut down the server
    queryServer.shutDown();
  }

  @Test
  public void testServerDown() throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilder.getEmptyDataTable();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    QueryServer queryServer = getQueryServer(500, responseBytes);
    queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);

    // Shut down the server before getting the response
    queryServer.shutDown();

    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    assertTrue(System.currentTimeMillis() - startTimeMs < 1000);

    // Submit query after server is down
    startTimeMs = System.currentTimeMillis();
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    response = asyncQueryResponse.getResponse();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getSubmitDelayMs(), -1);
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    assertTrue(System.currentTimeMillis() - startTimeMs < 1000);
  }

  @AfterClass
  public void tearDown() {
    _queryRouter.shutDown();
  }
}
