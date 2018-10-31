/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.transport;

import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryRouterTest {
  private static final int TEST_PORT = 12345;
  private static final String SERVER_INSTANCE_NAME = "Server_localhost_" + TEST_PORT;
  private static final Server OFFLINE_SERVER = new Server(SERVER_INSTANCE_NAME, TableType.OFFLINE);
  private static final Server REALTIME_SERVER = new Server(SERVER_INSTANCE_NAME, TableType.REALTIME);
  private static final BrokerRequest BROKER_REQUEST = new BrokerRequest();
  private static final Map<String, List<String>> ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE_NAME, Collections.emptyList());

  private QueryRouter _queryRouter;

  @BeforeClass
  public void setUp() {
    _queryRouter = new QueryRouter("testBroker", Mockito.mock(BrokerMetrics.class));
  }

  @Test
  public void testValidResponse() throws Exception {
    long requestId = 123;
    DataTable dataTable = new DataTableImplV2();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    DummyServer dummyServer = new DummyServer(TEST_PORT, 0L, responseBytes);
    Thread thread = new Thread(dummyServer);
    thread.start();
    while (!dummyServer.isReady()) {
      Thread.sleep(100L);
    }

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<Server, ServerResponse> response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNotNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // REALTIME only
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(REALTIME_SERVER));
    serverResponse = response.get(REALTIME_SERVER);
    Assert.assertNotNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // Hybrid
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, BROKER_REQUEST, ROUTING_TABLE,
            1_000L);
    response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 2);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNotNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    Assert.assertTrue(response.containsKey(REALTIME_SERVER));
    serverResponse = response.get(REALTIME_SERVER);
    Assert.assertNotNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseSize(), responseBytes.length);

    // Shut down the server
    dummyServer.shutDown();
    thread.join();
  }

  @Test
  public void testInvalidResponse() throws Exception {
    long requestId = 123;

    // Start the server
    DummyServer dummyServer = new DummyServer(TEST_PORT, 0L, new byte[0]);
    Thread thread = new Thread(dummyServer);
    thread.start();
    while (!dummyServer.isReady()) {
      Thread.sleep(100L);
    }

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<Server, ServerResponse> response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseDelayMs(), -1);
    Assert.assertEquals(serverResponse.getResponseSize(), 0);
    Assert.assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    Assert.assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);

    // Shut down the server
    dummyServer.shutDown();
    thread.join();
  }

  @Test
  public void testNonMatchingRequestId() throws Exception {
    long requestId = 123;
    DataTable dataTable = new DataTableImplV2();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    DummyServer dummyServer = new DummyServer(TEST_PORT, 0L, responseBytes);
    Thread thread = new Thread(dummyServer);
    thread.start();
    while (!dummyServer.isReady()) {
      Thread.sleep(100L);
    }

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<Server, ServerResponse> response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseDelayMs(), -1);
    Assert.assertEquals(serverResponse.getResponseSize(), 0);
    Assert.assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    Assert.assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);

    // Shut down the server
    dummyServer.shutDown();
    thread.join();
  }

  @Test
  public void testServerDown() throws Exception {
    long requestId = 123;
    DataTable dataTable = new DataTableImplV2();
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();

    // Start the server
    DummyServer dummyServer = new DummyServer(TEST_PORT, 500L, responseBytes);
    Thread thread = new Thread(dummyServer);
    thread.start();
    while (!dummyServer.isReady()) {
      Thread.sleep(100L);
    }

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);

    // Shut down the server before getting the response
    dummyServer.shutDown();
    thread.join();

    Map<Server, ServerResponse> response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getResponseDelayMs(), -1);
    Assert.assertEquals(serverResponse.getResponseSize(), 0);
    Assert.assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    Assert.assertTrue(System.currentTimeMillis() - startTimeMs < 1000);

    // Submit query after server is down
    startTimeMs = System.currentTimeMillis();
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    response = asyncQueryResponse.getResponse();
    Assert.assertEquals(response.size(), 1);
    Assert.assertTrue(response.containsKey(OFFLINE_SERVER));
    serverResponse = response.get(OFFLINE_SERVER);
    Assert.assertNull(serverResponse.getDataTable());
    Assert.assertEquals(serverResponse.getSubmitDelayMs(), -1);
    Assert.assertEquals(serverResponse.getResponseDelayMs(), -1);
    Assert.assertEquals(serverResponse.getResponseSize(), 0);
    Assert.assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should early terminate
    Assert.assertTrue(System.currentTimeMillis() - startTimeMs < 1000);
  }

  @AfterClass
  public void tearDown() {
    _queryRouter.shutDown();
  }
}