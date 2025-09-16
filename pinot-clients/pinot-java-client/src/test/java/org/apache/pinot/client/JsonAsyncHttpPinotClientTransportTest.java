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
package org.apache.pinot.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class JsonAsyncHttpPinotClientTransportTest implements HttpHandler {
  private static final String _VALID_RESPONSE_JSON = "{\"requestId\":\"4567\",\"traceInfo\":{},"
      + "\"numDocsScanned\":36542,"
      + "\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"36542\"}],\"timeUsedMs\":30,"
      + "\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":115545,\"numServersResponded\":99}";

  private static final String _CURSOR_RESPONSE_JSON = "{\"requestId\":\"cursor-123\",\"traceInfo\":{},"
      + "\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\",\"col2\"],"
      + "\"columnDataTypes\":[\"STRING\",\"INT\"]},"
      + "\"rows\":[[\"value1\",123],[\"value2\",456]],"
      + "\"metadata\":{\"currentPage\":0,\"pageSize\":100,\"totalRows\":1000,\"totalPages\":10,"
      + "\"hasNext\":true,\"hasPrevious\":false,\"expirationTimeMs\":" + (System.currentTimeMillis() + 300000) + "}},"
      + "\"timeUsedMs\":25,\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":1000,\"numServersResponded\":1}";

  private static final String _CURSOR_METADATA_JSON = "{\"requestId\":\"cursor-123\",\"traceInfo\":{},"
      + "\"resultTable\":{\"metadata\":{\"currentPage\":0,\"pageSize\":100,\"totalRows\":1000,\"totalPages\":10,"
      + "\"hasNext\":true,\"hasPrevious\":false,\"expirationTimeMs\":" + (System.currentTimeMillis() + 300000) + "}},"
      + "\"timeUsedMs\":5,\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":1000,\"numServersResponded\":1}";

  private HttpServer _dummyServer;
  private String _responseJson = _VALID_RESPONSE_JSON;
  private long _responseDelayMs = 0;
  private String _requestPath = "";
  private String _requestMethod = "";
  private String _requestBody = "";

  @BeforeClass
  public void setUp()
      throws Exception {
    _dummyServer = HttpServer.create();
    _dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    _dummyServer.start();
    _dummyServer.createContext("/", this);
  }

  @BeforeMethod
  public void setUpTestCase() {
    _responseJson = _VALID_RESPONSE_JSON;
    _responseDelayMs = 0L;
    _requestPath = "";
    _requestMethod = "";
    _requestBody = "";
  }

  @AfterClass
  public void tearDown() {
    if (_dummyServer != null) {
      _dummyServer.stop(0);
    }
  }

  @Test
  public void validJsonResponse() {
    _responseJson = _VALID_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    BrokerResponse response =
        transport.executeQuery("localhost:" + _dummyServer.getAddress().getPort(), "select * from planets");
    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "4567");
    ExecutionStats stats = response.getExecutionStats();
    assertEquals(stats.getTotalDocs(), 115545);
    assertEquals(stats.getNumServersResponded(), 99);
  }

  @Test
  public void invalidJsonResponseTriggersPinotClientException() {
    _responseJson = "{";
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    try {
      transport.executeQuery("localhost:" + _dummyServer.getAddress().getPort(), "select * from planets");
      fail("expected exception was not thrown");
    } catch (PinotClientException exception) {
      Throwable cause = ExceptionUtils.getRootCause(exception);
      assertEquals(cause.getClass().getName(), "com.fasterxml.jackson.core.io.JsonEOFException");
    }
  }

  @Test
  public void serverResponseExceedsBrokerReadTimeoutThreshold() {
    long brokerReadTimeoutMs = 100;
    _responseJson = _VALID_RESPONSE_JSON;
    _responseDelayMs = brokerReadTimeoutMs + 50;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    Properties connectionProps = new Properties();
    connectionProps.put("brokerReadTimeoutMs", String.valueOf(brokerReadTimeoutMs));
    factory.withConnectionProperties(connectionProps);
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    try {
      transport.executeQuery("localhost:" + _dummyServer.getAddress().getPort(), "select * from planets");
      fail("expected exception was not thrown");
    } catch (PinotClientException exception) {
      Throwable cause = ExceptionUtils.getRootCause(exception);
      assertEquals(cause.getClass().getName(), "java.util.concurrent.TimeoutException");
    }
  }

  // Cursor-related tests
  @Test
  public void testExecuteQueryWithCursor() {
    _responseJson = _CURSOR_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    BrokerResponse response = transport.executeQueryWithCursor("localhost:" + _dummyServer.getAddress().getPort(),
        "select * from planets", 100);

    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertNotNull(response.getResultTable());
    assertTrue(_requestPath.contains("getCursor=true"));
    assertTrue(_requestPath.contains("numRows=100"));
    assertTrue(_requestBody.contains("\"sql\":\"select * from planets\""));
  }

  @Test
  public void testFetchCursorResults()
      throws Exception {
    _responseJson = _CURSOR_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    CursorAwareBrokerResponse response = transport.fetchNextPage("localhost:"
        + _dummyServer.getAddress().getPort(), "cursor-123", 0, 10);
    assertNotNull(response);
    assertTrue(_requestPath.contains("/responseStore/cursor-123/results"));
    assertTrue(_requestPath.contains("offset=0"));
    assertTrue(_requestPath.contains("numRows=10"));
    assertEquals(_requestMethod, "GET");
  }

  @Test
  public void testExecuteQueryWithCursorAsync() throws Exception {
    _responseJson = _CURSOR_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    CompletableFuture<CursorAwareBrokerResponse> future = transport.executeQueryWithCursorAsync(
        "localhost:" + _dummyServer.getAddress().getPort(), "select * from planets", 50);

    CursorAwareBrokerResponse response = future.get();
    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertNotNull(response.getResultTable());
    assertTrue(_requestPath.contains("getCursor=true"));
    assertTrue(_requestPath.contains("numRows=50"));
    assertTrue(_requestBody.contains("\"sql\":\"select * from planets\""));
  }

  @Test
  public void testFetchNextPageWithOffsetAndNumRows() {
    _responseJson = _CURSOR_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();

    // Use the CursorCapable interface method instead of the removed legacy method
    CursorAwareBrokerResponse response = transport.fetchNextPage("localhost:" + _dummyServer.getAddress().getPort(),
        "cursor-123", 100, 50);

    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertTrue(_requestPath.contains("/responseStore/cursor-123/results"));
    assertTrue(_requestPath.contains("offset=100"));
    assertTrue(_requestPath.contains("numRows=50"));
    assertEquals(_requestMethod, "GET");
  }

  @Test
  public void testFetchCursorResultsAsync() throws Exception {
    _responseJson = _CURSOR_RESPONSE_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    CompletableFuture<CursorAwareBrokerResponse> future = transport.fetchNextPageAsync(
        "localhost:" + _dummyServer.getAddress().getPort(), "cursor-123", 0, 10);

    CursorAwareBrokerResponse response = future.get();
    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertTrue(_requestPath.contains("/responseStore/cursor-123/results"));
    assertEquals(_requestMethod, "GET");
  }

  @Test
  public void testGetCursorMetadata() {
    _responseJson = _CURSOR_METADATA_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    BrokerResponse response = transport.getCursorMetadata("localhost:" + _dummyServer.getAddress().getPort(),
        "cursor-123");

    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertTrue(_requestPath.contains("/responseStore/cursor-123/"));
    assertEquals(_requestMethod, "GET");
  }

  @Test
  public void testGetCursorMetadataAsync() throws Exception {
    _responseJson = _CURSOR_METADATA_JSON;
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    CompletableFuture<BrokerResponse> future = transport.getCursorMetadataAsync(
        "localhost:" + _dummyServer.getAddress().getPort(), "cursor-123");

    BrokerResponse response = future.get();
    assertFalse(response.hasExceptions());
    assertEquals(response.getRequestId(), "cursor-123");
    assertTrue(_requestPath.contains("/responseStore/cursor-123/"));
    assertEquals(_requestMethod, "GET");
  }

  @Test
  public void testDeleteCursor() {
    _responseJson = "{}"; // Empty response for delete
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();

    // Should not throw exception
    transport.deleteCursor("localhost:" + _dummyServer.getAddress().getPort(), "cursor-123");

    assertTrue(_requestPath.contains("/responseStore/cursor-123/"));
    assertEquals(_requestMethod, "DELETE");
  }

  @Test
  public void testDeleteCursorAsync() throws Exception {
    _responseJson = "{}"; // Empty response for delete
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    JsonAsyncHttpPinotClientTransport transport = (JsonAsyncHttpPinotClientTransport) factory.buildTransport();
    CompletableFuture<Void> future = transport.deleteCursorAsync(
        "localhost:" + _dummyServer.getAddress().getPort(), "cursor-123");

    future.get(); // Should complete without exception
    assertTrue(_requestPath.contains("/responseStore/cursor-123/"));
    assertEquals(_requestMethod, "DELETE");
  }

  @Override
  public void handle(HttpExchange exchange)
      throws IOException {
    // Capture request details for verification
    _requestPath = exchange.getRequestURI().toString();
    _requestMethod = exchange.getRequestMethod();

    // Capture request body for POST requests
    if ("POST".equals(_requestMethod)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
      StringBuilder requestBody = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        requestBody.append(line);
      }
      _requestBody = requestBody.toString();
      reader.close();
    } else {
      _requestBody = "";
    }

    if (_responseDelayMs > 0) {
      try {
        Thread.sleep(_responseDelayMs);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    exchange.sendResponseHeaders(200, 0);
    OutputStream out = exchange.getResponseBody();
    OutputStreamWriter writer = new OutputStreamWriter(out);
    writer.append(_responseJson);
    writer.flush();
    out.flush();
    out.close();
  }
}
