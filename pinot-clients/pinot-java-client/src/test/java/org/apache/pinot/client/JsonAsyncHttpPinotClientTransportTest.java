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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


public class JsonAsyncHttpPinotClientTransportTest implements HttpHandler {
  private static final String _VALID_RESPONSE_JSON = "{\"requestId\":\"4567\",\"traceInfo\":{},"
      + "\"numDocsScanned\":36542,"
      + "\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"36542\"}],\"timeUsedMs\":30,"
      + "\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":115545,\"numServersResponded\":99}";

  private HttpServer _dummyServer;
  private String _responseJson = _VALID_RESPONSE_JSON;
  private long _responseDelayMs = 0;

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

  @Override
  public void handle(HttpExchange exchange)
      throws IOException {
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
