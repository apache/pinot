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
package org.apache.pinot.controller.cursors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ResponseStoreCleanerTest {
  private static final String RESPONSE_STORE_PATH = "/responseStore";
  private static final long CURRENT_TIME_MS = 1000000L;

  private final Executor _executor = Executors.newFixedThreadPool(4);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  private PinotHelixResourceManager _helixResourceManager;
  private LeadControllerManager _leadControllerManager;
  private ControllerConf _controllerConf;

  private final List<FakeBrokerServer> _brokerServers = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws IOException {
    _helixResourceManager = mock(PinotHelixResourceManager.class);
    _leadControllerManager = mock(LeadControllerManager.class);
    _controllerConf = new ControllerConf();

    when(_leadControllerManager.isLeaderForTable(org.mockito.ArgumentMatchers.anyString())).thenReturn(true);
  }

  @AfterClass
  public void tearDown() {
    for (FakeBrokerServer server : _brokerServers) {
      server.stop();
    }
    _brokerServers.clear();
  }

  private FakeBrokerServer createBrokerServer(List<CursorResponseNative> responses, int deleteStatusCode)
      throws IOException {
    FakeBrokerServer server = new FakeBrokerServer(responses, deleteStatusCode);
    server.start();
    _brokerServers.add(server);
    return server;
  }

  private void setupBrokerInstances(List<FakeBrokerServer> servers) {
    List<InstanceConfig> brokerConfigs = new ArrayList<>();
    for (FakeBrokerServer server : servers) {
      ZNRecord record = new ZNRecord("Broker_" + server.getHost() + "_" + server.getPort());
      record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_HOST.name(), server.getHost());
      record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.name(),
          String.valueOf(server.getPort()));
      record.setSimpleField("GRPC_PORT", "8090");
      InstanceConfig instanceConfig = new InstanceConfig(record);
      brokerConfigs.add(instanceConfig);
    }
    when(_helixResourceManager.getAllBrokerInstanceConfigs()).thenReturn(brokerConfigs);
  }

  private CursorResponseNative createCursorResponse(String requestId, long expirationTimeMs) {
    CursorResponseNative response = new CursorResponseNative();
    response.setRequestId(requestId);
    response.setExpirationTimeMs(expirationTimeMs);
    response.setBrokerHost("localhost");
    response.setBrokerPort(8099);
    return response;
  }

  @Test
  public void testCleanupExpiredResponses()
      throws Exception {
    // Create broker with 2 expired and 1 non-expired response
    List<CursorResponseNative> responses = new ArrayList<>();
    responses.add(createCursorResponse("expired-1", CURRENT_TIME_MS - 1000)); // expired
    responses.add(createCursorResponse("expired-2", CURRENT_TIME_MS - 500));  // expired
    responses.add(createCursorResponse("not-expired", CURRENT_TIME_MS + 1000)); // not expired

    FakeBrokerServer broker = createBrokerServer(responses, 200);
    setupBrokerInstances(Collections.singletonList(broker));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    cleaner.doClean(CURRENT_TIME_MS);

    // Verify only expired responses were deleted
    assertEquals(broker.getDeleteCount(), 2);
    assertTrue(broker.getDeletedRequestIds().containsKey("expired-1"));
    assertTrue(broker.getDeletedRequestIds().containsKey("expired-2"));
    assertEquals(broker.getDeletedRequestIds().containsKey("not-expired"), false);
  }

  @Test
  public void testCleanupTreats404AsSuccess()
      throws Exception {
    // Create broker that returns 404 for deletes (simulating already-deleted responses)
    List<CursorResponseNative> responses = new ArrayList<>();
    responses.add(createCursorResponse("already-deleted", CURRENT_TIME_MS - 1000));

    FakeBrokerServer broker = createBrokerServer(responses, 404);
    setupBrokerInstances(Collections.singletonList(broker));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    // This should not throw an exception even though broker returns 404
    cleaner.doClean(CURRENT_TIME_MS);

    // Verify delete was attempted
    assertEquals(broker.getDeleteCount(), 1);
  }

  @Test
  public void testCleanupWithMultipleBrokers()
      throws Exception {
    // Create two brokers with different expired responses
    List<CursorResponseNative> broker1Responses = new ArrayList<>();
    broker1Responses.add(createCursorResponse("broker1-expired-1", CURRENT_TIME_MS - 1000));
    broker1Responses.add(createCursorResponse("broker1-expired-2", CURRENT_TIME_MS - 500));

    List<CursorResponseNative> broker2Responses = new ArrayList<>();
    broker2Responses.add(createCursorResponse("broker2-expired-1", CURRENT_TIME_MS - 2000));

    FakeBrokerServer broker1 = createBrokerServer(broker1Responses, 200);
    FakeBrokerServer broker2 = createBrokerServer(broker2Responses, 200);
    setupBrokerInstances(List.of(broker1, broker2));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    cleaner.doClean(CURRENT_TIME_MS);

    // Verify broker1 only received its own delete requests
    assertEquals(broker1.getDeleteCount(), 2);
    assertTrue(broker1.getDeletedRequestIds().containsKey("broker1-expired-1"));
    assertTrue(broker1.getDeletedRequestIds().containsKey("broker1-expired-2"));
    assertEquals(broker1.getDeletedRequestIds().containsKey("broker2-expired-1"), false);

    // Verify broker2 only received its own delete requests
    assertEquals(broker2.getDeleteCount(), 1);
    assertTrue(broker2.getDeletedRequestIds().containsKey("broker2-expired-1"));
    assertEquals(broker2.getDeletedRequestIds().containsKey("broker1-expired-1"), false);
  }

  @Test
  public void testPartialBrokerFailureDoesNotBlockOthers()
      throws Exception {
    // Create one broker that fails (returns 500) and one that succeeds
    List<CursorResponseNative> broker1Responses = new ArrayList<>();
    broker1Responses.add(createCursorResponse("broker1-expired", CURRENT_TIME_MS - 1000));

    List<CursorResponseNative> broker2Responses = new ArrayList<>();
    broker2Responses.add(createCursorResponse("broker2-expired", CURRENT_TIME_MS - 1000));

    FakeBrokerServer broker1 = createBrokerServer(broker1Responses, 500); // Will fail
    FakeBrokerServer broker2 = createBrokerServer(broker2Responses, 200); // Will succeed
    setupBrokerInstances(List.of(broker1, broker2));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    // Should not throw, even though broker1 fails
    cleaner.doClean(CURRENT_TIME_MS);

    // Both brokers should have received delete attempts
    assertEquals(broker1.getDeleteCount(), 1);
    assertEquals(broker2.getDeleteCount(), 1);
  }

  @Test
  public void testNoExpiredResponses()
      throws Exception {
    // Create broker with only non-expired responses
    List<CursorResponseNative> responses = new ArrayList<>();
    responses.add(createCursorResponse("not-expired-1", CURRENT_TIME_MS + 1000));
    responses.add(createCursorResponse("not-expired-2", CURRENT_TIME_MS + 2000));

    FakeBrokerServer broker = createBrokerServer(responses, 200);
    setupBrokerInstances(Collections.singletonList(broker));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    cleaner.doClean(CURRENT_TIME_MS);

    // No deletes should have been attempted
    assertEquals(broker.getDeleteCount(), 0);
  }

  @Test
  public void testEmptyResponseStore()
      throws Exception {
    // Create broker with no responses
    FakeBrokerServer broker = createBrokerServer(Collections.emptyList(), 200);
    setupBrokerInstances(Collections.singletonList(broker));

    ResponseStoreCleaner cleaner = new ResponseStoreCleaner(
        _controllerConf, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executor, _connectionManager);

    cleaner.doClean(CURRENT_TIME_MS);

    // No deletes should have been attempted
    assertEquals(broker.getDeleteCount(), 0);
  }

  /**
   * Fake broker server that simulates the ResponseStoreResource endpoints.
   */
  private static class FakeBrokerServer extends FakeHttpServer {
    private final List<CursorResponseNative> _responses;
    private final int _deleteStatusCode;
    private final AtomicInteger _deleteCount = new AtomicInteger(0);
    private final Map<String, Boolean> _deletedRequestIds = new ConcurrentHashMap<>();

    FakeBrokerServer(List<CursorResponseNative> responses, int deleteStatusCode) {
      _responses = responses;
      _deleteStatusCode = deleteStatusCode;
    }

    void start()
        throws IOException {
      HttpHandler handler = new HttpHandler() {
        @Override
        public void handle(HttpExchange exchange)
            throws IOException {
          String method = exchange.getRequestMethod();
          URI uri = exchange.getRequestURI();
          String path = uri.getPath();

          String responseBody;
          int statusCode;

          if ("GET".equals(method) && RESPONSE_STORE_PATH.equals(path)) {
            // GET /responseStore - return list of responses
            responseBody = JsonUtils.objectToString(_responses);
            statusCode = 200;
          } else if ("DELETE".equals(method) && path.startsWith(RESPONSE_STORE_PATH + "/")) {
            // DELETE /responseStore/{requestId}
            String requestId = path.substring(RESPONSE_STORE_PATH.length() + 1);
            _deleteCount.incrementAndGet();
            _deletedRequestIds.put(requestId, true);
            responseBody = "Deleted " + requestId;
            statusCode = _deleteStatusCode;
          } else {
            responseBody = "Not found";
            statusCode = 404;
          }

          byte[] responseBytes = responseBody.getBytes();
          exchange.sendResponseHeaders(statusCode, responseBytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
          }
        }
      };

      super.start(RESPONSE_STORE_PATH, handler);
    }

    String getHost() {
      return "localhost";
    }

    int getPort() {
      return _httpServer.getAddress().getPort();
    }

    int getDeleteCount() {
      return _deleteCount.get();
    }

    Map<String, Boolean> getDeletedRequestIds() {
      return _deletedRequestIds;
    }
  }
}
