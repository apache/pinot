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
package org.apache.pinot.controller.workload;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests retry behavior for requests whose service credential is resolved while the request is built.
public class WorkloadPropagationClientTest {
  @Test
  public void testServerRequestsResolveAdminAuthForEveryAttempt()
      throws Exception {
    List<String> receivedAuthHeaders = new CopyOnWriteArrayList<>();
    List<String> receivedMethods = new CopyOnWriteArrayList<>();
    AtomicInteger receivedRequests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/queryWorkloadConfigs", exchange -> {
      String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
      receivedAuthHeaders.add(authHeader != null ? authHeader : "<none>");
      receivedMethods.add(exchange.getRequestMethod());
      int status = receivedRequests.incrementAndGet() == 1 ? 500 : 200;
      exchange.sendResponseHeaders(status, -1);
      exchange.close();
    });
    server.start();

    int port = server.getAddress().getPort();
    String serverInstance = "Server_localhost_1234";
    String brokerInstance = "Broker_localhost_1234";
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllInstances()).thenReturn(List.of(serverInstance, brokerInstance));
    when(resourceManager.getHelixInstanceConfig(serverInstance)).thenReturn(instanceConfig(serverInstance, port));
    when(resourceManager.getHelixInstanceConfig(brokerInstance)).thenReturn(instanceConfig(brokerInstance, port));
    AtomicInteger authResolutions = new AtomicInteger();
    when(resourceManager.getServerAdminAuthProvider()).thenReturn(new AuthProvider() {
      @Override
      public Map<String, Object> getRequestHeaders() {
        return Map.of("Authorization", "token-" + authResolutions.incrementAndGet());
      }

      @Override
      public String getTaskToken() {
        return null;
      }
    });

    try (WorkloadPropagationClient client = new WorkloadPropagationClient(resourceManager, new ControllerConf(),
        mock(ControllerMetrics.class))) {
      client.sendQueryWorkloadMessage(
          Map.of(serverInstance, new QueryWorkloadRequest("refresh", new InstanceCost(1L, 2L))));
      client.sendQueryWorkloadMessage(
          Map.of(serverInstance, new QueryWorkloadRequest("delete", (InstanceCost) null)));
      client.sendQueryWorkloadMessage(
          Map.of(brokerInstance, new QueryWorkloadRequest("broker", new InstanceCost(3L, 4L))));

      Assert.assertEquals(receivedMethods, List.of("POST", "POST", "DELETE", "POST"));
      Assert.assertEquals(receivedAuthHeaders, List.of("token-1", "token-2", "token-3", "<none>"));
      Assert.assertEquals(authResolutions.get(), 3);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testRequestConstructionFailureIsRetried()
      throws Exception {
    AtomicInteger receivedRequests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/queryWorkloadConfigs", exchange -> {
      receivedRequests.incrementAndGet();
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.start();

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllInstances()).thenReturn(List.of());
    AtomicInteger requestConstructions = new AtomicInteger();
    Supplier<SimpleHttpRequest> requestSupplier = () -> {
      if (requestConstructions.incrementAndGet() == 1) {
        throw new IllegalStateException("credential refresh failed");
      }
      return SimpleRequestBuilder.get(
          "http://localhost:" + server.getAddress().getPort() + "/queryWorkloadConfigs").build();
    };

    try (WorkloadPropagationClient client = new WorkloadPropagationClient(resourceManager, new ControllerConf(),
        mock(ControllerMetrics.class))) {
      Assert.assertTrue(client.sendWorkloadRequestWithRetry(requestSupplier, "Server_localhost_1234")
          .get(10, TimeUnit.SECONDS));
      Assert.assertEquals(requestConstructions.get(), 2);
      Assert.assertEquals(receivedRequests.get(), 1);
    } finally {
      server.stop(0);
    }
  }

  private static InstanceConfig instanceConfig(String instanceName, int adminPort) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName("localhost");
    instanceConfig.getRecord().setIntField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, adminPort);
    return instanceConfig;
  }
}
