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
package org.apache.pinot.server.starter.helix;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.Header;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingReadyCheckerTest {
  private static final String SERVER_INSTANCE = "Server_localhost_8098";
  private static final String BROKER_INSTANCE = "Broker_localhost_8099";
  private static final String SECOND_BROKER_INSTANCE = "Broker_localhost_8100";

  @Test
  public void testBrokerRequestUsesAuthProvider() throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getInstanceName()).thenReturn(SERVER_INSTANCE);

    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setStateMap("0", Map.of(BROKER_INSTANCE, "ONLINE"));
    when(helixAdmin.getResourceExternalView("testCluster", CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerResource);
    InstanceConfig instanceConfig = new InstanceConfig(BROKER_INSTANCE);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8099");
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.ADMIN_HTTPS_PORT_KEY, "8099");
    when(helixAdmin.getInstanceConfig("testCluster", BROKER_INSTANCE)).thenReturn(instanceConfig);

    URI expectedUri = URI.create("https://localhost:8099/routing/server/" + SERVER_INSTANCE);
    AuthProvider authProvider = mock(AuthProvider.class);
    when(authProvider.getRequestHeaders()).thenReturn(Map.of("Authorization", "Bearer test-token"));
    AtomicReference<URI> requestedUri = new AtomicReference<>();
    AtomicReference<List<Header>> requestedAuthHeaders = new AtomicReference<>();
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient = (uri, authHeaders) -> {
      requestedUri.set(uri);
      requestedAuthHeaders.set(authHeaders);
      return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
    };

    try (BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(helixManager, 5_000L, false, authProvider, routingStatusClient)) {
      checker.check();
      assertTrue(checker.isReady());
    }

    assertEquals(requestedUri.get(), expectedUri);
    assertEquals(requestedAuthHeaders.get().size(), 1);
    assertEquals(requestedAuthHeaders.get().get(0).getName(), "Authorization");
    assertEquals(requestedAuthHeaders.get().get(0).getValue(), "Bearer test-token");
  }

  @Test
  public void testWaitsForOnlineBrokers() {
    AtomicReference<Set<String>> onlineBrokers = new AtomicReference<>(Set.of());
    BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(SERVER_INSTANCE, onlineBrokers::get, brokers -> true);

    assertFalse(checker.isReady());

    onlineBrokers.set(Set.of("Broker_localhost_8099"));
    checker.check();
    assertTrue(checker.isReady());
  }

  @Test
  public void testRetriesUntilAllBrokersConfirm() {
    AtomicInteger attempts = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099", "Broker_localhost_8100"),
        brokers -> attempts.incrementAndGet() > 1);

    assertEquals(attempts.get(), 0);
    checker.check();
    assertFalse(checker.isReady());

    checker.check();
    assertTrue(checker.isReady());
    assertTrue(checker.isReady());
    assertEquals(attempts.get(), 2);
  }

  @Test
  public void testBrokerMembershipMustRemainStable() {
    AtomicInteger reads = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> reads.incrementAndGet() == 1 ? Set.of("Broker_localhost_8099")
            : Set.of("Broker_localhost_8099", "Broker_localhost_8100"),
        brokers -> true);

    checker.check();
    assertFalse(checker.isReady());
  }

  @Test
  public void testTimeoutFailsOpen() {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099"), brokers -> false, 5_000L, true, currentTimeMs::get);

    checker.check();
    assertFalse(checker.isReady());

    currentTimeMs.set(6_000L);
    assertTrue(checker.isReady());
  }

  @Test
  public void testFailOpenDeadlineStopsSequentialBrokerSweep() {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getInstanceName()).thenReturn(SERVER_INSTANCE);
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setStateMap("0", Map.of(BROKER_INSTANCE, "ONLINE", SECOND_BROKER_INSTANCE, "ONLINE"));
    when(helixAdmin.getResourceExternalView("testCluster", CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerResource);
    when(helixAdmin.getInstanceConfig("testCluster", BROKER_INSTANCE))
        .thenReturn(createBrokerInstanceConfig(BROKER_INSTANCE, "8099"));
    when(helixAdmin.getInstanceConfig("testCluster", SECOND_BROKER_INSTANCE))
        .thenReturn(createBrokerInstanceConfig(SECOND_BROKER_INSTANCE, "8100"));
    AtomicInteger requests = new AtomicInteger();
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient = (uri, authHeaders) -> {
      requests.incrementAndGet();
      currentTimeMs.set(6_000L);
      return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
    };

    try (BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(helixManager, 5_000L, true,
        new NullAuthProvider(), routingStatusClient, currentTimeMs::get)) {
      checker.check();
      assertTrue(checker.isReady());
      assertEquals(requests.get(), 1);
    }
  }

  @Test
  public void testReadinessHonorsFailOpenDeadlineWhileCheckIsInFlight()
      throws Exception {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    CountDownLatch checkStarted = new CountDownLatch(1);
    CountDownLatch releaseCheck = new CountDownLatch(1);
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of(BROKER_INSTANCE), brokers -> {
          checkStarted.countDown();
          try {
            assertTrue(releaseCheck.await(5, TimeUnit.SECONDS));
            return false;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }, 5_000L, true, currentTimeMs::get);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> check = executor.submit(checker::check);
      assertTrue(checkStarted.await(5, TimeUnit.SECONDS));
      currentTimeMs.set(6_000L);
      assertTrue(checker.isReady());
      releaseCheck.countDown();
      check.get(5, TimeUnit.SECONDS);
    } finally {
      releaseCheck.countDown();
      checker.close();
      executor.shutdownNow();
    }
  }

  @Test
  public void testTimeoutFailsClosedButRecovers() {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    AtomicReference<Boolean> brokerReady = new AtomicReference<>(false);
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099"), brokers -> brokerReady.get(), 5_000L, false, currentTimeMs::get);

    currentTimeMs.set(6_000L);
    checker.check();
    assertFalse(checker.isReady());

    brokerReady.set(true);
    checker.check();
    assertTrue(checker.isReady());
  }

  @Test
  public void testConcurrentBackgroundChecksAreSerializedAndReadinessDoesNotBlock() throws Exception {
    CountDownLatch firstCheckStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstCheck = new CountDownLatch(1);
    AtomicInteger activeChecks = new AtomicInteger();
    AtomicInteger maxActiveChecks = new AtomicInteger();
    AtomicInteger attempts = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of(BROKER_INSTANCE), brokers -> {
          int active = activeChecks.incrementAndGet();
          maxActiveChecks.accumulateAndGet(active, Math::max);
          attempts.incrementAndGet();
          firstCheckStarted.countDown();
          try {
            assertTrue(releaseFirstCheck.await(5, TimeUnit.SECONDS));
            return false;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } finally {
            activeChecks.decrementAndGet();
          }
        });

    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<?> firstResult = callers.submit(checker::check);
      assertTrue(firstCheckStarted.await(5, TimeUnit.SECONDS));
      assertFalse(checker.isReady());
      Future<?> secondResult = callers.submit(checker::check);
      assertFalse(secondResult.isDone());

      releaseFirstCheck.countDown();
      firstResult.get(5, TimeUnit.SECONDS);
      secondResult.get(5, TimeUnit.SECONDS);
      assertEquals(attempts.get(), 2);
      assertEquals(maxActiveChecks.get(), 1);
    } finally {
      releaseFirstCheck.countDown();
      callers.shutdownNow();
    }
  }

  @Test
  public void testCredentialsAreNotSentOverHttp() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getInstanceName()).thenReturn(SERVER_INSTANCE);
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setStateMap("0", Map.of(BROKER_INSTANCE, "ONLINE"));
    when(helixAdmin.getResourceExternalView("testCluster", CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerResource);
    InstanceConfig instanceConfig = new InstanceConfig(BROKER_INSTANCE);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8099");
    when(helixAdmin.getInstanceConfig("testCluster", BROKER_INSTANCE)).thenReturn(instanceConfig);
    AuthProvider authProvider = mock(AuthProvider.class);
    when(authProvider.getRequestHeaders()).thenReturn(Map.of("Authorization", "Bearer test-token"));
    AtomicInteger requests = new AtomicInteger();
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient = (uri, authHeaders) -> {
      requests.incrementAndGet();
      return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
    };

    try (BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(helixManager, 5_000L, false, authProvider, routingStatusClient)) {
      checker.check();
      assertFalse(checker.isReady());
    }
    assertEquals(requests.get(), 0);
  }

  @Test
  public void testDynamicAuthHeadersAreResolvedOncePerRequest() {
    HelixManager helixManager = mockHelixManagerWithOnlineBroker(false);
    AuthProvider authProvider = mock(AuthProvider.class);
    when(authProvider.getRequestHeaders()).thenReturn(Map.of(),
        Map.of("Authorization", "Bearer must-not-be-sent"));
    AtomicReference<List<Header>> requestedAuthHeaders = new AtomicReference<>();
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient = (uri, authHeaders) -> {
      requestedAuthHeaders.set(authHeaders);
      return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
    };

    try (BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(helixManager, 5_000L, false, authProvider, routingStatusClient)) {
      checker.check();
      assertTrue(checker.isReady());
    }

    verify(authProvider, times(1)).getRequestHeaders();
    assertTrue(requestedAuthHeaders.get().isEmpty());
  }

  @Test
  public void testCloseCancelsActiveCheckWithoutWaitingForCheckerMonitor()
      throws Exception {
    HelixManager helixManager = mockHelixManagerWithOnlineBroker(false);
    CountDownLatch requestStarted = new CountDownLatch(1);
    CountDownLatch releaseRequest = new CountDownLatch(1);
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient =
        new BrokerRoutingReadyChecker.RoutingStatusClient() {
          @Override
          public SimpleHttpResponse get(URI uri, List<Header> authHeaders)
              throws IOException {
            requestStarted.countDown();
            try {
              if (!releaseRequest.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for the test client to close");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("Interrupted while waiting for the test client to close", e);
            }
            return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
          }

          @Override
          public void close() {
            releaseRequest.countDown();
          }
        };
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(helixManager, 5_000L, false,
        new NullAuthProvider(), routingStatusClient);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> check = executor.submit(checker::check);
      assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
      Future<?> close = executor.submit(checker::close);
      close.get(1, TimeUnit.SECONDS);
      check.get(5, TimeUnit.SECONDS);
      assertFalse(checker.isReady());
    } finally {
      releaseRequest.countDown();
      checker.close();
      executor.shutdownNow();
    }
  }

  private HelixManager mockHelixManagerWithOnlineBroker(boolean useHttps) {
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getInstanceName()).thenReturn(SERVER_INSTANCE);
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setStateMap("0", Map.of(BROKER_INSTANCE, "ONLINE"));
    when(helixAdmin.getResourceExternalView("testCluster", CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerResource);
    InstanceConfig instanceConfig = new InstanceConfig(BROKER_INSTANCE);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8099");
    if (useHttps) {
      instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.ADMIN_HTTPS_PORT_KEY, "8099");
    }
    when(helixAdmin.getInstanceConfig("testCluster", BROKER_INSTANCE)).thenReturn(instanceConfig);
    return helixManager;
  }

  private InstanceConfig createBrokerInstanceConfig(String instanceId, String port) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort(port);
    return instanceConfig;
  }
}
