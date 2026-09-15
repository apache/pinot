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

import java.net.URI;
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
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingReadyCheckerTest {
  private static final String SERVER_INSTANCE = "Server_localhost_8098";
  private static final String BROKER_INSTANCE = "Broker_localhost_8099";

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
    when(helixAdmin.getInstanceConfig("testCluster", BROKER_INSTANCE)).thenReturn(instanceConfig);

    URI expectedUri = URI.create("http://localhost:8099/routing/server/" + SERVER_INSTANCE);
    AuthProvider authProvider = mock(AuthProvider.class);
    AtomicReference<URI> requestedUri = new AtomicReference<>();
    AtomicReference<AuthProvider> requestedAuthProvider = new AtomicReference<>();
    BrokerRoutingReadyChecker.RoutingStatusClient routingStatusClient = (uri, provider) -> {
      requestedUri.set(uri);
      requestedAuthProvider.set(provider);
      return new SimpleHttpResponse(200, CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
    };

    try (BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(helixManager, 5_000L, false, authProvider, routingStatusClient)) {
      assertTrue(checker.isReady());
    }

    assertEquals(requestedUri.get(), expectedUri);
    assertSame(requestedAuthProvider.get(), authProvider);
  }

  @Test
  public void testWaitsForOnlineBrokers() {
    AtomicReference<Set<String>> onlineBrokers = new AtomicReference<>(Set.of());
    BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(SERVER_INSTANCE, onlineBrokers::get, brokers -> true);

    assertFalse(checker.isReady());

    onlineBrokers.set(Set.of("Broker_localhost_8099"));
    assertTrue(checker.isReady());
  }

  @Test
  public void testRetriesUntilAllBrokersConfirm() {
    AtomicInteger attempts = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099", "Broker_localhost_8100"),
        brokers -> attempts.incrementAndGet() > 1);

    assertEquals(attempts.get(), 0);
    assertFalse(checker.isReady());

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

    assertFalse(checker.isReady());
  }

  @Test
  public void testTimeoutFailsOpen() {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099"), brokers -> false, 5_000L, true, currentTimeMs::get);

    assertFalse(checker.isReady());

    currentTimeMs.set(6_000L);
    assertTrue(checker.isReady());
  }

  @Test
  public void testTimeoutFailsClosedButRecovers() {
    AtomicLong currentTimeMs = new AtomicLong(1_000L);
    AtomicReference<Boolean> brokerReady = new AtomicReference<>(false);
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099"), brokers -> brokerReady.get(), 5_000L, false, currentTimeMs::get);

    currentTimeMs.set(6_000L);
    assertFalse(checker.isReady());

    brokerReady.set(true);
    assertTrue(checker.isReady());
  }

  @Test
  public void testConcurrentReadinessChecksAreSerialized() throws Exception {
    CountDownLatch firstCheckStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstCheck = new CountDownLatch(1);
    CountDownLatch secondRequestStarted = new CountDownLatch(1);
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
      Future<Boolean> firstResult = callers.submit(checker::isReady);
      assertTrue(firstCheckStarted.await(5, TimeUnit.SECONDS));
      Future<Boolean> secondResult = callers.submit(() -> {
        secondRequestStarted.countDown();
        return checker.isReady();
      });
      assertTrue(secondRequestStarted.await(5, TimeUnit.SECONDS));
      assertFalse(secondResult.isDone());

      releaseFirstCheck.countDown();
      assertFalse(firstResult.get(5, TimeUnit.SECONDS));
      assertFalse(secondResult.get(5, TimeUnit.SECONDS));
      assertEquals(attempts.get(), 2);
      assertEquals(maxActiveChecks.get(), 1);
    } finally {
      releaseFirstCheck.countDown();
      callers.shutdownNow();
    }
  }
}
