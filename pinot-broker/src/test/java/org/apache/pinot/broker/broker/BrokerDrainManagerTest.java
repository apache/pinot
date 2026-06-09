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
package org.apache.pinot.broker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class BrokerDrainManagerTest {
  private static final String INSTANCE_ID = "Broker_localhost_8099";

  @Test
  public void testDrainWaitsForAcceptedQueriesBeforeShutdown()
      throws Exception {
    AtomicInteger drainStartedCount = new AtomicInteger();
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    BrokerDrainManager drainManager =
        BrokerDrainManager.localOnly("Broker_localhost_8099", drainStartedCount::incrementAndGet,
            shutdownLatch::countDown, 10_000L);

    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    assertEquals(drainManager.getStatus().getInFlightQueries(), 1);

    BrokerDrainManager.DrainStatus timedOutStatus = drainManager.drain(10L, true);
    assertFalse(timedOutStatus.isDrained());
    assertFalse(timedOutStatus.isShutdownTriggered());
    assertEquals(drainStartedCount.get(), 1);
    assertNull(drainManager.tryAcquireQuery());

    queryPermit.close();
    BrokerDrainManager.DrainStatus drainedStatus = drainManager.drain(10_000L, true);
    assertTrue(drainedStatus.isDrained());
    assertTrue(shutdownLatch.await(10, TimeUnit.SECONDS));
    assertTrue(drainManager.getStatus().isShutdownTriggered());
    assertEquals(drainStartedCount.get(), 1);
  }

  @Test
  public void testConcurrentQueryAdmissionAndDrainAreAtomic()
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 500; i++) {
        BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
        }, () -> {
        }, 10_000L);
        CyclicBarrier start = new CyclicBarrier(3);
        Future<BrokerDrainManager.QueryPermit> admission = executor.submit(() -> {
          start.await();
          return drainManager.tryAcquireQuery();
        });
        Future<BrokerDrainManager.DrainStatus> drain = executor.submit(() -> {
          start.await();
          return drainManager.drain(0L, false);
        });
        start.await();

        BrokerDrainManager.QueryPermit queryPermit = admission.get(10L, TimeUnit.SECONDS);
        BrokerDrainManager.DrainStatus drainStatus = drain.get(10L, TimeUnit.SECONDS);
        assertNull(drainManager.tryAcquireQuery(), "Queries must be rejected after the drain transition");
        if (queryPermit != null) {
          assertFalse(drainStatus.isDrained(), "An admitted query must be counted by the racing drain");
          assertEquals(drainStatus.getInFlightQueries(), 1);
          queryPermit.close();
          assertTrue(drainManager.drain(0L, false).isDrained());
        } else {
          assertTrue(drainStatus.isDrained());
          assertEquals(drainStatus.getInFlightQueries(), 0);
        }
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testConcurrentQueryPermitCloseReleasesExactlyOnce()
      throws Exception {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
    }, () -> {
    }, 10_000L);
    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    assertFalse(drainManager.drain(0L, false).isDrained());

    int numThreads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier start = new CyclicBarrier(numThreads + 1);
    List<Future<?>> closes = new ArrayList<>();
    try {
      for (int i = 0; i < numThreads; i++) {
        closes.add(executor.submit(() -> {
          start.await();
          queryPermit.close();
          return null;
        }));
      }
      start.await();
      for (Future<?> close : closes) {
        close.get(10L, TimeUnit.SECONDS);
      }
      assertEquals(drainManager.getStatus().getInFlightQueries(), 0);
      assertTrue(drainManager.isDrainComplete());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testDrainUsesDefaultTimeout()
      throws Exception {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 1L);

    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    try {
      BrokerDrainManager.DrainStatus status = drainManager.drain(-1L, false);
      assertFalse(status.isDrained());
      assertTrue(status.isDraining());
      assertEquals(status.getInFlightQueries(), 1);
    } finally {
      queryPermit.close();
    }
  }

  @Test
  public void testLongTimeoutDoesNotOverflow()
      throws Exception {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
    }, () -> {
    }, 10_000L);
    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<BrokerDrainManager.DrainStatus> drain =
          executor.submit(() -> drainManager.drain(Long.MAX_VALUE, false));
      TestUtils.waitForCondition(aVoid -> drainManager.isDraining(), 10_000L, "Drain did not start");
      expectThrows(TimeoutException.class, () -> drain.get(100L, TimeUnit.MILLISECONDS));

      queryPermit.close();
      assertTrue(drain.get(10L, TimeUnit.SECONDS).isDrained());
    } finally {
      queryPermit.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testShutdownCanBeRetriedAfterCallbackFailure()
      throws Exception {
    AtomicInteger shutdownAttempts = new AtomicInteger();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch secondAttempt = new CountDownLatch(1);
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
    }, () -> {
      if (shutdownAttempts.getAndIncrement() == 0) {
        firstAttempt.countDown();
        throw new IllegalStateException("injected shutdown failure");
      }
      secondAttempt.countDown();
    }, 10_000L);

    assertTrue(drainManager.drain(0L, true).isDrained());
    assertTrue(firstAttempt.await(10, TimeUnit.SECONDS));
    TestUtils.waitForCondition(aVoid -> !drainManager.getStatus().isShutdownTriggered(), 10_000L,
        "Shutdown callback failure did not reset the retry guard");

    assertTrue(drainManager.drain(0L, true).isDrained());
    assertTrue(secondAttempt.await(10, TimeUnit.SECONDS));
    assertEquals(shutdownAttempts.get(), 2);
  }

  @Test
  public void testDrainContinuesWhenDrainStartedCallbackFails()
      throws Exception {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
      throw new RuntimeException("callback failed");
    }, () -> {
    }, 10_000L);

    BrokerDrainManager.DrainStatus status = drainManager.drain(0L, false);

    assertTrue(status.isDraining());
    assertTrue(status.isDrained());
    assertFalse(status.isAcceptingQueries());
  }

  @Test
  public void testProductionDrainRequiresStartupReady()
      throws Exception {
    HelixDataAccessor helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.isConnected()).thenReturn(true);
    Mockito.when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    InstanceConfig instanceConfig = brokerInstanceConfig();
    AtomicInteger helixManagerLookups = new AtomicInteger();
    BrokerDrainManager drainManager = new BrokerDrainManager(INSTANCE_ID, () -> {
      helixManagerLookups.incrementAndGet();
      return helixManager;
    }, () -> {
    }, () -> {
    }, 10_000L);
    CountDownLatch startupReconciliationStarted = new CountDownLatch(1);
    CountDownLatch allowStartupReady = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (MockedStatic<HelixHelper> helixHelper = Mockito.mockStatic(HelixHelper.class)) {
      stubInstanceConfigUpdate(helixHelper, helixDataAccessor, instanceConfig);
      helixHelper.when(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any())).thenReturn(Mockito.mock(IdealState.class));
      Future<?> startup = executor.submit(() -> {
        startupReconciliationStarted.countDown();
        try {
          if (!allowStartupReady.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to finish startup reconciliation");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted during startup reconciliation", e);
        }
        drainManager.markStartupReady();
      });
      assertTrue(startupReconciliationStarted.await(10, TimeUnit.SECONDS));

      BrokerDrainManager.BrokerStartupInProgressException exception =
          expectThrows(BrokerDrainManager.BrokerStartupInProgressException.class,
              () -> drainManager.drain(0L, false));

      assertEquals(exception.getMessage(), "Broker " + INSTANCE_ID + " is still starting and cannot drain yet");
      assertEquals(helixManagerLookups.get(), 0);
      assertAcceptingQueriesAfterFailedDrain(drainManager, new AtomicInteger());
      Mockito.verifyNoInteractions(helixDataAccessor);

      allowStartupReady.countDown();
      startup.get(10, TimeUnit.SECONDS);

      BrokerDrainManager.DrainStatus status = drainManager.drain(0L, false);
      assertTrue(status.isDraining());
      assertTrue(status.isDrained());
      assertEquals(helixManagerLookups.get(), 1);
      assertTrue(instanceConfig.getRecord()
          .getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false));
      assertEquals(instanceConfig.getTags(), List.of("customTag"));
      assertEquals(instanceConfig.getRecord().getListField(CommonConstants.Helix.PREVIOUS_TAGS),
          List.of(TagNameUtils.getBrokerTagForTenant("tenant"), "customTag"));
      helixHelper.verify(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any()));
    } finally {
      allowStartupReady.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testDrainBeforeHelixConnectionKeepsAcceptingQueries() {
    AtomicInteger drainStartedCount = new AtomicInteger();
    BrokerDrainManager drainManager =
        new BrokerDrainManager(INSTANCE_ID, () -> null, drainStartedCount::incrementAndGet, () -> {
        }, 10_000L);
    drainManager.markStartupReady();

    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> drainManager.drain(0L, false));

    assertEquals(exception.getMessage(), "Broker participant Helix manager is not connected");
    assertAcceptingQueriesAfterFailedDrain(drainManager, drainStartedCount);
  }

  @Test
  public void testShutdownMarkerFailureKeepsAcceptingQueriesAndCanRetry()
      throws Exception {
    HelixDataAccessor helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.isConnected()).thenReturn(true);
    Mockito.when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    InstanceConfig instanceConfig = brokerInstanceConfig();
    AtomicInteger drainStartedCount = new AtomicInteger();
    BrokerDrainManager drainManager =
        new BrokerDrainManager(INSTANCE_ID, () -> helixManager, drainStartedCount::incrementAndGet, () -> {
        }, 10_000L);
    drainManager.markStartupReady();

    try (MockedStatic<HelixHelper> helixHelper = Mockito.mockStatic(HelixHelper.class)) {
      AtomicInteger markerUpdateAttempts = new AtomicInteger();
      helixHelper.when(() -> HelixHelper.updateInstanceConfig(Mockito.same(helixDataAccessor),
          Mockito.eq(INSTANCE_ID), Mockito.<Consumer<InstanceConfig>>any())).thenAnswer(invocation -> {
            if (markerUpdateAttempts.getAndIncrement() == 0) {
              throw new RuntimeException("Helix update failed");
            }
            Consumer<InstanceConfig> updater = invocation.getArgument(2);
            updater.accept(instanceConfig);
            return true;
          });
      helixHelper.when(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any())).thenReturn(Mockito.mock(IdealState.class));

      RuntimeException exception = expectThrows(RuntimeException.class, () -> drainManager.drain(0L, false));

      assertEquals(exception.getMessage(), "Helix update failed");
      assertFalse(drainManager.isDraining());
      assertFalse(drainManager.getStatus().isDrained());
      assertFalse(drainManager.getStatus().isShutdownMarkerReconciled());
      assertFalse(drainManager.getStatus().isBrokerResourceReconciled());
      assertAcceptingQueriesAfterFailedDrain(drainManager, drainStartedCount);

      BrokerDrainManager.DrainStatus retryStatus = drainManager.drain(0L, false);
      assertTrue(retryStatus.isDrained());
      assertEquals(drainStartedCount.get(), 1);
      assertEquals(markerUpdateAttempts.get(), 2);
      assertTrue(instanceConfig.getRecord()
          .getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false));
      helixHelper.verify(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any()), Mockito.times(1));
    }
  }

  @Test
  public void testBrokerResourceUpdateFailureCanRetryDrain()
      throws Exception {
    HelixDataAccessor helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.isConnected()).thenReturn(true);
    Mockito.when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    InstanceConfig instanceConfig = brokerInstanceConfig();
    AtomicInteger drainStartedCount = new AtomicInteger();
    BrokerDrainManager drainManager =
        new BrokerDrainManager(INSTANCE_ID, () -> helixManager, drainStartedCount::incrementAndGet, () -> {
        }, 10_000L);
    drainManager.markStartupReady();
    String tableNameWithType = "testTable_OFFLINE";
    IdealState brokerResource = new IdealState(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setPartitionState(tableNameWithType, INSTANCE_ID, "ONLINE");
    AtomicInteger brokerResourceUpdateAttempts = new AtomicInteger();
    try (MockedStatic<HelixHelper> helixHelper = Mockito.mockStatic(HelixHelper.class)) {
      stubInstanceConfigUpdate(helixHelper, helixDataAccessor, instanceConfig);
      helixHelper.when(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any()))
          .thenAnswer(invocation -> {
            if (brokerResourceUpdateAttempts.getAndIncrement() == 0) {
              throw new RuntimeException("brokerResource update failed");
            }
            Function<IdealState, IdealState> updater = invocation.getArgument(2);
            return updater.apply(brokerResource);
          });

      RuntimeException exception = expectThrows(RuntimeException.class, () -> drainManager.drain(0L, false));

      assertEquals(exception.getMessage(), "brokerResource update failed");
      assertTrue(instanceConfig.getRecord()
          .getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false));
      assertFalse(drainManager.isDraining());
      assertFalse(drainManager.getStatus().isDrained());
      assertTrue(drainManager.getStatus().isShutdownMarkerReconciled());
      assertFalse(drainManager.getStatus().isBrokerResourceReconciled());
      assertAcceptingQueriesAfterFailedDrain(drainManager, drainStartedCount);
      assertTrue(brokerResource.getInstanceSet(tableNameWithType).contains(INSTANCE_ID));

      BrokerDrainManager.DrainStatus retryStatus = drainManager.drain(0L, false);
      assertTrue(retryStatus.isDraining());
      assertTrue(retryStatus.isDrained());
      assertEquals(retryStatus.getTablesRemovedFromBrokerResource(), List.of(tableNameWithType));
      assertFalse(brokerResource.getInstanceSet(tableNameWithType).contains(INSTANCE_ID));
      assertEquals(brokerResourceUpdateAttempts.get(), 2);
      assertEquals(drainStartedCount.get(), 1);
      helixHelper.verify(() -> HelixHelper.updateInstanceConfig(Mockito.same(helixDataAccessor),
          Mockito.eq(INSTANCE_ID), Mockito.<Consumer<InstanceConfig>>any()), Mockito.times(1));
    }
  }

  @Test
  public void testInterruptedBrokerResourceUpdateCanRetryDrain()
      throws Exception {
    HelixDataAccessor helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.isConnected()).thenReturn(true);
    Mockito.when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    InstanceConfig instanceConfig = brokerInstanceConfig();
    BrokerDrainManager drainManager = new BrokerDrainManager(INSTANCE_ID, () -> helixManager, () -> {
    }, () -> {
    }, 10_000L);
    drainManager.markStartupReady();
    String tableNameWithType = "testTable_OFFLINE";
    IdealState brokerResource = new IdealState(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setPartitionState(tableNameWithType, INSTANCE_ID, "ONLINE");
    AtomicInteger brokerResourceUpdateAttempts = new AtomicInteger();
    try (MockedStatic<HelixHelper> helixHelper = Mockito.mockStatic(HelixHelper.class)) {
      stubInstanceConfigUpdate(helixHelper, helixDataAccessor, instanceConfig);
      helixHelper.when(() -> HelixHelper.updateIdealState(Mockito.same(helixManager),
          Mockito.eq(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE),
          Mockito.<Function<IdealState, IdealState>>any())).thenAnswer(invocation -> {
            if (brokerResourceUpdateAttempts.getAndIncrement() == 0) {
              return null;
            }
            Function<IdealState, IdealState> updater = invocation.getArgument(2);
            return updater.apply(brokerResource);
          });

      IllegalStateException exception =
          expectThrows(IllegalStateException.class, () -> drainManager.drain(0L, false));

      assertEquals(exception.getMessage(),
          "Interrupted while removing broker instance " + INSTANCE_ID + " from brokerResource");
      assertFalse(drainManager.isDraining());
      assertTrue(drainManager.getStatus().isShutdownMarkerReconciled());
      assertFalse(drainManager.getStatus().isBrokerResourceReconciled());
      assertFalse(drainManager.getStatus().isDrained());
      assertTrue(drainManager.getStatus().isAcceptingQueries());
      try (BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery()) {
        assertNotNull(queryPermit);
      }

      BrokerDrainManager.DrainStatus retryStatus = drainManager.drain(0L, false);
      assertTrue(retryStatus.isDrained());
      assertEquals(retryStatus.getTablesRemovedFromBrokerResource(), List.of(tableNameWithType));
      assertFalse(brokerResource.getInstanceSet(tableNameWithType).contains(INSTANCE_ID));
      assertEquals(brokerResourceUpdateAttempts.get(), 2);
    }
  }

  @Test
  public void testConcurrentDrainWaitsForInitialization() throws Exception {
    CountDownLatch helixLookupStarted = new CountDownLatch(1);
    CountDownLatch allowHelixLookupToFail = new CountDownLatch(1);
    BrokerDrainManager drainManager = new BrokerDrainManager(INSTANCE_ID, () -> {
      helixLookupStarted.countDown();
      try {
        if (!allowHelixLookupToFail.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Timed out waiting to fail Helix lookup");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting to fail Helix lookup", e);
      }
      return null;
    }, () -> {
    }, () -> {
    }, 10_000L);
    drainManager.markStartupReady();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<BrokerDrainManager.DrainStatus> firstDrain = executor.submit(() -> drainManager.drain(0L, false));
      assertTrue(helixLookupStarted.await(10, TimeUnit.SECONDS));
      CountDownLatch secondDrainStarted = new CountDownLatch(1);
      Future<BrokerDrainManager.DrainStatus> secondDrain = executor.submit(() -> {
        secondDrainStarted.countDown();
        return drainManager.drain(0L, false);
      });
      assertTrue(secondDrainStarted.await(10, TimeUnit.SECONDS));

      expectThrows(TimeoutException.class, () -> secondDrain.get(1, TimeUnit.SECONDS));
      allowHelixLookupToFail.countDown();

      assertDrainFailedBeforeHelixConnection(firstDrain);
      assertDrainFailedBeforeHelixConnection(secondDrain);
      assertFalse(drainManager.isDraining());
      try (BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery()) {
        assertNotNull(queryPermit);
      }
    } finally {
      allowHelixLookupToFail.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertAcceptingQueriesAfterFailedDrain(BrokerDrainManager drainManager,
      AtomicInteger drainStartedCount) {
    BrokerDrainManager.DrainStatus status = drainManager.getStatus();
    assertFalse(status.isDraining());
    assertTrue(status.isAcceptingQueries());
    assertEquals(status.getDrainStartTimeMs(), -1L);
    assertEquals(drainStartedCount.get(), 0);
    try (BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery()) {
      assertNotNull(queryPermit);
    }
  }

  private static void assertDrainFailedBeforeHelixConnection(Future<BrokerDrainManager.DrainStatus> drain) {
    ExecutionException exception =
        expectThrows(ExecutionException.class, () -> drain.get(10, TimeUnit.SECONDS));
    assertTrue(exception.getCause() instanceof IllegalStateException);
    assertEquals(exception.getCause().getMessage(), "Broker participant Helix manager is not connected");
  }

  private static InstanceConfig brokerInstanceConfig() {
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_ID);
    instanceConfig.addTag(TagNameUtils.getBrokerTagForTenant("tenant"));
    instanceConfig.addTag("customTag");
    return instanceConfig;
  }

  private static void stubInstanceConfigUpdate(MockedStatic<HelixHelper> helixHelper,
      HelixDataAccessor helixDataAccessor, InstanceConfig instanceConfig) {
    helixHelper.when(() -> HelixHelper.updateInstanceConfig(Mockito.same(helixDataAccessor),
        Mockito.eq(INSTANCE_ID), Mockito.<Consumer<InstanceConfig>>any())).thenAnswer(invocation -> {
          Consumer<InstanceConfig> updater = invocation.getArgument(2);
          updater.accept(instanceConfig);
          return true;
        });
  }
}
