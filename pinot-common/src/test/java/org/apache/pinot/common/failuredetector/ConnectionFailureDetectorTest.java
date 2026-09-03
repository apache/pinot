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
package org.apache.pinot.common.failuredetector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.failuredetector.FailureDetector.ServerState;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConnectionFailureDetectorTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";
  private static final String HOST_NAME = "localhost";

  private static final String OTHER_INSTANCE_ID = "Server_localhost_5678";

  /// Long enough that the retry loop never runs during a test that is not about it.
  private static final long RETRIES_OFF_MS = 600_000L;

  private final List<FailureDetector> _startedFailureDetectors = new ArrayList<>();
  private final List<String> _markedUnhealthy = new CopyOnWriteArrayList<>();
  private BrokerMetrics _brokerMetrics;
  private FailureDetector _failureDetector;
  private UnhealthyServerRetrier _unhealthyServerRetrier;
  private HealthyServerNotifier _healthyServerNotifier;
  private UnhealthyServerNotifier _unhealthyServerNotifier;

  @BeforeMethod
  public void setUp() {
    _markedUnhealthy.clear();
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, 100);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    _brokerMetrics = new BrokerMetrics(new NoopPinotMetricsRegistry());
    _failureDetector = FailureDetectorFactory.getFailureDetector(config, _brokerMetrics);
    assertTrue(_failureDetector instanceof ConnectionFailureDetector);
    _healthyServerNotifier = new HealthyServerNotifier();
    _failureDetector.registerHealthyServerNotifier(_healthyServerNotifier);
    _unhealthyServerNotifier = new UnhealthyServerNotifier();
    _failureDetector.registerUnhealthyServerNotifier(_unhealthyServerNotifier);
    _failureDetector.start();
  }

  @Test
  public void testConnectionFailure() {
    // No unhealthy servers initially
    verify(Set.of(), 0, 0);

    _failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);

    // Mark server unhealthy again should have no effect
    _failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);

    // Mark server healthy should remove it from the unhealthy servers and trigger a callback
    _failureDetector.markServerHealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(), 1, 1);
  }

  @Test
  public void testRetryWithoutRecovery() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(10);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    _failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);

    // Should get 10 retries in 1s, then remove the failed server from the unhealthy servers.
    // Wait for up to 5s to avoid flakiness
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < Broker.FailureDetector.DEFAULT_MAX_RETRIES) {
        assertEquals(_failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID));
        assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        return false;
      }
      assertEquals(numRetries, Broker.FailureDetector.DEFAULT_MAX_RETRIES);
      // There might be a small delay between the last retry and removing failed server from the unhealthy servers.
      // Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 10 retries");
  }

  @Test
  public void testRetryWithRecovery() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(6);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    _failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);

    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 7) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 5) {
          assertEquals(_failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID));
          assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        }
        return false;
      }
      assertEquals(numRetries, 7);
      // There might be a small delay between the successful attempt and removing failed server from the unhealthy
      // servers. Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 7 retries");

    // Verify no further retries
    assertEquals(_unhealthyServerRetrier._retryUnhealthyServerCalled, 7);
  }

  @Test
  public void testRetryWithMultipleUnhealthyServerRetriers() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(5);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    // This retrier will only be called after the first retrier starts returning HEALTHY. So we expect a total of 7
    // failures and 8 retries until the server is marked as healthy again.
    UnhealthyServerRetrier unhealthyServerRetrier2 = new UnhealthyServerRetrier(2);
    _failureDetector.registerUnhealthyServerRetrier(unhealthyServerRetrier2);

    // Register a retrier that isn't aware of the failing server. This should not affect the retry process.
    _failureDetector.registerUnhealthyServerRetrier(instanceId -> FailureDetector.ServerState.UNKNOWN);

    _failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);

    // Should retry until both unhealthy server retriers return that the server is healthy
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 8) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 5) {
          assertEquals(_failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID));
          assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        }
        return false;
      }
      assertEquals(numRetries, 8);
      // There might be a small delay between the successful attempt and removing failed server from the unhealthy
      // servers. Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 8 retries");

    // Verify no further retries
    assertEquals(_unhealthyServerRetrier._retryUnhealthyServerCalled, 8);
  }

  /// A server that leaves a query unanswered and then fails the follow-up check is marked unhealthy.
  @Test
  public void testServerFailingTheTimeoutCheckIsMarkedUnhealthy() {
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> ServerState.UNHEALTHY);
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);

    TestUtils.waitForCondition(aVoid -> failureDetector.getUnhealthyServers().contains(INSTANCE_ID), 5_000L,
        "Server failing the check was not marked unhealthy");
    assertEquals(_markedUnhealthy, List.of(INSTANCE_ID));
  }

  /// A server that leaves a query unanswered but passes the check -- busy, not dead -- is left alone.
  @Test
  public void testServerPassingTheTimeoutCheckIsLeftAlone() {
    AtomicInteger numChecks = new AtomicInteger();
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> {
      numChecks.incrementAndGet();
      return ServerState.HEALTHY;
    });
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);

    awaitCheckFinished(failureDetector, INSTANCE_ID, numChecks);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(_markedUnhealthy, List.of());
  }

  /// A check after a timeout runs the checkers only, never the retriers. A retrier may report a live server unhealthy
  /// -- the multi-stage one does for an idle gRPC channel -- which must not take a server that answered out of routing.
  @Test
  public void testTimeoutCheckIgnoresRetriers() {
    AtomicInteger numChecks = new AtomicInteger();
    AtomicInteger numRetries = new AtomicInteger();
    FailureDetector failureDetector = startFailureDetector(new BrokerMetrics(new NoopPinotMetricsRegistry()),
        RETRIES_OFF_MS, instanceId -> {
          numRetries.incrementAndGet();
          return ServerState.UNHEALTHY;
        }, instanceId -> {
          numChecks.incrementAndGet();
          return ServerState.HEALTHY;
        });
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);

    awaitCheckFinished(failureDetector, INSTANCE_ID, numChecks);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(numRetries.get(), 0);
  }

  /// A check that throws is inconclusive, and an inconclusive check must err towards keeping the server.
  @Test
  public void testThrowingTimeoutCheckDoesNotMarkServerUnhealthy() {
    AtomicInteger numChecks = new AtomicInteger();
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> {
      numChecks.incrementAndGet();
      throw new RuntimeException("Check failed to run");
    });
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);

    awaitCheckFinished(failureDetector, INSTANCE_ID, numChecks);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of());
  }

  /// A burst of timeouts against one server -- every query in flight to it, say -- checks it once.
  @Test
  public void testBurstOfTimeoutsChecksServerOnce() {
    CountDownLatch releaseCheck = new CountDownLatch(1);
    AtomicInteger numChecks = new AtomicInteger();
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> {
      numChecks.incrementAndGet();
      awaitQuietly(releaseCheck);
      return ServerState.HEALTHY;
    });
    for (int i = 0; i < 100; i++) {
      failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    TestUtils.waitForCondition(aVoid -> numChecks.get() == 1, 5_000L, "Check did not start");
    for (int i = 0; i < 100; i++) {
      failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    releaseCheck.countDown();
    assertEquals(numChecks.get(), 1);
  }

  /// A server already unhealthy is the retry loop's business; a timeout does not check it again.
  @Test
  public void testUnhealthyServerIsNotCheckedOnTimeout() {
    List<String> checkedServers = new CopyOnWriteArrayList<>();
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> {
      checkedServers.add(instanceId);
      return ServerState.HEALTHY;
    });
    failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    // The skip happens on the caller's thread, so once another server's check has run there is nothing left pending.
    failureDetector.notifyServerNotResponded(OTHER_INSTANCE_ID, HOST_NAME);

    TestUtils.waitForCondition(aVoid -> checkedServers.contains(OTHER_INSTANCE_ID), 5_000L, "Check did not run");
    assertEquals(checkedServers, List.of(OTHER_INSTANCE_ID));
  }

  /// With no checker registered -- pings disabled -- a timeout does nothing, while connection failures are still
  /// detected.
  @Test
  public void testTimeoutDoesNothingWithoutAChecker()
      throws InterruptedException {
    AtomicInteger numRetries = new AtomicInteger();
    FailureDetector failureDetector =
        startFailureDetector(new BrokerMetrics(new NoopPinotMetricsRegistry()), RETRIES_OFF_MS, instanceId -> {
          numRetries.incrementAndGet();
          return ServerState.UNHEALTHY;
        }, null);
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    // A check runs off this thread, within milliseconds; give one far longer than that before concluding there is none.
    Thread.sleep(500L);
    assertEquals(numRetries.get(), 0);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of());

    failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID));
  }

  /// Several servers going silent at once are all checked side by side, and all marked unhealthy. Each check waits
  /// here until both have started: run one after the other, the first would give up waiting and pass.
  @Test
  public void testTimeoutChecksRunInParallel() {
    CountDownLatch bothStarted = new CountDownLatch(2);
    FailureDetector failureDetector = startFailureDetectorWithChecker(instanceId -> {
      bothStarted.countDown();
      return awaitQuietly(bothStarted) ? ServerState.UNHEALTHY : ServerState.HEALTHY;
    });
    failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    failureDetector.notifyServerNotResponded(OTHER_INSTANCE_ID, HOST_NAME);

    TestUtils.waitForCondition(aVoid -> failureDetector.getUnhealthyServers().size() == 2, 5_000L,
        "Checks did not run in parallel");
    assertEquals(failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID, OTHER_INSTANCE_ID));
  }

  /// Retries run side by side too, so one dead server waiting out a ping timeout cannot delay another's recovery.
  @Test
  public void testRetriesRunInParallel() {
    CountDownLatch bothRetried = new CountDownLatch(2);
    FailureDetector failureDetector = startFailureDetectorWithRetrier(instanceId -> {
      bothRetried.countDown();
      return awaitQuietly(bothRetried) ? ServerState.HEALTHY : ServerState.UNHEALTHY;
    });
    failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    failureDetector.markServerUnhealthy(OTHER_INSTANCE_ID, HOST_NAME);

    TestUtils.waitForCondition(aVoid -> failureDetector.getUnhealthyServers().isEmpty(), 5_000L,
        "Retries did not run in parallel");
  }

  /// A retrier that throws counts as a failed retry. The server is retried again later rather than stranded unhealthy
  /// forever, which is what happened when the exception escaped the retry loop.
  @Test
  public void testThrowingRetryDoesNotStrandServer() {
    AtomicInteger numRetries = new AtomicInteger();
    FailureDetector failureDetector = startFailureDetectorWithRetrier(instanceId -> {
      if (numRetries.incrementAndGet() == 1) {
        throw new RuntimeException("Retry failed to run");
      }
      return ServerState.HEALTHY;
    });
    failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);

    TestUtils.waitForCondition(aVoid -> failureDetector.getUnhealthyServers().isEmpty(), 5_000L,
        "Server was stranded unhealthy");
    assertEquals(numRetries.get(), 2);
  }

  /// The gauge stays exact when servers change state concurrently, which parallel checks make routine. It used to be
  /// adjusted by one inside each map update, where two concurrent updates read the same size. Repeated, since a single
  /// round only sometimes hits the race.
  @Test
  public void testUnhealthyServerGaugeStaysExactUnderConcurrentChanges()
      throws Exception {
    BrokerMetrics brokerMetrics = new BrokerMetrics(new NoopPinotMetricsRegistry());
    FailureDetector failureDetector = startFailureDetector(brokerMetrics, RETRIES_OFF_MS, null, null);
    int numServers = 64;
    ExecutorService executorService = Executors.newFixedThreadPool(16);
    try {
      List<Callable<Void>> markUnhealthy = new ArrayList<>();
      List<Callable<Void>> markHealthy = new ArrayList<>();
      for (int i = 0; i < numServers; i++) {
        String instanceId = "Server_host_" + i;
        markUnhealthy.add(() -> {
          failureDetector.markServerUnhealthy(instanceId, HOST_NAME);
          return null;
        });
        markHealthy.add(() -> {
          failureDetector.markServerHealthy(instanceId, HOST_NAME);
          return null;
        });
      }
      for (int round = 0; round < 20; round++) {
        executorService.invokeAll(markUnhealthy);
        assertEquals(MetricValueUtils.getGlobalGaugeValue(brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), numServers);
        executorService.invokeAll(markHealthy);
        assertEquals(MetricValueUtils.getGlobalGaugeValue(brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 0);
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  /// Starts a detector that checks servers after a timeout with `checker`, and never retries during the test.
  private FailureDetector startFailureDetectorWithChecker(Function<String, ServerState> checker) {
    return startFailureDetector(new BrokerMetrics(new NoopPinotMetricsRegistry()), RETRIES_OFF_MS, null, checker);
  }

  /// Starts a detector that retries unhealthy servers with `retrier` after 100 ms, and has no timeout check.
  private FailureDetector startFailureDetectorWithRetrier(Function<String, ServerState> retrier) {
    return startFailureDetector(new BrokerMetrics(new NoopPinotMetricsRegistry()), 100L, retrier, null);
  }

  /// Starts a detector with the given retrier and timeout checker, each optional, recording the servers it marks
  /// unhealthy. Stopped after the test.
  private FailureDetector startFailureDetector(BrokerMetrics brokerMetrics, long retryInitialDelayMs,
      @Nullable Function<String, ServerState> retrier, @Nullable Function<String, ServerState> checker) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, retryInitialDelayMs);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    FailureDetector failureDetector = FailureDetectorFactory.getFailureDetector(config, brokerMetrics);
    failureDetector.registerUnhealthyServerNotifier(_markedUnhealthy::add);
    failureDetector.registerHealthyServerNotifier(instanceId -> {
    });
    if (retrier != null) {
      failureDetector.registerUnhealthyServerRetrier(retrier);
    }
    if (checker != null) {
      failureDetector.registerServerNotRespondedChecker(checker);
    }
    failureDetector.start();
    _startedFailureDetectors.add(failureDetector);
    return failureDetector;
  }

  /// Waits until the check of `instanceId` has finished, not merely started. A notification only starts a new check
  /// once the previous one has finished, so a second check proves the first is done. Stops early if the server got
  /// marked unhealthy, which ends its checks, and leaves that to the caller's assertions.
  private static void awaitCheckFinished(FailureDetector failureDetector, String instanceId, AtomicInteger numChecks) {
    TestUtils.waitForCondition(aVoid -> {
      failureDetector.notifyServerNotResponded(instanceId, HOST_NAME);
      return numChecks.get() >= 2 || failureDetector.getUnhealthyServers().contains(instanceId);
    }, 5_000L, "Check did not finish");
  }

  private static boolean awaitQuietly(CountDownLatch latch) {
    try {
      return latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void verify(Set<String> expectedUnhealthyServers, int expectedNotifyUnhealthyServerCalled,
      int expectedNotifyHealthyServerCalled) {
    assertEquals(_failureDetector.getUnhealthyServers(), expectedUnhealthyServers);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS),
        expectedUnhealthyServers.size());
    assertEquals(_unhealthyServerNotifier._notifyUnhealthyServerCalled, expectedNotifyUnhealthyServerCalled);
    assertEquals(_healthyServerNotifier._notifyHealthyServerCalled, expectedNotifyHealthyServerCalled);
  }

  @AfterMethod
  public void tearDown() {
    _failureDetector.stop();
    for (FailureDetector failureDetector : _startedFailureDetectors) {
      failureDetector.stop();
    }
    _startedFailureDetectors.clear();
  }

  private static class HealthyServerNotifier implements Consumer<String> {
    int _notifyHealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyHealthyServerCalled++;
    }
  }

  private static class UnhealthyServerNotifier implements Consumer<String> {
    int _notifyUnhealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyUnhealthyServerCalled++;
    }
  }

  private static class UnhealthyServerRetrier implements Function<String, FailureDetector.ServerState> {
    int _retryUnhealthyServerCalled = 0;
    final int _numFailures;

    UnhealthyServerRetrier(int numFailures) {
      _numFailures = numFailures;
    }

    @Override
    public FailureDetector.ServerState apply(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _retryUnhealthyServerCalled++;
      return _retryUnhealthyServerCalled > _numFailures ? FailureDetector.ServerState.HEALTHY
          : FailureDetector.ServerState.UNHEALTHY;
    }
  }
}
