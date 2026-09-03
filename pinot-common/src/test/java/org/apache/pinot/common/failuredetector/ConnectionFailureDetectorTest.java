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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConnectionFailureDetectorTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";
  private static final String HOST_NAME = "localhost";

  private static final String OTHER_INSTANCE_ID = "Server_localhost_5678";

  private BrokerMetrics _brokerMetrics;
  private FailureDetector _failureDetector;
  private UnhealthyServerRetrier _unhealthyServerRetrier;
  private HealthyServerNotifier _healthyServerNotifier;
  private UnhealthyServerNotifier _unhealthyServerNotifier;

  @BeforeMethod
  public void setUp() {
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

  /// A server that leaves requests unanswered while staying connected is only ejected once it has missed
  /// [Broker.FailureDetector#DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD] consecutive requests. This pins the shipped
  /// default: lowering it would make the broker quicker to eject a slow-but-healthy replica.
  @Test
  public void testTimeoutDetectionUsesConfiguredThreshold() {
    int threshold = Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD;
    assertTrue(threshold > 1, "The default must require more than one unanswered request");

    for (int i = 0; i < threshold - 1; i++) {
      _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
      verify(Set.of(), 0, 0);
    }

    _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    verify(Set.of(INSTANCE_ID), 1, 0);
  }

  /// The count is consecutive, not cumulative: any response from the server discards it, so timeouts on either
  /// side of a successful response can never be summed into an ejection.
  @Test
  public void testResponseResetsConsecutiveTimeoutCount() {
    int threshold = Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD;
    for (int i = 0; i < threshold - 1; i++) {
      _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    verify(Set.of(), 0, 0);

    _failureDetector.notifyServerResponded(INSTANCE_ID);

    for (int i = 0; i < threshold - 1; i++) {
      _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    verify(Set.of(), 0, 0);
  }

  /// A response is not the only way a count is cleared: the retrier marking the server healthy again clears it too,
  /// so a server that has genuinely recovered starts from a clean slate.
  @Test
  public void testMarkServerHealthyClearsTimeoutCount() {
    int threshold = Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD;
    for (int i = 0; i < threshold; i++) {
      _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    verify(Set.of(INSTANCE_ID), 1, 0);

    _failureDetector.markServerHealthy(INSTANCE_ID, HOST_NAME);
    verify(Set.of(), 1, 1);

    // The full threshold has to be reached again, so a single further unanswered request changes nothing.
    _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    verify(Set.of(), 1, 1);
  }

  /// Several servers going silent at once are all ejected. There is no cap: a shared cause makes servers fail
  /// together, and the caller only attributes a timeout to a sole non-responder, so a shared cause accuses nobody.
  /// Independent servers each going silent while their peers serve is the case that should shed all of them.
  @Test
  public void testEveryServerThatGoesSilentIsEjected() {
    int threshold = Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD;
    for (int i = 0; i < threshold; i++) {
      _failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
      _failureDetector.notifyServerNotResponded(OTHER_INSTANCE_ID, HOST_NAME);
    }
    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID, OTHER_INSTANCE_ID));
    assertEquals(_unhealthyServerNotifier._notifiedServers, List.of(INSTANCE_ID, OTHER_INSTANCE_ID));
  }

  /// A threshold of zero or less disables timeout-based detection, leaving only connection-failure detection --
  /// the escape hatch for operators who do not want the new behaviour.
  @Test
  public void testNonPositiveThresholdDisablesTimeoutDetection() {
    FailureDetector failureDetector = newFailureDetector(0);
    for (int i = 0; i < Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUT_THRESHOLD * 2; i++) {
      failureDetector.notifyServerNotResponded(INSTANCE_ID, HOST_NAME);
    }
    assertEquals(failureDetector.getUnhealthyServers(), Set.of());

    // Connection-failure detection still works.
    failureDetector.markServerUnhealthy(INSTANCE_ID, HOST_NAME);
    assertEquals(failureDetector.getUnhealthyServers(), Set.of(INSTANCE_ID));
  }

  /// Builds an extra detector with its own metrics registry, so that it does not share the gauge with the one built
  /// by [#setUp()]. Not started: no caller here needs the retry thread.
  private FailureDetector newFailureDetector(int consecutiveTimeoutThreshold) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_CONSECUTIVE_TIMEOUT_THRESHOLD, consecutiveTimeoutThreshold);
    FailureDetector failureDetector =
        FailureDetectorFactory.getFailureDetector(config, new BrokerMetrics(new NoopPinotMetricsRegistry()));
    failureDetector.registerHealthyServerNotifier(instanceId -> {
    });
    failureDetector.registerUnhealthyServerNotifier(instanceId -> {
    });
    return failureDetector;
  }

  private void verify(Set<String> expectedUnhealthyServers, int expectedNotifyUnhealthyServerCalled,
      int expectedNotifyHealthyServerCalled) {
    assertEquals(_failureDetector.getUnhealthyServers(), expectedUnhealthyServers);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS),
        expectedUnhealthyServers.size());
    assertEquals(_unhealthyServerNotifier._notifyUnhealthyServerCalled, expectedNotifyUnhealthyServerCalled);
    assertEquals(_healthyServerNotifier._notifyHealthyServerCalled, expectedNotifyHealthyServerCalled);
  }

  @AfterClass
  public void tearDown() {
    _failureDetector.stop();
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
    final List<String> _notifiedServers = new ArrayList<>();
    int _notifyUnhealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      _notifiedServers.add(instanceId);
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
