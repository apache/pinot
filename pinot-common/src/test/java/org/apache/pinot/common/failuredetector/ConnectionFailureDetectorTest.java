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

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConnectionFailureDetectorTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";

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
    _brokerMetrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
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
    verify(Collections.emptySet(), 0, 0);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server unhealthy again should have no effect
    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server healthy should remove it from the unhealthy servers and trigger a callback
    _failureDetector.markServerHealthy(INSTANCE_ID);
    verify(Collections.emptySet(), 1, 1);
  }

  @Test
  public void testRetryWithoutRecovery() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(10);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Should get 10 retries in 1s, then remove the failed server from the unhealthy servers.
    // Wait for up to 5s to avoid flakiness
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < Broker.FailureDetector.DEFAULT_MAX_RETRIES) {
        assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
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

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 7) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 5) {
          assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
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
    _unhealthyServerRetrier = new UnhealthyServerRetrier(7);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    UnhealthyServerRetrier unhealthyServerRetrier2 = new UnhealthyServerRetrier(8);
    _failureDetector.registerUnhealthyServerRetrier(unhealthyServerRetrier2);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Should retry until both unhealthy server retriers return that the server is healthy
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 9) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 7) {
          assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
          assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        }
        return false;
      }
      assertEquals(numRetries, 9);
      // There might be a small delay between the successful attempt and removing failed server from the unhealthy
      // servers. Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 5 retries");

    // Verify no further retries
    assertEquals(_unhealthyServerRetrier._retryUnhealthyServerCalled, 9);
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
    int _notifyUnhealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyUnhealthyServerCalled++;
    }
  }

  private static class UnhealthyServerRetrier implements Function<String, Boolean> {
    int _retryUnhealthyServerCalled = 0;
    final int _numFailures;

    UnhealthyServerRetrier(int numFailures) {
      _numFailures = numFailures;
    }

    @Override
    public Boolean apply(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _retryUnhealthyServerCalled++;
      return _retryUnhealthyServerCalled > _numFailures;
    }
  }
}
