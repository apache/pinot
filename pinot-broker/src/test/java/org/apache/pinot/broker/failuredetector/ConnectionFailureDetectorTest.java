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
package org.apache.pinot.broker.failuredetector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConnectionFailureDetectorTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";

  private BrokerMetrics _brokerMetrics;
  private FailureDetector _failureDetector;
  private Listener _listener;

  @BeforeClass
  public void setUp() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, 100);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    _brokerMetrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _failureDetector = FailureDetectorFactory.getFailureDetector(config, _brokerMetrics);
    assertTrue(_failureDetector instanceof ConnectionFailureDetector);
    _listener = new Listener();
    _failureDetector.register(_listener);
    _failureDetector.start();
  }

  @Test
  public void testConnectionFailure() {
    QueryResponse queryResponse = mock(QueryResponse.class);
    when(queryResponse.getFailedServer()).thenReturn(new ServerRoutingInstance("localhost", 1234));

    // No failure detection when submitting the query
    _failureDetector.notifyQuerySubmitted(queryResponse);
    verify(Collections.emptySet(), 0, 0);

    // When query finishes, the failed server should be count as unhealthy and trigger a callback
    _failureDetector.notifyQueryFinished(queryResponse);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server unhealthy again should have no effect
    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server healthy should remove it from the unhealthy servers and trigger a callback
    _failureDetector.markServerHealthy(INSTANCE_ID);
    verify(Collections.emptySet(), 1, 1);

    _listener.reset();
  }

  @Test
  public void testRetry() {
    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Should get 10 retries in 1s, then remove the failed server from the unhealthy servers.
    // Wait for up to 5s to avoid flakiness
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _listener._retryUnhealthyServerCalled.get();
      if (numRetries < Broker.FailureDetector.DEFAULT_MAX_RETIRES) {
        assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
        assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        return false;
      }
      assertEquals(numRetries, Broker.FailureDetector.DEFAULT_MAX_RETIRES);
      // There might be a small delay between the last retry and removing failed server from the unhealthy servers.
      // Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _listener._notifyUnhealthyServerCalled.get() == 1 && _listener._notifyHealthyServerCalled.get() == 1;
    }, 5_000L, "Failed to get 10 retires");

    _listener.reset();
  }

  private void verify(Set<String> expectedUnhealthyServers, int expectedNotifyUnhealthyServerCalled,
      int expectedNotifyHealthyServerCalled) {
    assertEquals(_failureDetector.getUnhealthyServers(), expectedUnhealthyServers);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS),
        expectedUnhealthyServers.size());
    assertEquals(_listener._notifyUnhealthyServerCalled.get(), expectedNotifyUnhealthyServerCalled);
    assertEquals(_listener._notifyHealthyServerCalled.get(), expectedNotifyHealthyServerCalled);
  }

  @AfterClass
  public void tearDown() {
    _failureDetector.stop();
  }

  private static class Listener implements FailureDetector.Listener {
    final AtomicInteger _notifyUnhealthyServerCalled = new AtomicInteger();
    final AtomicInteger _retryUnhealthyServerCalled = new AtomicInteger();
    final AtomicInteger _notifyHealthyServerCalled = new AtomicInteger();

    @Override
    public void notifyUnhealthyServer(String instanceId, FailureDetector failureDetector) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyUnhealthyServerCalled.getAndIncrement();
    }

    @Override
    public void retryUnhealthyServer(String instanceId, FailureDetector failureDetector) {
      assertEquals(instanceId, INSTANCE_ID);
      _retryUnhealthyServerCalled.getAndIncrement();
    }

    @Override
    public void notifyHealthyServer(String instanceId, FailureDetector failureDetector) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyHealthyServerCalled.getAndIncrement();
    }

    void reset() {
      _notifyUnhealthyServerCalled.set(0);
      _retryUnhealthyServerCalled.set(0);
      _notifyHealthyServerCalled.set(0);
    }
  }
}
