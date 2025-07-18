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
package org.apache.pinot.core.data.manager.realtime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class ServerRateLimitConfigChangeListenerTest {

  private static final PinotConfiguration SERVER_CONFIG = mock(PinotConfiguration.class);
  private static final double DELTA = 0.0001;
  private static final ServerMetrics MOCK_SERVER_METRICS = mock(ServerMetrics.class);

  static {
    when(SERVER_CONFIG.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);
  }

  @Test
  public void testRateLimitUpdate()
      throws InterruptedException {
    AtomicReference<Throwable> errorRef = new AtomicReference<>();
    simulateThrottling(errorRef);
    // Initial state
    RealtimeConsumptionRateManager.getInstance().createServerRateLimiter(SERVER_CONFIG, null);
    RealtimeConsumptionRateManager.ServerRateLimiter serverRateLimiter = getServerRateLimiter();
    double initialRate = serverRateLimiter.getRate();
    assertEquals(initialRate, 5.0, DELTA);

    // Simulate config change
    Map<String, String> newConfig = new HashMap<>();
    newConfig.put(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT, "300.0");
    ServerRateLimitConfigChangeListener listener = new ServerRateLimitConfigChangeListener(MOCK_SERVER_METRICS);
    Set<String> changedConfigSet =
        new HashSet<>(List.of(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT));
    simulateThrottling(errorRef);
    listener.onChange(changedConfigSet, newConfig);
    simulateThrottling(errorRef);

    // Verify that rate changed
    double rate = serverRateLimiter.getRate();
    assertEquals(rate, 300.0, DELTA);
    double updatedRate = getServerRateLimiter().getRate();
    assertEquals(updatedRate, 300.0, DELTA);

    // Test removal of serverRateLimit
    newConfig = new HashMap<>();
    newConfig.put(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT, "0");
    changedConfigSet = new HashSet<>(List.of(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT));
    simulateThrottling(errorRef);
    listener.onChange(changedConfigSet, newConfig);
    simulateThrottling(errorRef);

    // Verify that old rate remains same and the new rate is applied
    rate = serverRateLimiter.getRate();
    assertEquals(rate, 300.0, DELTA);

    assertEquals(RealtimeConsumptionRateManager.NOOP_RATE_LIMITER,
        RealtimeConsumptionRateManager.getInstance().getServerRateLimiter());

    // Test update of serverRateLimit after it was removed
    newConfig = new HashMap<>();
    newConfig.put(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT, "10000");
    changedConfigSet = new HashSet<>(List.of(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT));
    simulateThrottling(errorRef);
    listener.onChange(changedConfigSet, newConfig);
    simulateThrottling(errorRef);

    // Verify that old rate (one before the config change was deleted and again added) remains same.
    rate = serverRateLimiter.getRate();
    assertEquals(rate, 300.0, DELTA);

    updatedRate = getServerRateLimiter().getRate();
    assertEquals(updatedRate, 10000, DELTA);

    Thread.sleep(1000);
    if (errorRef.get() != null) {
      throw new RuntimeException("Throttle call failed: " + errorRef.get().getMessage());
    }
  }

  private void simulateThrottling(AtomicReference<Throwable> errorRef) {
    // A helper method to test side effects of throttling during serverRateLimit config change.
    for (int i = 0; i < 10; i++) {
      CompletableFuture.runAsync(() -> {
        try {
          RealtimeConsumptionRateManager.getInstance().getServerRateLimiter().throttle(100);
        } catch (Throwable throwable) {
          errorRef.set(throwable);
        }
      });
    }
  }

  private RealtimeConsumptionRateManager.ServerRateLimiter getServerRateLimiter() {
    return (RealtimeConsumptionRateManager.ServerRateLimiter) (RealtimeConsumptionRateManager.getInstance()
        .getServerRateLimiter());
  }
}
