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
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ByteCountThrottlingStrategy;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.MessageCountThrottlingStrategy;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ServerRateLimiter;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ThrottlingStrategy;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class ServerRateLimitConfigChangeListenerTest {

  private static final PinotConfiguration SERVER_CONFIG = mock(PinotConfiguration.class);
  private static final double DELTA = 0.0001;
  private static final ServerMetrics MOCK_SERVER_METRICS = mock(ServerMetrics.class);

  @Test
  public void testRateLimitUpdate()
      throws InterruptedException {
    String rateLimiterConfigKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT;
    if (Math.random() < 0.5) {
      rateLimiterConfigKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES;
    }

    when(SERVER_CONFIG.getProperty(rateLimiterConfigKey,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);

    AtomicReference<Throwable> errorRef = new AtomicReference<>();
    simulateThrottling(errorRef);
    // Initial state
    RealtimeConsumptionRateManager.getInstance().createServerRateLimiter(SERVER_CONFIG, null);
    RealtimeConsumptionRateManager.ServerRateLimiter serverRateLimiter = getServerRateLimiter();
    double initialRate = serverRateLimiter.getRate();
    assertEquals(initialRate, 5.0, DELTA);

    // Simulate config change
    Map<String, String> newConfig = new HashMap<>();
    newConfig.put(rateLimiterConfigKey, "300.0");
    ServerRateLimitConfigChangeListener listener = new ServerRateLimitConfigChangeListener(MOCK_SERVER_METRICS);
    Set<String> changedConfigSet = new HashSet<>(List.of(rateLimiterConfigKey));
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
    newConfig.put(rateLimiterConfigKey, "0");
    changedConfigSet = new HashSet<>(List.of(rateLimiterConfigKey));
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
    newConfig.put(rateLimiterConfigKey, "10000");
    changedConfigSet = new HashSet<>(List.of(rateLimiterConfigKey));
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

  @Test
  public void testRateLimitConfigChangeUpdatesGauge() {
    ServerMetrics metrics = mock(ServerMetrics.class);
    ServerRateLimitConfigChangeListener listener = new ServerRateLimitConfigChangeListener(metrics);
    String byteKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES;

    // Enabling/updating the limit via cluster config re-emits the configured-cap gauge with the new value.
    listener.onChange(new HashSet<>(List.of(byteKey)), Map.of(byteKey, "150000000"));
    verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, 150_000_000L);

    // Disabling via cluster config resets the gauge to -1.
    listener.onChange(new HashSet<>(List.of(byteKey)), Map.of(byteKey, "0"));
    verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, -1L);
  }

  /// Every supported key combination updates the real limiter and charges the expected batch units.
  @Test
  public void testDeterministicRateLimitModesChargeExpectedUnits() {
    String messageKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT;
    String byteKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES;
    List<Map<String, String>> configurations = List.of(
        Map.of(messageKey, "100000000"),
        Map.of(byteKey, "200000000"),
        Map.of(messageKey, "100000000", byteKey, "200000000"));
    double[] expectedRates = {100_000_000, 200_000_000, 200_000_000};
    List<ThrottlingStrategy> expectedStrategies = List.of(
        MessageCountThrottlingStrategy.INSTANCE, ByteCountThrottlingStrategy.INSTANCE,
        ByteCountThrottlingStrategy.INSTANCE);
    RealtimeConsumptionRateManager manager = RealtimeConsumptionRateManager.getInstance();
    for (int i = 0; i < configurations.size(); i++) {
      // Each variant owns a fresh limiter and emitter. High rates avoid waiting on throttling permits.
      manager.updateServerRateLimiter(RealtimeConsumptionRateManager.resolveServerRateLimit(-1, -1), null);
      ServerMetrics metrics = mock(ServerMetrics.class);
      try {
        Map<String, String> configuration = configurations.get(i);
        new ServerRateLimitConfigChangeListener(metrics).onChange(configuration.keySet(), configuration);
        ServerRateLimiter limiter = (ServerRateLimiter) manager.getServerRateLimiter();
        assertEquals(limiter.getRate(), expectedRates[i], DELTA);
        assertSame(limiter.getThrottlingStrategy(), expectedStrategies.get(i));
        verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, (long) expectedRates[i]);

        MessageBatch batch = mock(MessageBatch.class);
        when(batch.getMessageCount()).thenReturn(7);
        when(batch.getSizeInBytes()).thenReturn(11L);
        limiter.throttle(batch);
        if (i == 0) {
          verify(batch).getMessageCount();
          verify(batch, never()).getSizeInBytes();
        } else {
          verify(batch).getSizeInBytes();
          verify(batch, never()).getMessageCount();
        }
      } finally {
        // The real disable path closes the owned emitter and restores the singleton's no-op limiter.
        manager.updateServerRateLimiter(RealtimeConsumptionRateManager.resolveServerRateLimit(-1, -1), null);
        assertSame(manager.getServerRateLimiter(), RealtimeConsumptionRateManager.NOOP_RATE_LIMITER);
      }
    }
  }

  /// Invalid and unrelated changes must preserve the active cap, mode, limiter identity, and configured-cap gauge.
  @Test
  public void testRejectedRateLimitChangesPreserveActiveLimiterAndGauge() {
    String messageKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT;
    String byteKey = CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES;
    RealtimeConsumptionRateManager manager = RealtimeConsumptionRateManager.getInstance();
    manager.updateServerRateLimiter(RealtimeConsumptionRateManager.resolveServerRateLimit(-1, -1), null);
    ServerMetrics metrics = mock(ServerMetrics.class);
    ServerRateLimitConfigChangeListener listener = new ServerRateLimitConfigChangeListener(metrics);
    try {
      listener.onChange(Set.of(messageKey), Map.of(messageKey, "100000000"));
      ServerRateLimiter active = (ServerRateLimiter) manager.getServerRateLimiter();
      verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, 100_000_000L);
      clearInvocations(metrics);

      List<Set<String>> changedKeys = List.of(
          Set.of(messageKey), Set.of(byteKey), Set.of(messageKey, byteKey), Set.of("unrelated.setting"), Set.of());
      List<Map<String, String>> configurations = List.of(
          Map.of(messageKey, "not-a-number"),
          Map.of(byteKey, "not-a-number"),
          Map.of(messageKey, "not-a-number", byteKey, "200000000"),
          Map.of(messageKey, "200000000", byteKey, "300000000"),
          Map.of(messageKey, "200000000", byteKey, "300000000"));
      for (int i = 0; i < changedKeys.size(); i++) {
        listener.onChange(changedKeys.get(i), configurations.get(i));
        assertSame(manager.getServerRateLimiter(), active);
        assertEquals(active.getRate(), 100_000_000, DELTA);
        assertSame(active.getThrottlingStrategy(), MessageCountThrottlingStrategy.INSTANCE);
        // Periodic utilization emission may continue; no configured-cap update is permitted.
        verify(metrics, never()).setValueOfGlobalGauge(eq(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT), anyLong());
      }
    } finally {
      manager.updateServerRateLimiter(RealtimeConsumptionRateManager.resolveServerRateLimit(-1, -1), null);
      assertSame(manager.getServerRateLimiter(), RealtimeConsumptionRateManager.NOOP_RATE_LIMITER);
    }
  }

  private void simulateThrottling(AtomicReference<Throwable> errorRef) {
    // A helper method to test side effects of throttling during serverRateLimit config change.
    MessageBatch messageBatch = Mockito.mock(MessageBatch.class);
    when(messageBatch.getMessageCount()).thenReturn(100);
    for (int i = 0; i < 10; i++) {
      CompletableFuture.runAsync(() -> {
        try {
          RealtimeConsumptionRateManager.getInstance().getServerRateLimiter().throttle(messageBatch);
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
