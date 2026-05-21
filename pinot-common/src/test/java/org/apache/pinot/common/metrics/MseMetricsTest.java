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
package org.apache.pinot.common.metrics;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.metrics.fake.FakeMetricsFactory;
import org.apache.pinot.plugin.metrics.fake.FakeMetricsInspector;
import org.apache.pinot.plugin.metrics.fake.FakePinotMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Mode-aware behavior tests for {@link MseMetrics}. SERVER mode must forward to
 * {@link ServerMetrics} only; MSE mode emits {@code pinot.mse.*} only; DUAL hits both.
 */
public class MseMetricsTest {

  @BeforeClass
  public void initMetricsFactory() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    PinotMetricUtils.init(config);
  }

  @AfterClass
  public void cleanUpMetricsFactory() {
    PinotMetricUtils.cleanUp();
  }

  @AfterMethod
  public void deregisterSingletons() {
    MseMetrics.deregister();
    ServerMetrics.deregister();
  }

  @Test
  public void serverModeForwardsToServerMetricsOnly() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    FakeMetricsInspector serverInspector = new FakeMetricsInspector(serverMetrics.getMetricsRegistry());

    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.SERVER, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    mseMetrics.addMeteredGlobalValue(MseMeter.QUERIES, 7L);

    PinotMetricName serverName = serverInspector.lastMetric();
    assertNotNull(serverName, "SERVER mode should register a meter under ServerMetrics");
    assertEquals(serverInspector.getMeteredCount(serverName), 7L);
    assertTrue(mseRegistry.allMetrics().isEmpty(), "SERVER mode must not emit to the pinot.mse.* registry");
  }

  @Test
  public void mseModeEmitsToOwnRegistryOnly() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);

    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.MSE, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    mseMetrics.addMeteredGlobalValue(MseMeter.OPCHAINS_STARTED, 3L);

    PinotMetricName mseName = mseInspector.lastMetric();
    assertNotNull(mseName, "MSE mode should register a meter under MseMetrics");
    assertEquals(mseInspector.getMeteredCount(mseName), 3L);
    assertTrue(serverMetrics.getMetricsRegistry().allMetrics().isEmpty(),
        "MSE mode must not touch the ServerMetrics registry");
  }

  @Test
  public void dualModeEmitsToBothRegistries() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    FakeMetricsInspector serverInspector = new FakeMetricsInspector(serverMetrics.getMetricsRegistry());

    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.DUAL, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    mseMetrics.addMeteredGlobalValue(MseMeter.OPCHAINS_COMPLETED, 5L);

    assertEquals(mseInspector.getMeteredCount(mseInspector.lastMetric()), 5L);
    assertEquals(serverInspector.getMeteredCount(serverInspector.lastMetric()), 5L);
  }

  @Test
  public void dualModeReusedMeterMarksBothSides() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    FakeMetricsInspector serverInspector = new FakeMetricsInspector(serverMetrics.getMetricsRegistry());

    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.DUAL, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    // Mimic the MetricsExecutor caching pattern: resolve once, then mark() repeatedly.
    PinotMeter cached = mseMetrics.getMeteredValue(MseMeter.EMITTED_ROWS);
    cached.mark(11L);
    cached.mark(4L);

    assertEquals(mseInspector.getMeteredCount(mseInspector.lastMetric()), 15L);
    assertEquals(serverInspector.getMeteredCount(serverInspector.lastMetric()), 15L);
  }

  @Test
  public void addTimedValueForwardsByMode() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    FakeMetricsInspector serverInspector = new FakeMetricsInspector(serverMetrics.getMetricsRegistry());

    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.DUAL, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    mseMetrics.addTimedValue(MseTimer.SERIALIZATION_CPU_TIME_MS, 250, TimeUnit.MILLISECONDS);

    assertEquals(mseInspector.getTimerSumMs(mseInspector.lastMetric()), 250);
    assertEquals(serverInspector.getTimerSumMs(serverInspector.lastMetric()), 250);
  }

  @Test
  public void getMeteredValueReturnsServerHandleInServerMode() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.SERVER, new FakePinotMetricsRegistry());
    MseMetrics.register(mseMetrics);

    PinotMeter direct = serverMetrics.getMeteredValue(MseMeter.QUERIES.getServerMeter());
    PinotMeter throughShim = mseMetrics.getMeteredValue(MseMeter.QUERIES);
    assertSame(throughShim, direct, "SERVER mode should hand back the same ServerMetrics handle");
  }

  @Test
  public void noopFallbackIsServerMode() {
    // Default (no register call): must behave like the old direct-ServerMetrics call sites.
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    FakeMetricsInspector serverInspector = new FakeMetricsInspector(serverMetrics.getMetricsRegistry());

    MseMetrics.get().addMeteredGlobalValue(MseMeter.RUNNER_STARTED_TASKS, 1L);

    assertEquals(serverInspector.getMeteredCount(serverInspector.lastMetric()), 1L);
  }

  @Test
  public void registerFromConfigParsesModeFromConfig() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("pinot.metrics.mse.mode", "DUAL");

    MseMetrics.registerFromConfig(config, new FakePinotMetricsRegistry());

    assertEquals(MseMetrics.get().getMode(), MseMetricsMode.DUAL);
  }

  @Test
  public void registerFromConfigDefaultsToServerMode() {
    MseMetrics.registerFromConfig(new PinotConfiguration(), new FakePinotMetricsRegistry());

    assertEquals(MseMetrics.get().getMode(), MseMetricsMode.SERVER);
  }

  @Test
  public void registerFromConfigFallsBackOnInvalidValue() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("pinot.metrics.mse.mode", "NOT_A_MODE");

    MseMetrics.registerFromConfig(config, new FakePinotMetricsRegistry());

    assertEquals(MseMetrics.get().getMode(), MseMetricsMode.SERVER);
  }

  @Test
  public void serverModeWithoutRegisteredServerMetricsSilentlyDropsEmissions() {
    // ServerMetrics.register() deliberately not called — the resolved handle is the NOOP singleton.
    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.SERVER, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    // Must not throw — SERVER mode resolves through ServerMetrics.get(); documented behavior is
    // that emissions are silently dropped when the underlying singleton is the noop instance.
    mseMetrics.addMeteredGlobalValue(MseMeter.QUERIES, 1L);
    mseMetrics.addTimedValue(MseTimer.HASH_JOIN_BUILD_TABLE_CPU_TIME_MS, 5L, TimeUnit.MILLISECONDS);

    assertTrue(mseRegistry.allMetrics().isEmpty(),
        "SERVER mode must not register any pinot.mse.* meters even when forwarding is a noop");
  }

  @Test
  public void mseModeStillEmitsWhenServerMetricsIsNoop() {
    // ServerMetrics is noop; MSE mode emits to the local registry instead of forwarding, so
    // engine emissions surface even without a registered ServerMetrics.
    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.MSE, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    mseMetrics.addMeteredGlobalValue(MseMeter.OPCHAINS_STARTED, 2L);

    assertEquals(mseInspector.getMeteredCount(mseInspector.lastMetric()), 2L);
  }

  @Test
  public void dualModeWrapperIsCachedAcrossLookups() {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.DUAL, new FakePinotMetricsRegistry());
    assertTrue(MseMetrics.register(mseMetrics));

    PinotMeter first = mseMetrics.getMeteredValue(MseMeter.QUERIES);
    PinotMeter second = mseMetrics.getMeteredValue(MseMeter.QUERIES);
    PinotMeter other = mseMetrics.getMeteredValue(MseMeter.OPCHAINS_STARTED);

    assertSame(second, first, "DUAL mode must return the cached wrapper for repeated lookups");
    assertNotNull(other);
    assertTrue(first != other, "Distinct meters must get distinct wrappers");
  }

  @Test
  public void serverModeMeterWithoutCounterpartReturnsNoop()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.SERVER, new FakePinotMetricsRegistry());
    assertTrue(MseMetrics.register(mseMetrics));

    // Simulate an MSE-native meter (no ServerMeter counterpart added in a future release) by
    // nulling the field on an existing entry for the duration of the test.
    withNullServerCounterpart(MseMeter.QUERIES, () -> {
      PinotMeter handle = mseMetrics.getMeteredValue(MseMeter.QUERIES);
      assertNotNull(handle, "SERVER mode without a counterpart must still return a non-null handle");
      handle.mark(7);
      assertEquals(handle.count(), 0L, "Returned handle must be the noop meter — SERVER mode has no series to mark");
    });
  }

  @Test
  public void mseModeMeterWithoutCounterpartStillEmits()
      throws Exception {
    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.MSE, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    withNullServerCounterpart(MseMeter.OPCHAINS_STARTED, () -> {
      mseMetrics.addMeteredGlobalValue(MseMeter.OPCHAINS_STARTED, 9L);
    });

    assertEquals(mseInspector.getMeteredCount(mseInspector.lastMetric()), 9L);
  }

  @Test
  public void dualModeMeterWithoutCounterpartOnlyEmitsToMseSide()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    PinotMetricsRegistry mseRegistry = new FakePinotMetricsRegistry();
    FakeMetricsInspector mseInspector = new FakeMetricsInspector(mseRegistry);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.DUAL, mseRegistry);
    assertTrue(MseMetrics.register(mseMetrics));

    withNullServerCounterpart(MseMeter.EMITTED_ROWS, () -> {
      mseMetrics.addMeteredGlobalValue(MseMeter.EMITTED_ROWS, 4L);
    });

    assertEquals(mseInspector.getMeteredCount(mseInspector.lastMetric()), 4L);
    assertTrue(serverMetrics.getMetricsRegistry().allMetrics().isEmpty(),
        "DUAL mode must not register any pinot.server.* meter when there is no counterpart");
  }

  @Test
  public void serverModeTimerWithoutCounterpartIsDropped()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(new FakePinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    MseMetrics mseMetrics = new MseMetrics(MseMetricsMode.SERVER, new FakePinotMetricsRegistry());
    assertTrue(MseMetrics.register(mseMetrics));

    withNullServerTimerCounterpart(MseTimer.SERIALIZATION_CPU_TIME_MS, () -> {
      mseMetrics.addTimedValue(MseTimer.SERIALIZATION_CPU_TIME_MS, 250, TimeUnit.MILLISECONDS);
    });

    assertTrue(serverMetrics.getMetricsRegistry().allMetrics().isEmpty(),
        "SERVER mode without a timer counterpart must not register any pinot.server.* timer");
  }

  @Test
  public void registerIsCompareAndSetAgainstNoop() {
    MseMetrics first = new MseMetrics(MseMetricsMode.MSE, new FakePinotMetricsRegistry());
    assertTrue(MseMetrics.register(first));
    assertSame(MseMetrics.get(), first);

    MseMetrics second = new MseMetrics(MseMetricsMode.DUAL, new FakePinotMetricsRegistry());
    assertFalse(MseMetrics.register(second), "Second register must fail; CAS guards against accidental swap");
    assertSame(MseMetrics.get(), first);
  }

  /// Reflection helper: temporarily null out `MseMeter._serverMeter` to simulate an MSE-native
  /// entry, run the test body, then restore. Used to exercise the no-counterpart branches
  /// without adding a real null-counterpart entry to the public enum.
  private static void withNullServerCounterpart(MseMeter meter, Runnable body)
      throws Exception {
    Field field = MseMeter.class.getDeclaredField("_serverMeter");
    field.setAccessible(true);
    Object original = field.get(meter);
    field.set(meter, null);
    try {
      body.run();
    } finally {
      field.set(meter, original);
    }
  }

  private static void withNullServerTimerCounterpart(MseTimer timer, Runnable body)
      throws Exception {
    Field field = MseTimer.class.getDeclaredField("_serverTimer");
    field.setAccessible(true);
    Object original = field.get(timer);
    field.set(timer, null);
    try {
      body.run();
    } finally {
      field.set(timer, original);
    }
  }
}
