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
package org.apache.pinot.core.accounting;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryResourceAggregatorTest {

  // Use 0 for critical ratio to guarantee heap always exceeds the threshold in tests, regardless of JVM
  // configuration. Use a very high panic ratio (1.1) to prevent hitting panic level.
  private static final float TEST_CRITICAL_RATIO = 0f;
  private static final float TEST_PANIC_RATIO = 1.1f;

  private QueryResourceAggregator createAggregator(long oomPauseTimeoutMs) {
    return createAggregator(oomPauseTimeoutMs, false);
  }

  /// Creates an aggregator that always hits critical level. When {@code pauseOnPanic} is true, the panic ratio is
  /// also set to 0 so that the aggregator always hits panic level instead.
  private QueryResourceAggregator createAggregator(long oomPauseTimeoutMs, boolean pauseOnPanic) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS, oomPauseTimeoutMs);
    config.setProperty(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, TEST_CRITICAL_RATIO);
    config.setProperty(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, pauseOnPanic ? 0f : TEST_PANIC_RATIO);
    config.setProperty(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, true);
    config.setProperty(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    config.setProperty(Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE, pauseOnPanic);

    long maxHeapSize = org.apache.pinot.spi.utils.ResourceUsageUtils.getMaxHeapSize();
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(new QueryMonitorConfig(config, maxHeapSize));

    return new QueryResourceAggregator("test-instance", InstanceType.SERVER, false, true, configRef);
  }

  @Test
  public void testConfigReadsOomPauseTimeout() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS, 2000L);

    long maxHeapSize = org.apache.pinot.spi.utils.ResourceUsageUtils.getMaxHeapSize();
    QueryMonitorConfig qmc = new QueryMonitorConfig(config, maxHeapSize);

    assertTrue(qmc.isOomPreQueryKillPauseEnabled(), "OOM pause should be enabled when timeout > 0");
    assertEquals(qmc.getOomPreQueryKillPauseDurationMs(), 2000L, "OOM pause timeout should be 2000ms");

    PinotConfiguration disabledConfig = new PinotConfiguration();
    QueryMonitorConfig disabledQmc = new QueryMonitorConfig(disabledConfig, maxHeapSize);
    assertFalse(disabledQmc.isOomPreQueryKillPauseEnabled(), "OOM pause should be disabled by default");
  }

  @Test
  public void testPauseActivatedOnTransitionToCritical() {
    QueryResourceAggregator aggregator = createAggregator(5000);

    assertFalse(aggregator.isPauseActive(), "Pause should not be active initially");

    aggregator.preAggregate(Collections.emptyList());
    assertEquals(aggregator.getTriggeringLevel(), QueryResourceAggregator.TriggeringLevel.HeapMemoryCritical,
        "Triggering level should be HeapMemoryCritical");
    assertEquals(aggregator.getPreviousTriggeringLevel(), QueryResourceAggregator.TriggeringLevel.Normal,
        "Previous triggering level should be Normal");

    aggregator.postAggregate();
    assertTrue(aggregator.isPauseActive(),
        "Pause should be active after transitioning to critical with OOM pause enabled");
  }

  @Test
  public void testWaitIfPausedFastPathWhenNotActive() {
    QueryResourceAggregator aggregator = createAggregator(1000);
    long start = System.nanoTime();
    aggregator.waitIfPaused();
    long elapsed = System.nanoTime() - start;
    assertTrue(elapsed < 1_000_000, "Fast path should complete in under 1ms, took " + elapsed + "ns");
  }

  @Test
  public void testWaitIfPausedBlocksUntilSignaled()
      throws Exception {
    QueryResourceAggregator aggregator = createAggregator(5000);

    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();
    assertTrue(aggregator.isPauseActive(), "Pause should be active");

    // Clear the pause from a background thread after 200ms
    Thread signaler = new Thread(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      aggregator.clearPause();
    });
    signaler.start();

    long start = System.currentTimeMillis();
    aggregator.waitIfPaused();
    long elapsed = System.currentTimeMillis() - start;
    signaler.join(1000);

    assertTrue(elapsed >= 150,
        "waitIfPaused should have blocked until signaled (~200ms), but only blocked for " + elapsed + "ms");
    assertTrue(elapsed < 2000, "waitIfPaused should have unblocked promptly after signal, but took " + elapsed + "ms");
  }

  @Test
  public void testNoPauseWhenDisabled() {
    QueryResourceAggregator aggregator = createAggregator(-1);

    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();

    assertFalse(aggregator.isPauseActive(), "Pause should not be active when feature is disabled");
  }

  @Test
  public void testNoPauseWhenAlreadyAtCritical() {
    QueryResourceAggregator aggregator = createAggregator(200);

    // First cycle: transitions Normal -> Critical, activates pause
    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();
    assertTrue(aggregator.isPauseActive(), "Pause should be active after first cycle");

    // Explicitly clear the pause (simulating the watcher clearing after timeout)
    aggregator.clearPause();

    // Second cycle: stays at Critical -> Critical, should NOT re-pause
    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();

    assertFalse(aggregator.isPauseActive(), "Should not re-pause when already at critical level");
  }

  @Test
  public void testPauseActivatedOnFreshJumpToPanic() {
    QueryResourceAggregator aggregator = createAggregator(5000, true);

    assertFalse(aggregator.isPauseActive(), "Pause should not be active initially");

    aggregator.preAggregate(Collections.emptyList());
    assertEquals(aggregator.getTriggeringLevel(), QueryResourceAggregator.TriggeringLevel.HeapMemoryPanic,
        "Triggering level should be HeapMemoryPanic");
    assertEquals(aggregator.getPreviousTriggeringLevel(), QueryResourceAggregator.TriggeringLevel.Normal,
        "Previous triggering level should be Normal");

    assertTrue(aggregator.isPauseActive(),
        "Pause should be active after fresh jump to panic with pause-on-panic enabled");
  }

  @Test
  public void testNoPanicPauseWhenDisabled() {
    // Create aggregator that hits panic but with pauseOnPanic=false
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS, 5000L);
    config.setProperty(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0f);
    config.setProperty(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, 0f);
    config.setProperty(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, true);
    config.setProperty(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    config.setProperty(Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE, false);

    long maxHeapSize = org.apache.pinot.spi.utils.ResourceUsageUtils.getMaxHeapSize();
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(new QueryMonitorConfig(config, maxHeapSize));
    QueryResourceAggregator aggregator =
        new QueryResourceAggregator("test-instance", InstanceType.SERVER, false, true, configRef);

    aggregator.preAggregate(Collections.emptyList());

    assertFalse(aggregator.isPauseActive(),
        "Pause should not be active at panic level when pause-on-panic is disabled");
  }

  @Test
  public void testNoPanicRePauseAfterFirstPause() {
    QueryResourceAggregator aggregator = createAggregator(5000, true);

    // First cycle: Normal → Panic, activates pause
    aggregator.preAggregate(Collections.emptyList());
    assertTrue(aggregator.isPauseActive(), "Pause should be active from first panic transition");

    // Simulate the watcher clearing the pause after timeout
    aggregator.clearPause();

    // Second cycle: Panic → Panic, should NOT re-pause
    aggregator.preAggregate(Collections.emptyList());
    assertEquals(aggregator.getPreviousTriggeringLevel(), QueryResourceAggregator.TriggeringLevel.HeapMemoryPanic);
    assertFalse(aggregator.isPauseActive(), "Should not re-pause on Panic→Panic transition");
  }
}
