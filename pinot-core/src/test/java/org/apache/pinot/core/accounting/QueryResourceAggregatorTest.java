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
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryResourceAggregatorTest {

  // Use 0 for critical ratio to guarantee heap always exceeds the threshold in tests, regardless of JVM
  // configuration. Use a very high panic ratio (1.1) to prevent hitting panic level.
  private static final float TEST_CRITICAL_RATIO = 0f;
  private static final float TEST_PANIC_RATIO = 1.1f;

  private QueryResourceAggregator createAggregator(boolean oomPauseEnabled, long oomPauseTimeoutMs) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PAUSE_ENABLED, oomPauseEnabled);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PAUSE_TIMEOUT_MS, oomPauseTimeoutMs);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, TEST_CRITICAL_RATIO);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO, TEST_PANIC_RATIO);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);

    long maxHeapSize = org.apache.pinot.spi.utils.ResourceUsageUtils.getMaxHeapSize();
    AtomicReference<QueryMonitorConfig> configRef =
        new AtomicReference<>(new QueryMonitorConfig(config, maxHeapSize));

    return new QueryResourceAggregator("test-instance", InstanceType.SERVER, false, true, configRef);
  }

  @Test
  public void testConfigReadsOomPauseEnabled() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PAUSE_ENABLED, true);
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PAUSE_TIMEOUT_MS, 2000L);

    long maxHeapSize = org.apache.pinot.spi.utils.ResourceUsageUtils.getMaxHeapSize();
    QueryMonitorConfig qmc = new QueryMonitorConfig(config, maxHeapSize);

    assertTrue(qmc.isOomPauseEnabled(), "OOM pause should be enabled");
    assertEquals(qmc.getOomPauseTimeoutMs(), 2000L, "OOM pause timeout should be 2000ms");
  }

  @Test
  public void testPauseActivatedOnTransitionToCritical() {
    QueryResourceAggregator aggregator = createAggregator(true, 5000);

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
    QueryResourceAggregator aggregator = createAggregator(true, 1000);
    long start = System.nanoTime();
    aggregator.waitIfPaused();
    long elapsed = System.nanoTime() - start;
    assertTrue(elapsed < 1_000_000, "Fast path should complete in under 1ms, took " + elapsed + "ns");
  }

  @Test
  public void testWaitIfPausedBlocksForTimeout() {
    QueryResourceAggregator aggregator = createAggregator(true, 300);

    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();
    assertTrue(aggregator.isPauseActive(), "Pause should be active");

    long start = System.currentTimeMillis();
    aggregator.waitIfPaused();
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed >= 200,
        "waitIfPaused should have blocked for ~300ms, but only blocked for " + elapsed + "ms");
  }

  @Test
  public void testNoPauseWhenDisabled() {
    QueryResourceAggregator aggregator = createAggregator(false, 1000);

    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();

    assertFalse(aggregator.isPauseActive(), "Pause should not be active when feature is disabled");
  }

  @Test
  public void testNoPauseWhenAlreadyAtCritical() {
    QueryResourceAggregator aggregator = createAggregator(true, 200);

    // First cycle: transitions Normal -> Critical, activates pause
    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();
    assertTrue(aggregator.isPauseActive(), "Pause should be active after first cycle");

    // Wait out the pause
    aggregator.waitIfPaused();

    // Second cycle: stays at Critical -> Critical, should NOT re-pause
    aggregator.preAggregate(Collections.emptyList());
    aggregator.postAggregate();

    assertFalse(aggregator.isPauseActive(),
        "Should not re-pause when already at critical level");
  }
}
