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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryMonitorConfigTest {
  private static final double EXPECTED_MIN_MEMORY_FOOTPRINT_FOR_KILL = 0.05;
  private static final double EXPECTED_PANIC_LEVEL = 0.9f;
  private static final double EXPECTED_CRITICAL_LEVEL = 0.95f;
  private static final double EXPECTED_ALARMING_LEVEL = 0.8f;
  private static final int EXPECTED_NORMAL_SLEEP_TIME = 50;
  private static final int EXPECTED_ALARMING_SLEEP_TIME_DENOMINATOR = 2;
  private static final boolean EXPECTED_OOM_KILL_QUERY_ENABLED = true;
  private static final boolean EXPECTED_PUBLISH_HEAP_USAGE_METRIC = true;
  private static final boolean EXPECTED_IS_CPU_TIME_BASED_KILLING_ENABLED = true;
  private static final long EXPECTED_CPU_TIME_BASED_KILLING_THRESHOLD_NS = 1000;
  private static final boolean EXPECTED_IS_QUERY_KILLED_METRIC_ENABLED = true;
  private static final Map<String, String> CLUSTER_CONFIGS = new HashMap<>();

  private static final long EXPECTED_OOM_PAUSE_TIMEOUT_MS = 2000;
  private static final boolean EXPECTED_OOM_PAUSE_ON_PANIC_ENABLED = true;

  @BeforeClass
  public void setUp() {
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY,
        Boolean.toString(EXPECTED_OOM_KILL_QUERY_ENABLED));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.PUBLISHING_JVM_USAGE,
        Boolean.toString(EXPECTED_PUBLISH_HEAP_USAGE_METRIC));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED,
        Boolean.toString(EXPECTED_IS_CPU_TIME_BASED_KILLING_ENABLED));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS,
        Long.toString(EXPECTED_CPU_TIME_BASED_KILLING_THRESHOLD_NS));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO,
        Double.toString(EXPECTED_PANIC_LEVEL));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO,
        Double.toString(EXPECTED_CRITICAL_LEVEL));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO,
        Double.toString(EXPECTED_ALARMING_LEVEL));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.SLEEP_TIME_MS,
        Integer.toString(EXPECTED_NORMAL_SLEEP_TIME));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.SLEEP_TIME_DENOMINATOR,
        Integer.toString(EXPECTED_ALARMING_SLEEP_TIME_DENOMINATOR));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO,
        Double.toString(EXPECTED_MIN_MEMORY_FOOTPRINT_FOR_KILL));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.QUERY_KILLED_METRIC_ENABLED,
        Boolean.toString(EXPECTED_IS_QUERY_KILLED_METRIC_ENABLED));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS,
        Long.toString(EXPECTED_OOM_PAUSE_TIMEOUT_MS));
    CLUSTER_CONFIGS.put(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE,
        Boolean.toString(EXPECTED_OOM_PAUSE_ON_PANIC_ENABLED));
  }

  @Test
  void testOOMProtectionKillingQueryConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  @Test
  void testPublishHeapUsageMetricConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isPublishHeapUsageMetric());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.PUBLISHING_JVM_USAGE), CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isPublishHeapUsageMetric());
  }

  @Test
  void testCPUTimeBasedKillingEnabledConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isCpuTimeBasedKillingEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isCpuTimeBasedKillingEnabled());
  }

  @Test
  void testCPUTimeBasedKillingThresholdConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getCpuTimeBasedKillingThresholdNs(),
        Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS * 1000_000L);
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getCpuTimeBasedKillingThresholdNs(),
        EXPECTED_CPU_TIME_BASED_KILLING_THRESHOLD_NS * 1000_000L);
  }

  @Test
  void testPanicLevelHeapUsageRatioConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getPanicLevel(),
        Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO * accountant.getQueryMonitorConfig().getMaxHeapSize());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getPanicLevel(),
        EXPECTED_PANIC_LEVEL * accountant.getQueryMonitorConfig().getMaxHeapSize());
  }

  @Test
  void testCriticalLevelHeapUsageRatioConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getCriticalLevel(),
        Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO * accountant.getQueryMonitorConfig().getMaxHeapSize());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getCriticalLevel(),
        EXPECTED_CRITICAL_LEVEL * accountant.getQueryMonitorConfig().getMaxHeapSize());
  }

  @Test
  void testAlarmingLevelHeapUsageRatioConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getAlarmingLevel(),
        Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO * accountant.getQueryMonitorConfig().getMaxHeapSize());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getAlarmingLevel(),
        EXPECTED_ALARMING_LEVEL * accountant.getQueryMonitorConfig().getMaxHeapSize());
  }

  @Test
  void testSleepTimeConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getNormalSleepTime(), Accounting.DEFAULT_SLEEP_TIME_MS);
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.SLEEP_TIME_MS), CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getNormalSleepTime(), EXPECTED_NORMAL_SLEEP_TIME);
  }

  @Test
  void testSleepTimeDenominatorConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getAlarmingSleepTime(),
        accountant.getQueryMonitorConfig().getNormalSleepTime() / Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.SLEEP_TIME_DENOMINATOR), CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getAlarmingSleepTime(),
        accountant.getQueryMonitorConfig().getNormalSleepTime() / EXPECTED_ALARMING_SLEEP_TIME_DENOMINATOR);
  }

  @Test
  void testMinMemoryFootprintToKillRatioConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getMinMemoryFootprintForKill(),
        (long) (Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO * accountant.getQueryMonitorConfig()
            .getMaxHeapSize()));
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getMinMemoryFootprintForKill(),
        (long) (EXPECTED_MIN_MEMORY_FOOTPRINT_FOR_KILL * accountant.getQueryMonitorConfig().getMaxHeapSize()));
  }

  @Test
  void testQueryKilledMetricEnabledConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isQueryKilledMetricEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.QUERY_KILLED_METRIC_ENABLED),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isQueryKilledMetricEnabled());
  }

  @Test
  void testOomPauseTimeoutMsConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getOomPreQueryKillPauseDurationMs(),
        Accounting.DEFAULT_OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS);
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS),
            CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getOomPreQueryKillPauseDurationMs(), EXPECTED_OOM_PAUSE_TIMEOUT_MS);
  }

  @Test
  void testOomPauseOnPanicEnabledConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isOomPanicPreQueryKillPauseEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isOomPanicPreQueryKillPauseEnabled());
  }

  /// Verifies the role-specific prefix takes precedence over {@link Accounting#COMMON_PREFIX} on a server accountant,
  /// and that the broker-specific prefix is ignored.
  @Test
  void testServerPrefixOverridesCommonAndIgnoresBrokerPrefix() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String brokerKey = Accounting.BROKER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    // Common says false, broker says false, server says true. The server accountant should pick the server value.
    Map<String, String> clusterConfigs = Map.of(commonKey, "false", brokerKey, "false", serverKey, "true");
    accountant.getWatcherTask().onChange(Set.of(commonKey, brokerKey, serverKey), clusterConfigs);
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Server-specific prefix value should override the common prefix value");
  }

  /// Same as above but asserts the broker accountant picks up {@link Accounting#BROKER_PREFIX} and ignores
  /// {@link Accounting#SERVER_PREFIX}.
  @Test
  void testBrokerPrefixOverridesCommonAndIgnoresServerPrefix() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.BROKER);
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String brokerKey = Accounting.BROKER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    Map<String, String> clusterConfigs = Map.of(commonKey, "false", brokerKey, "true", serverKey, "false");
    accountant.getWatcherTask().onChange(Set.of(commonKey, brokerKey, serverKey), clusterConfigs);
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Broker-specific prefix value should override the common prefix value");
  }

  /// Common config added to cluster: the accountant should pick up the new value.
  @Test
  void testCommonConfigAdded() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  /// Common config value changed: the accountant should reflect the new value.
  @Test
  void testCommonConfigChanged() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(commonKey, "false"));
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  /// Common config removed from cluster: key is in changedConfigs but absent from clusterConfigs; the accountant should
  /// revert to the default value.
  @Test
  void testCommonConfigRemovedRevertsToDefault() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of());
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Removed common config should revert to the default value");
  }

  /// Role-specific config added on top of an existing common value: role value wins.
  @Test
  void testServerConfigAddedOverridesCommon() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(commonKey, "true", serverKey, "false"));
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Added server-specific value should override the common value");
  }

  /// Role-specific config value changed.
  @Test
  void testServerConfigChanged() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(serverKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(serverKey, "false"));
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  /// Role-specific config removed while the common value is still present: the accountant falls back to the common
  /// value.
  @Test
  void testServerConfigRemovedFallsBackToCommon() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    accountant.getWatcherTask().onChange(Set.of(commonKey, serverKey), Map.of(commonKey, "true", serverKey, "false"));
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    // Remove server-specific: changedConfigs has serverKey but clusterConfigs only has commonKey.
    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Removed server-specific config should fall back to the common value");
  }

  /// Changing the common config does not override the role-specific value while the role-specific value is set.
  @Test
  void testChangingCommonDoesNotOverrideServer() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String commonKey = Accounting.COMMON_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    // Start with server=true -> enabled=true.
    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(serverKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    // Add conflicting common=false. Server still wins; common value is ignored.
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(serverKey, "true", commonKey, "false"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Adding a conflicting common value should not override the role-specific value");

    // Flip common back and forth; server value should continue to dominate.
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(serverKey, "true", commonKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
    accountant.getWatcherTask().onChange(Set.of(commonKey), Map.of(serverKey, "true", commonKey, "false"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Subsequent common changes should not override the role-specific value");
  }

  /// Role-specific config removed with no common value set: the accountant reverts to the default.
  @Test
  void testServerConfigRemovedRevertsToDefault() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);
    String serverKey = Accounting.SERVER_PREFIX + "." + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY;

    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of(serverKey, "true"));
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());

    accountant.getWatcherTask().onChange(Set.of(serverKey), Map.of());
    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled(),
        "Removed server-specific config without a common fallback should revert to the default");
  }
}
