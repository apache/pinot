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
import org.apache.pinot.spi.utils.CommonConstants;
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

  private static String getFullyQualifiedConfigName(String config) {
    return CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + config;
  }

  @BeforeClass
  public void setUp() {
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY),
        Boolean.toString(EXPECTED_OOM_KILL_QUERY_ENABLED));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE),
        Boolean.toString(EXPECTED_PUBLISH_HEAP_USAGE_METRIC));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED),
        Boolean.toString(EXPECTED_IS_CPU_TIME_BASED_KILLING_ENABLED));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS),
        Long.toString(EXPECTED_CPU_TIME_BASED_KILLING_THRESHOLD_NS));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO),
        Double.toString(EXPECTED_PANIC_LEVEL));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO),
        Double.toString(EXPECTED_CRITICAL_LEVEL));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO),
        Double.toString(EXPECTED_ALARMING_LEVEL));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_SLEEP_TIME_MS),
        Integer.toString(EXPECTED_NORMAL_SLEEP_TIME));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR),
        Integer.toString(EXPECTED_ALARMING_SLEEP_TIME_DENOMINATOR));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO),
        Double.toString(EXPECTED_MIN_MEMORY_FOOTPRINT_FOR_KILL));
    CLUSTER_CONFIGS.put(getFullyQualifiedConfigName(Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED),
        Boolean.toString(EXPECTED_IS_QUERY_KILLED_METRIC_ENABLED));
  }

  @Test
  void testOOMProtectionKillingQueryConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY)),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isOomKillQueryEnabled());
  }

  @Test
  void testPublishHeapUsageMetricConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isPublishHeapUsageMetric());
    accountant.getWatcherTask()
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE)), CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isPublishHeapUsageMetric());
  }

  @Test
  void testCPUTimeBasedKillingEnabledConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertFalse(accountant.getQueryMonitorConfig().isCpuTimeBasedKillingEnabled());
    accountant.getWatcherTask()
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_SLEEP_TIME_MS)), CLUSTER_CONFIGS);
    assertEquals(accountant.getQueryMonitorConfig().getNormalSleepTime(), EXPECTED_NORMAL_SLEEP_TIME);
  }

  @Test
  void testSleepTimeDenominatorConfigChange() {
    PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test", InstanceType.SERVER);

    assertEquals(accountant.getQueryMonitorConfig().getAlarmingSleepTime(),
        accountant.getQueryMonitorConfig().getNormalSleepTime() / Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);
    accountant.getWatcherTask()
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR)), CLUSTER_CONFIGS);
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO)),
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
        .onChange(Set.of(getFullyQualifiedConfigName(Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED)),
            CLUSTER_CONFIGS);
    assertTrue(accountant.getQueryMonitorConfig().isQueryKilledMetricEnabled());
  }
}
