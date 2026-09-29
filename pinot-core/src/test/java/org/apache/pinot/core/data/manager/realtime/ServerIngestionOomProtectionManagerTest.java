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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ServerIngestionOomProtectionManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @Test
  public void testDefaultDisableModeDoesNotProtectTables() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    TableConfig tableConfig = buildTableConfig(null);

    ServerIngestionOomProtectionManager statelessRealtimeManager =
        buildManager(Map.of(), tableConfig, false, usedHeapBytes, maxHeapBytes, nowMs);
    assertFalse(statelessRealtimeManager.shouldThrottle());

    ServerIngestionOomProtectionManager upsertOrDedupManager =
        buildManager(Map.of(), tableConfig, true, usedHeapBytes, maxHeapBytes, nowMs);
    assertFalse(upsertOrDedupManager.shouldThrottle());
  }

  @Test
  public void testUpsertDedupOnlyModeAppliesOnlyToUpsertOrDedupTables() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    TableConfig tableConfig = buildTableConfig(null);

    ServerIngestionOomProtectionManager statelessRealtimeManager =
        buildManager(upsertDedupOnlyServerConfig(), tableConfig, false, usedHeapBytes, maxHeapBytes,
            nowMs);
    assertFalse(statelessRealtimeManager.shouldThrottle());

    ServerIngestionOomProtectionManager upsertOrDedupManager =
        buildManager(upsertDedupOnlyServerConfig(), tableConfig, true, usedHeapBytes, maxHeapBytes,
            nowMs);
    assertTrue(upsertOrDedupManager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(91);
    assertTrue(upsertOrDedupManager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(90);
    assertFalse(upsertOrDedupManager.shouldThrottle());
  }

  @Test
  public void testTableDisableOverridesServerPolicy() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();

    ServerIngestionOomProtectionManager manager =
        buildManager(upsertDedupOnlyServerConfig(), buildTableConfig(Enablement.DISABLE), true, usedHeapBytes,
            maxHeapBytes, nowMs);

    assertFalse(manager.shouldThrottle());
  }

  @Test
  public void testDisabledTableDoesNotUpdateGauges() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);

    ServerIngestionOomProtectionManager manager =
        buildManager(upsertDedupOnlyServerConfig(), buildTableConfig(Enablement.DISABLE), true, usedHeapBytes,
            maxHeapBytes, nowMs, serverMetrics);

    assertFalse(manager.shouldThrottle());
    verifyNoInteractions(serverMetrics);
  }

  @Test
  public void testSharedServerThrottleStateSamplesHeapOnceAcrossManagersWithinInterval() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger usedHeapReads = new AtomicInteger();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(upsertDedupOnlyServerConfig(), () -> {
          usedHeapReads.incrementAndGet();
          return usedHeapBytes.get();
        }, maxHeapBytes::get, nowMs, serverMetrics);
    ServerIngestionOomProtectionManager manager1 =
        buildManager(buildTableConfig(null), true, serverThrottleState);
    ServerIngestionOomProtectionManager manager2 =
        buildManager(buildTableConfig(null), true, serverThrottleState);

    assertTrue(manager1.shouldThrottle());
    assertTrue(manager2.shouldThrottle());
    assertEquals(usedHeapReads.get(), 1);
  }

  @Test
  public void testTableEnableUsesServerThresholds() {
    AtomicLong usedHeapBytes = new AtomicLong(90);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();

    ServerIngestionOomProtectionManager manager =
        buildManager(Map.of(), buildTableConfig(Enablement.ENABLE), false, usedHeapBytes,
            maxHeapBytes, nowMs);

    assertFalse(manager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(96);
    assertTrue(manager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(91);
    assertTrue(manager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(90);
    assertFalse(manager.shouldThrottle());
  }

  @Test
  public void testGcRequestedWhenThrottlingAndRateLimited() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger gcRequests = new AtomicInteger();
    ServerIngestionOomProtectionManager manager =
        buildManager(upsertDedupOnlyServerConfig(), buildTableConfig(null), true, usedHeapBytes,
            maxHeapBytes, nowMs, mock(ServerMetrics.class), () -> gcRequests.incrementAndGet());

    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 1);

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 1);

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_GC_INTERVAL_MS);
    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 2);
  }

  @Test
  public void testGcRequestCanBeDisabled() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger gcRequests = new AtomicInteger();
    Map<String, Object> serverConfig =
        upsertDedupOnlyServerConfig(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_GC_INTERVAL_MS,
            0L);
    ServerIngestionOomProtectionManager manager =
        buildManager(serverConfig, buildTableConfig(null), true, usedHeapBytes, maxHeapBytes, nowMs,
            mock(ServerMetrics.class), () -> gcRequests.incrementAndGet());

    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 0);
  }

  @Test
  public void testGcRequestTimerResetsWhenThrottlingReleases() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger gcRequests = new AtomicInteger();
    ServerIngestionOomProtectionManager manager =
        buildManager(upsertDedupOnlyServerConfig(), buildTableConfig(null), true, usedHeapBytes,
            maxHeapBytes, nowMs, mock(ServerMetrics.class), () -> gcRequests.incrementAndGet());

    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 1);

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(90);
    assertFalse(manager.shouldThrottle());

    nowMs.addAndGet(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
    usedHeapBytes.set(96);
    assertTrue(manager.shouldThrottle());
    assertEquals(gcRequests.get(), 2);
  }

  @Test
  public void testGcRequestIsRateLimitedAcrossManagers() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger gcRequests = new AtomicInteger();
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(upsertDedupOnlyServerConfig(), usedHeapBytes::get, maxHeapBytes::get, nowMs,
            mock(ServerMetrics.class), () -> gcRequests.incrementAndGet());
    ServerIngestionOomProtectionManager manager1 =
        buildManager(buildTableConfig(null), true, serverThrottleState);
    ServerIngestionOomProtectionManager manager2 =
        buildManager(buildTableConfig(null), true, serverThrottleState);

    assertTrue(manager1.shouldThrottle());
    assertTrue(manager2.shouldThrottle());
    assertEquals(gcRequests.get(), 1);
  }

  @Test
  public void testServerEnableModeProtectsAllRealtimeTables() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    Map<String, Object> serverConfig =
        serverModeConfig("ENABLE");

    ServerIngestionOomProtectionManager manager =
        buildManager(serverConfig, buildTableConfig(null), false, usedHeapBytes, maxHeapBytes,
            nowMs);

    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testClusterConfigCanDynamicallyUpdateServerMode() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(Map.of(), usedHeapBytes::get, maxHeapBytes::get, nowMs, serverMetrics);
    ServerIngestionOomProtectionManager manager =
        buildManager(buildTableConfig(null), true, serverThrottleState);

    assertFalse(manager.shouldThrottle());

    String modeConfigKey =
        fullServerInstanceConfigKey(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE);
    serverThrottleState.onChange(Set.of(modeConfigKey), Map.of(modeConfigKey, "UPSERT_DEDUP_ONLY"));

    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testClusterConfigCanDynamicallyUpdateServerThresholds() {
    AtomicLong usedHeapBytes = new AtomicLong(92);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(upsertDedupOnlyServerConfig(), usedHeapBytes::get, maxHeapBytes::get, nowMs,
            serverMetrics);
    ServerIngestionOomProtectionManager manager =
        buildManager(buildTableConfig(null), true, serverThrottleState);

    assertFalse(manager.shouldThrottle());

    String throttleThresholdConfigKey = fullServerInstanceConfigKey(
        CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_THROTTLE_THRESHOLD);
    String recoveryThresholdConfigKey = fullServerInstanceConfigKey(
        CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_RECOVERY_THRESHOLD);
    serverThrottleState.onChange(Set.of(throttleThresholdConfigKey, recoveryThresholdConfigKey),
        Map.of(throttleThresholdConfigKey, "0.90", recoveryThresholdConfigKey, "0.85"));

    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testInvalidClusterConfigUpdateKeepsPreviousConfig() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(upsertDedupOnlyServerConfig(), usedHeapBytes::get, maxHeapBytes::get, nowMs,
            mock(ServerMetrics.class));
    ServerIngestionOomProtectionManager manager =
        buildManager(buildTableConfig(null), true, serverThrottleState);

    String thresholdConfigKey = fullServerInstanceConfigKey(
        CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_THROTTLE_THRESHOLD);
    serverThrottleState.onChange(Set.of(thresholdConfigKey), Map.of(thresholdConfigKey, "not-a-double"));

    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testClusterConfigDeletionFallsBackToInstanceConfig() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(serverModeConfig("ENABLE"), usedHeapBytes::get, maxHeapBytes::get, nowMs,
            serverMetrics);
    ServerIngestionOomProtectionManager manager =
        buildManager(buildTableConfig(null), false, serverThrottleState);

    String modeConfigKey =
        fullServerInstanceConfigKey(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE);
    serverThrottleState.onChange(Set.of(modeConfigKey), Map.of(modeConfigKey, "DISABLE"));
    assertFalse(manager.shouldThrottle());

    serverThrottleState.onChange(Set.of(modeConfigKey), Map.of());
    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testClusterConfigModeChangeClearsActiveThrottle() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState =
        buildServerThrottleState(serverModeConfig("ENABLE"), usedHeapBytes::get, maxHeapBytes::get, nowMs,
            serverMetrics);
    ServerIngestionOomProtectionManager manager =
        buildManager(buildTableConfig(null), false, serverThrottleState);

    assertTrue(manager.shouldThrottle());

    String modeConfigKey =
        fullServerInstanceConfigKey(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE);
    serverThrottleState.onChange(Set.of(modeConfigKey), Map.of(modeConfigKey, "DISABLE"));

    assertFalse(manager.isThrottling());
    verify(serverMetrics).setValueOfGlobalGauge(ServerGauge.REALTIME_INGESTION_OOM_PROTECTION_ACTIVE, 0L);
  }

  @Test
  public void testTableEnableOverridesServerDisableMode() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();

    ServerIngestionOomProtectionManager manager =
        buildManager(Map.of(), buildTableConfig(Enablement.ENABLE), true, usedHeapBytes,
            maxHeapBytes, nowMs);

    assertTrue(manager.shouldThrottle());
  }

  @Test
  public void testResetMetricsClearsProtectionGauges() {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerIngestionOomProtectionManager manager =
        buildManager(upsertDedupOnlyServerConfig(), buildTableConfig(null), true, usedHeapBytes,
            maxHeapBytes, nowMs, serverMetrics);

    assertTrue(manager.shouldThrottle());
    manager.resetMetrics();

    verify(serverMetrics).setValueOfGlobalGauge(ServerGauge.REALTIME_INGESTION_OOM_PROTECTION_ACTIVE, 0L);
  }

  @Test
  public void testWaitIfProtectionNeededStopsWhenStopConditionIsMet()
      throws Exception {
    AtomicLong usedHeapBytes = new AtomicLong(96);
    AtomicLong maxHeapBytes = new AtomicLong(100);
    AtomicLong nowMs = new AtomicLong();
    AtomicInteger stopChecks = new AtomicInteger();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    Map<String, Object> serverConfig =
        upsertDedupOnlyServerConfig(
            CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS, 1L);
    ServerIngestionOomProtectionManager manager =
        buildManager(serverConfig, buildTableConfig(null), true, usedHeapBytes, maxHeapBytes, nowMs,
            serverMetrics);

    assertTrue(manager.waitIfProtectionNeeded(() -> stopChecks.getAndIncrement() > 0));
  }

  private static ServerIngestionOomProtectionManager buildManager(Map<String, Object> serverConfig,
      TableConfig tableConfig, boolean upsertOrDedupEnabled, AtomicLong usedHeapBytes, AtomicLong maxHeapBytes,
      AtomicLong nowMs) {
    return buildManager(serverConfig, tableConfig, upsertOrDedupEnabled, usedHeapBytes, maxHeapBytes, nowMs,
        mock(ServerMetrics.class));
  }

  private static ServerIngestionOomProtectionManager buildManager(Map<String, Object> serverConfig,
      TableConfig tableConfig, boolean upsertOrDedupEnabled, AtomicLong usedHeapBytes, AtomicLong maxHeapBytes,
      AtomicLong nowMs, ServerMetrics serverMetrics) {
    return buildManager(serverConfig, tableConfig, upsertOrDedupEnabled, usedHeapBytes, maxHeapBytes, nowMs,
        serverMetrics, ServerIngestionOomProtectionManagerTest::noOpGc);
  }

  private static ServerIngestionOomProtectionManager buildManager(Map<String, Object> serverConfig,
      TableConfig tableConfig, boolean upsertOrDedupEnabled, AtomicLong usedHeapBytes, AtomicLong maxHeapBytes,
      AtomicLong nowMs, ServerMetrics serverMetrics, Runnable gcRunner) {
    return new ServerIngestionOomProtectionManager(() -> tableConfig, () -> upsertOrDedupEnabled,
        new ServerIngestionOomProtectionManager.ServerThrottleState(new PinotConfiguration(serverConfig), serverMetrics,
            usedHeapBytes::get, maxHeapBytes::get, nowMs::get, gcRunner));
  }

  private static ServerIngestionOomProtectionManager buildManager(TableConfig tableConfig,
      boolean upsertOrDedupEnabled, ServerIngestionOomProtectionManager.ServerThrottleState serverThrottleState) {
    return new ServerIngestionOomProtectionManager(() -> tableConfig, () -> upsertOrDedupEnabled, serverThrottleState);
  }

  private static ServerIngestionOomProtectionManager.ServerThrottleState buildServerThrottleState(
      Map<String, Object> serverConfig, LongSupplier usedHeapSupplier, LongSupplier maxHeapSupplier,
      AtomicLong nowMs, ServerMetrics serverMetrics) {
    return buildServerThrottleState(serverConfig, usedHeapSupplier, maxHeapSupplier, nowMs, serverMetrics,
        ServerIngestionOomProtectionManagerTest::noOpGc);
  }

  private static ServerIngestionOomProtectionManager.ServerThrottleState buildServerThrottleState(
      Map<String, Object> serverConfig, LongSupplier usedHeapSupplier, LongSupplier maxHeapSupplier,
      AtomicLong nowMs, ServerMetrics serverMetrics, Runnable gcRunner) {
    return new ServerIngestionOomProtectionManager.ServerThrottleState(new PinotConfiguration(serverConfig),
        serverMetrics, usedHeapSupplier, maxHeapSupplier, nowMs::get, gcRunner);
  }

  private static Map<String, Object> upsertDedupOnlyServerConfig() {
    return serverModeConfig("UPSERT_DEDUP_ONLY");
  }

  private static Map<String, Object> upsertDedupOnlyServerConfig(String key, Object value) {
    return Map.of(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE, "UPSERT_DEDUP_ONLY", key,
        value);
  }

  private static Map<String, Object> serverModeConfig(String mode) {
    return Map.of(CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE, mode);
  }

  private static String fullServerInstanceConfigKey(String key) {
    return CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + key;
  }

  private static TableConfig buildTableConfig(@Nullable Enablement oomProtection) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();
    if (oomProtection != null) {
      StreamIngestionConfig streamIngestionConfig =
          new StreamIngestionConfig(List.of(Map.of("streamType", "kafka")));
      streamIngestionConfig.setOomProtection(oomProtection);
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
      tableConfig.setIngestionConfig(ingestionConfig);
    }
    return tableConfig;
  }

  private static void noOpGc() {
  }
}
