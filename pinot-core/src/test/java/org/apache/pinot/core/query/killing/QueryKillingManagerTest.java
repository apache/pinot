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
package org.apache.pinot.core.query.killing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting.ScanKillingMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link QueryKillingManager}.
 */
public class QueryKillingManagerTest {

  private ServerMetrics _serverMetrics;

  @BeforeMethod
  public void setUp() {
    _serverMetrics = mock(ServerMetrics.class);
  }

  private QueryMonitorConfig buildConfig(String mode, long maxEntriesInFilter,
      long maxDocsScanned) {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE, mode);
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER,
        maxEntriesInFilter);
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_DOCS_SCANNED, maxDocsScanned);
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_POST_FILTER, Long.MAX_VALUE);
    PinotConfiguration pinotConfig = new PinotConfiguration(props);
    return new QueryMonitorConfig(pinotConfig, Runtime.getRuntime().maxMemory());
  }

  // --- Strategy built from config (init-time validation) ---

  @Test
  public void testInitBuildsStrategyFromConfig() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    assertNotNull(manager.getActiveStrategy(), "Strategy should be built when thresholds are configured");
    assertTrue(manager.getActiveStrategy() instanceof ScanEntriesThresholdStrategy);
  }

  @Test
  public void testInitWithNoThresholdsLogsWarningAndReturnsNullStrategy() {
    // All thresholds are MAX_VALUE — factory should return null
    QueryMonitorConfig config = buildConfig("enforce", Long.MAX_VALUE, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    assertNull(manager.getActiveStrategy(),
        "Strategy should be null when no thresholds are configured");
  }

  @Test
  public void testInitWithDisabledReturnsNullStrategy() {
    QueryMonitorConfig config = buildConfig("disabled", 100L, 100L);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    assertNull(manager.getActiveStrategy(),
        "Strategy should be null when killing is disabled");
  }

  // --- Default strategy (ScanEntriesThresholdStrategy from config) ---

  @Test
  public void testDisabledDoesNotKill() {
    QueryMonitorConfig config = buildConfig("disabled", 100L, 100L);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(500L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q1", "testTable_OFFLINE", null);
    assertNull(execCtx.getTerminateException());
  }

  @Test
  public void testEnabledKillsWhenThresholdExceeded() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q2", "testTable_OFFLINE", null);
    assertNotNull(execCtx.getTerminateException());
    assertEquals(execCtx.getTerminateException().getErrorCode(), QueryErrorCode.QUERY_SCAN_LIMIT_EXCEEDED);
  }

  @Test
  public void testLogOnlyDoesNotKill() {
    QueryMonitorConfig config = buildConfig("logOnly", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q3", "testTable_OFFLINE", null);
    assertNull(execCtx.getTerminateException());
  }

  @Test
  public void testBelowThresholdDoesNotKill() {
    QueryMonitorConfig config = buildConfig("enforce", 1000L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(500L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q4", "testTable_OFFLINE", null);
    assertNull(execCtx.getTerminateException());
  }

  @Test
  public void testAlreadyTerminatedSkipsEvaluation() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.terminate(QueryErrorCode.QUERY_CANCELLATION, "cancelled");

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q5", "testTable_OFFLINE", null);
    assertNotNull(execCtx.getTerminateException());
  }

  @Test
  public void testDocsScannedThreshold() {
    QueryMonitorConfig config = buildConfig("enforce", Long.MAX_VALUE, 100L);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addDocsScanned(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q8", "testTable_OFFLINE", null);
    assertNotNull(execCtx.getTerminateException());
  }

  // --- Table overrides via forQuery() ---

  @Test
  public void testTableOverrideRaisesThreshold() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L); // Above cluster (100), below table (500)

    QueryConfig queryConfig = new QueryConfig(null, null, null, null, null, null, 500L, null, null);
    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q6", "testTable_OFFLINE", queryConfig);
    assertNull(execCtx.getTerminateException(),
        "Table override should raise threshold, preventing kill");
  }

  @Test
  public void testTableOverrideLowersThreshold() {
    QueryMonitorConfig config = buildConfig("enforce", 1000L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // Below cluster (1000), above table (50)

    QueryConfig queryConfig = new QueryConfig(null, null, null, null, null, null, 50L, null, null);
    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q7", "testTable_OFFLINE", queryConfig);
    assertNotNull(execCtx.getTerminateException(),
        "Table override should lower threshold, causing kill");
  }

  // --- Custom strategy factory pluggability ---

  @Test
  public void testCustomFactoryClassFromConfig() {
    // Configure a custom factory class name
    Map<String, Object> props = new HashMap<>();
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE,
        "enforce");
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_STRATEGY_FACTORY_CLASS_NAME,
        AlwaysKillStrategyFactory.class.getName());
    PinotConfiguration pinotConfig = new PinotConfiguration(props);
    QueryMonitorConfig config = new QueryMonitorConfig(pinotConfig, Runtime.getRuntime().maxMemory());

    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    assertNotNull(manager.getActiveStrategy(), "Custom factory should create a strategy");

    // Should kill even with zero scan entries (AlwaysKillStrategy always kills)
    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q10", "testTable_OFFLINE", null);
    assertNotNull(execCtx.getTerminateException(),
        "Custom AlwaysKillStrategy should kill regardless of scan counts");
  }

  @Test
  public void testInvalidFactoryClassFallsBackGracefully() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE,
        "enforce");
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_STRATEGY_FACTORY_CLASS_NAME,
        "com.nonexistent.FakeFactory");
    props.put(CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER, 100L);
    PinotConfiguration pinotConfig = new PinotConfiguration(props);
    QueryMonitorConfig config = new QueryMonitorConfig(pinotConfig, Runtime.getRuntime().maxMemory());

    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    // Should not crash — falls back to default factory
    assertNotNull(manager.getActiveStrategy(),
        "Invalid factory should fall back to default ScanEntriesThresholdStrategy");
    assertTrue(manager.getActiveStrategy() instanceof ScanEntriesThresholdStrategy);
  }

  @Test
  public void testRebuildStrategyPicksUpConfigChanges() {
    // Start with no thresholds
    QueryMonitorConfig config1 = buildConfig("enforce", Long.MAX_VALUE, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config1);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();
    assertNull(manager.getActiveStrategy(), "No thresholds = no strategy");

    // Update config with thresholds
    QueryMonitorConfig config2 = buildConfig("enforce", 100L, Long.MAX_VALUE);
    configRef.set(config2);
    manager.rebuildStrategy();
    assertNotNull(manager.getActiveStrategy(), "After config update, strategy should be built");
  }

  // --- onChange (dynamic config reload) ---

  @Test
  public void testOnChangeRebuildsStrategy() {
    // Start with killing disabled
    QueryMonitorConfig disabledConfig = buildConfig("disabled", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(disabledConfig);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();
    assertNull(manager.getActiveStrategy(), "Strategy should be null when disabled");

    // Simulate cluster config change enabling killing with enforce mode + threshold.
    // Keys arrive from ZK with full "pinot.query.scheduler." prefix — onChange() strips it.
    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";

    Set<String> changedKeys = new HashSet<>();
    changedKeys.add(prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE);
    changedKeys.add(prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE, "enforce");
    clusterConfigs.put(
        prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER, "500");

    manager.onChange(changedKeys, clusterConfigs);
    assertNotNull(manager.getActiveStrategy(),
        "Strategy should be rebuilt after onChange enables killing");
  }

  @Test
  public void testOnChangeDisablesStrategy() {
    // Start with killing enabled
    QueryMonitorConfig enabledConfig = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(enabledConfig);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();
    assertNotNull(manager.getActiveStrategy(), "Strategy should be active when enabled");

    // Simulate cluster config change to disable killing (full ZK-prefixed keys)
    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";

    Set<String> changedKeys = new HashSet<>();
    changedKeys.add(prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(prefix + CommonConstants.Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE, "disabled");

    manager.onChange(changedKeys, clusterConfigs);
    assertNull(manager.getActiveStrategy(),
        "Strategy should be null after onChange disables killing");
  }

  @Test
  public void testOnChangeIgnoresIrrelevantKeys() {
    // Start with killing enabled
    QueryMonitorConfig enabledConfig = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(enabledConfig);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();
    assertNotNull(manager.getActiveStrategy(), "Strategy should be active when enabled");

    // Simulate a ZK change that only touches non-scheduler keys — should be ignored
    Set<String> changedKeys = new HashSet<>();
    changedKeys.add("some.unrelated.config");
    changedKeys.add("helix.rebalance.something");

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put("some.unrelated.config", "value");

    manager.onChange(changedKeys, clusterConfigs);
    assertNotNull(manager.getActiveStrategy(),
        "Strategy should remain unchanged when no scheduler keys changed");
  }

  // --- Convenience overload (2-arg checkAndKillIfNeeded) ---

  @Test
  public void testConvenienceOverloadKillsViaCachedStrategy() {
    // Create a manager with killing enabled, threshold = 50 for entries scanned
    QueryMonitorConfig config = buildConfig("enforce", 50L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    // Resolve the per-query strategy and cache it on the execution context
    QueryKillingStrategy resolvedStrategy = manager.resolveQueryStrategy(null);
    assertNotNull(resolvedStrategy);

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setTableName("testTable_OFFLINE");
    execCtx.setQueryId("conv-q1");
    execCtx.setCachedKillingStrategy(resolvedStrategy);

    // Create scan cost exceeding the threshold
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // Above threshold of 50

    // Use the 2-arg convenience overload
    manager.checkAndKillIfNeeded(execCtx, scanCtx);
    assertNotNull(execCtx.getTerminateException(),
        "Query should be terminated via cached strategy when threshold is exceeded");
  }

  @Test
  public void testConvenienceOverloadNullScanCostContextNoOp() {
    // When the manager's strategy is null (disabled), calling with null scanCostContext is a no-op.
    // In production, BaseOperator guards against null scanCostContext before calling the manager.
    // Here we verify the manager's early-return when disabled.
    QueryMonitorConfig config = buildConfig("disabled", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // No queryScanCostContext set on execCtx — mirrors the real scenario where
    // scan-based killing is not initialized for this query.

    // The 2-arg overload with null scanCostContext is safe when strategy is null (disabled)
    manager.checkAndKillIfNeeded(execCtx, null);
    assertNull(execCtx.getTerminateException(),
        "No exception should be set when killing is disabled and scanCostContext is null");
  }

  // --- resolveQueryStrategy ---

  @Test
  public void testResolveQueryStrategyReturnsNullWhenDisabled() {
    QueryMonitorConfig config = buildConfig("disabled", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryKillingStrategy resolved = manager.resolveQueryStrategy(null);
    assertNull(resolved, "resolveQueryStrategy should return null when killing is disabled");
  }

  @Test
  public void testResolveQueryStrategyReturnsStrategyWhenEnabled() {
    QueryMonitorConfig config = buildConfig("enforce", 200L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryKillingStrategy resolved = manager.resolveQueryStrategy(null);
    assertNotNull(resolved, "resolveQueryStrategy should return a strategy when killing is enabled");
    assertTrue(resolved instanceof ScanEntriesThresholdStrategy);
  }

  @Test
  public void testTableModeEnforceOverridesClusterLogOnly() {
    // Cluster is logOnly — normally no kills
    QueryMonitorConfig config = buildConfig("logOnly", 50L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // Table override: enforce
    execCtx.setEffectiveScanKillingMode(ScanKillingMode.ENFORCE);
    execCtx.setTableName("testTable_OFFLINE");
    execCtx.setQueryId("tbl-mode-q1");

    QueryKillingStrategy strategy = manager.resolveQueryStrategy(null);
    assertNotNull(strategy);
    execCtx.setCachedKillingStrategy(strategy);

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // exceeds cluster threshold of 50

    manager.checkAndKillIfNeeded(execCtx, scanCtx);
    assertNotNull(execCtx.getTerminateException(),
        "Table enforce override should cause real termination even when cluster is logOnly");
    assertEquals(execCtx.getTerminateException().getErrorCode(), QueryErrorCode.QUERY_SCAN_LIMIT_EXCEEDED);
  }

  @Test
  public void testTableModeLogOnlyOverridesClusterEnforce() {
    // Cluster is enforce — normally kills
    QueryMonitorConfig config = buildConfig("enforce", 50L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // Table override: logOnly — should downgrade to dry-run
    execCtx.setEffectiveScanKillingMode(ScanKillingMode.LOG_ONLY);
    execCtx.setTableName("testTable_OFFLINE");
    execCtx.setQueryId("tbl-mode-q2");

    QueryKillingStrategy strategy = manager.resolveQueryStrategy(null);
    assertNotNull(strategy);
    execCtx.setCachedKillingStrategy(strategy);

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // exceeds cluster threshold of 50

    manager.checkAndKillIfNeeded(execCtx, scanCtx);
    assertNull(execCtx.getTerminateException(),
        "Table logOnly override should prevent real kill even when cluster is enforce");
  }

  @Test
  public void testTableModeDisabledOverridesClusterEnforce() {
    // Cluster is enforce — normally kills
    QueryMonitorConfig config = buildConfig("enforce", 50L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // Table override: disabled — fully exempt
    execCtx.setEffectiveScanKillingMode(ScanKillingMode.DISABLED);
    execCtx.setTableName("testTable_OFFLINE");
    execCtx.setQueryId("tbl-mode-q3");

    QueryKillingStrategy strategy = manager.resolveQueryStrategy(null);
    assertNotNull(strategy);
    execCtx.setCachedKillingStrategy(strategy);

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // exceeds cluster threshold of 50

    manager.checkAndKillIfNeeded(execCtx, scanCtx);
    assertNull(execCtx.getTerminateException(),
        "Table disabled override should fully exempt the table from killing");
  }

  @Test
  public void testNoTableModeOverrideFallsBackToCluster() {
    // Cluster is enforce, no table mode override
    QueryMonitorConfig config = buildConfig("enforce", 50L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // No setEffectiveScanKillingMode call — should use cluster mode (enforce)
    execCtx.setTableName("testTable_OFFLINE");
    execCtx.setQueryId("tbl-mode-q4");

    QueryKillingStrategy strategy = manager.resolveQueryStrategy(null);
    assertNotNull(strategy);
    execCtx.setCachedKillingStrategy(strategy);

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(100L); // exceeds cluster threshold of 50

    manager.checkAndKillIfNeeded(execCtx, scanCtx);
    assertNotNull(execCtx.getTerminateException(),
        "Without table mode override, cluster enforce mode should kill");
  }

  // --- Per-table metric emission ---

  @Test
  public void testEnforceKillEmitsPerTableMetric() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q-metric-1", "myTable_REALTIME", null);

    verify(_serverMetrics).addMeteredTableValue("myTable_REALTIME", ServerMeter.QUERIES_KILLED_SCAN, 1L);
    verify(_serverMetrics, never()).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1L);
  }

  @Test
  public void testLogOnlyKillEmitsPerTableDryRunMetric() {
    QueryMonitorConfig config = buildConfig("logOnly", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    manager.checkAndKillIfNeeded(execCtx, scanCtx, "q-metric-2", "myTable_REALTIME", null);

    verify(_serverMetrics).addMeteredTableValue("myTable_REALTIME", ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    verify(_serverMetrics, never()).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    // logOnly should not actually terminate
    assertNull(execCtx.getTerminateException());
  }

  @Test
  public void testNullTableNameFallsBackToGlobalErrorMetric() {
    // When the strategy throws inside checkAndKillIfNeeded, the catch block emits the error
    // metric. With a null table name, the helper falls back to global emission so we do not
    // silently drop the error signal.
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    // Force the catch path with a cached strategy that throws inside shouldTerminate.
    execCtx.setCachedKillingStrategy(new QueryKillingStrategy() {
      @Override
      public boolean shouldTerminate(QueryScanCostContext context) {
        throw new RuntimeException("boom");
      }

      @Override
      public QueryKillReport buildKillReport(QueryScanCostContext context, long requestId,
          String queryId, String tableName, String configSource) {
        // unused — shouldTerminate throws first
        throw new UnsupportedOperationException();
      }
    });
    execCtx.setTableName(null);
    execCtx.setQueryId("q-metric-null");

    manager.checkAndKillIfNeeded(execCtx, new QueryScanCostContext());

    // Null table name → global fallback emission, never per-table
    verify(_serverMetrics).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_ERROR, 1L);
    verify(_serverMetrics, never()).addMeteredTableValue(anyString(),
        eq(ServerMeter.QUERIES_KILLED_SCAN_ERROR), anyLong());
  }

  @Test
  public void testNullTableNameInReportFallsBackToGlobalEnforceMetric() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setCachedKillingStrategy(new QueryKillingStrategy() {
      @Override
      public boolean shouldTerminate(QueryScanCostContext context) {
        return true;
      }

      @Override
      public QueryKillReport buildKillReport(QueryScanCostContext context, long requestId,
          String queryId, String tableName, String configSource) {
        // Intentionally drop the table name to exercise the null-fallback path
        return new QueryKillReport(requestId, queryId, null, "TestStrategy", "test", 0, 0,
            configSource, context);
      }

      @Override
      public org.apache.pinot.spi.exception.QueryErrorCode getErrorCode() {
        return org.apache.pinot.spi.exception.QueryErrorCode.QUERY_SCAN_LIMIT_EXCEEDED;
      }
    });
    execCtx.setTableName(null);
    execCtx.setQueryId("q-null-report");

    manager.checkAndKillIfNeeded(execCtx, new QueryScanCostContext());

    verify(_serverMetrics).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1L);
    verify(_serverMetrics, never()).addMeteredTableValue(anyString(),
        eq(ServerMeter.QUERIES_KILLED_SCAN), anyLong());
  }

  @Test
  public void testUnknownTableNameSentinelFallsBackToGlobalMetric() {
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    // Use the convenience overload with null tableName → routes through "unknown" sentinel
    execCtx.setTableName(null);
    execCtx.setQueryId("q-unknown");
    execCtx.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    manager.checkAndKillIfNeeded(execCtx, scanCtx);

    verify(_serverMetrics).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1L);
    verify(_serverMetrics, never()).addMeteredTableValue(eq("unknown"),
        eq(ServerMeter.QUERIES_KILLED_SCAN), anyLong());
  }

  // --- Dry-run emit-once guard ---

  @Test
  public void testLogOnlyEmitsExactlyOncePerQueryAcrossManyBlockChecks() {
    // logOnly mode never terminates the query, so without a guard, every subsequent block-level
    // termination check after the threshold is crossed re-builds a report, re-logs, and
    // re-emits the metric. This test simulates 10,000 block checks and verifies exactly one
    // emission — the CAS guard on QueryExecutionContext suppresses the duplicates.
    QueryMonitorConfig config = buildConfig("logOnly", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setTableName("dryRunTable_REALTIME");
    execCtx.setQueryId("q-dry-run-once");
    execCtx.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L); // permanently over threshold

    for (int i = 0; i < 10_000; i++) {
      manager.checkAndKillIfNeeded(execCtx, scanCtx);
    }

    // Exactly one per-table dry-run metric emission across 10k block checks
    verify(_serverMetrics).addMeteredTableValue("dryRunTable_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    verify(_serverMetrics, never()).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    // logOnly never terminates
    assertNull(execCtx.getTerminateException());
  }

  @Test
  public void testEnforceEmitsOncePerQueryAcrossManyBlockChecks() {
    // Sanity check: enforce mode is already guarded by getTerminateException() != null, but the
    // dry-run CAS must not regress that. Verify enforce still emits exactly once.
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setTableName("enforceTable_REALTIME");
    execCtx.setQueryId("q-enforce-once");
    execCtx.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    for (int i = 0; i < 10_000; i++) {
      manager.checkAndKillIfNeeded(execCtx, scanCtx);
    }

    verify(_serverMetrics).addMeteredTableValue("enforceTable_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN, 1L);
    assertNotNull(execCtx.getTerminateException());
  }

  @Test
  public void testDryRunGuardIsPerQueryNotGlobal() {
    // Two independent queries in logOnly mode should each emit once — the CAS lives on
    // QueryExecutionContext, not on the manager or a static.
    QueryMonitorConfig config = buildConfig("logOnly", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    QueryExecutionContext execCtxA = QueryExecutionContext.forSseTest();
    execCtxA.setTableName("tableA_REALTIME");
    execCtxA.setQueryId("qA");
    execCtxA.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    QueryExecutionContext execCtxB = QueryExecutionContext.forSseTest();
    execCtxB.setTableName("tableB_REALTIME");
    execCtxB.setQueryId("qB");
    execCtxB.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    for (int i = 0; i < 100; i++) {
      manager.checkAndKillIfNeeded(execCtxA, scanCtx);
      manager.checkAndKillIfNeeded(execCtxB, scanCtx);
    }

    verify(_serverMetrics).addMeteredTableValue("tableA_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    verify(_serverMetrics).addMeteredTableValue("tableB_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
  }

  @Test
  public void testLogOnlyConcurrentCallersEmitExactlyOnce()
      throws InterruptedException {
    // Production reality: BaseOperator.checkTermination() is invoked from multiple worker
    // threads in parallel (one per segment / per block). Without an atomic CAS, two threads
    // racing past the threshold at the same instant could both pass the "not yet emitted"
    // check and double-emit. AtomicBoolean.compareAndSet guarantees exactly one winner; this
    // test fails if the guard is downgraded to a plain or volatile boolean.
    QueryMonitorConfig config = buildConfig("logOnly", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setTableName("concurrentLogOnly_REALTIME");
    execCtx.setQueryId("q-concurrent-dry-run");
    execCtx.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    int threadCount = 16;
    int iterationsPerThread = 5_000;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startBarrier = new CountDownLatch(1);
    CountDownLatch doneBarrier = new CountDownLatch(threadCount);

    try {
      for (int t = 0; t < threadCount; t++) {
        pool.submit(() -> {
          try {
            startBarrier.await();
            for (int i = 0; i < iterationsPerThread; i++) {
              manager.checkAndKillIfNeeded(execCtx, scanCtx);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneBarrier.countDown();
          }
        });
      }
      startBarrier.countDown();
      assertTrue(doneBarrier.await(30, TimeUnit.SECONDS), "Workers did not complete in time");
    } finally {
      pool.shutdownNow();
    }

    // Even with 16 threads × 5000 iterations = 80,000 concurrent attempts, the CAS guarantees
    // exactly one emission.
    verify(_serverMetrics, times(1)).addMeteredTableValue("concurrentLogOnly_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    verify(_serverMetrics, never()).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1L);
    assertNull(execCtx.getTerminateException(), "logOnly must not terminate");
  }

  @Test
  public void testEnforceConcurrentCallersEmitExactlyOnce()
      throws InterruptedException {
    // Enforce-mode duplicate-emit prevention: terminate() is internally a synchronized CAS that
    // returns true only on the first transition. The manager gates the emit + warn log on the
    // return value, so two threads that both observe (!isTerminated) and cross the threshold
    // before either commits will still result in exactly one emission.
    QueryMonitorConfig config = buildConfig("enforce", 100L, Long.MAX_VALUE);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, _serverMetrics);
    manager.rebuildStrategy();

    QueryExecutionContext execCtx = QueryExecutionContext.forSseTest();
    execCtx.setTableName("concurrentEnforce_REALTIME");
    execCtx.setQueryId("q-concurrent-enforce");
    execCtx.setCachedKillingStrategy(manager.resolveQueryStrategy(null));

    QueryScanCostContext scanCtx = new QueryScanCostContext();
    scanCtx.addEntriesScannedInFilter(200L);

    int threadCount = 16;
    int iterationsPerThread = 5_000;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startBarrier = new CountDownLatch(1);
    CountDownLatch doneBarrier = new CountDownLatch(threadCount);

    try {
      for (int t = 0; t < threadCount; t++) {
        pool.submit(() -> {
          try {
            startBarrier.await();
            for (int i = 0; i < iterationsPerThread; i++) {
              manager.checkAndKillIfNeeded(execCtx, scanCtx);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneBarrier.countDown();
          }
        });
      }
      startBarrier.countDown();
      assertTrue(doneBarrier.await(30, TimeUnit.SECONDS), "Workers did not complete in time");
    } finally {
      pool.shutdownNow();
    }

    verify(_serverMetrics, times(1)).addMeteredTableValue("concurrentEnforce_REALTIME",
        ServerMeter.QUERIES_KILLED_SCAN, 1L);
    verify(_serverMetrics, never()).addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1L);
    assertNotNull(execCtx.getTerminateException());
  }

  /**
   * A test strategy that always kills — used to verify custom factory loading.
   */
  public static class AlwaysKillStrategy implements QueryKillingStrategy {
    @Override
    public boolean shouldTerminate(QueryScanCostContext context) {
      return true;
    }

    @Override
    public QueryKillReport buildKillReport(QueryScanCostContext context,
        long requestId, String queryId, String tableName, String configSource) {
      return new QueryKillReport(requestId, queryId, tableName, "AlwaysKillStrategy",
          "always", 0, 0, configSource, context);
    }
  }

  /**
   * A test factory that creates an AlwaysKillStrategy — loaded by class name via config.
   */
  public static class AlwaysKillStrategyFactory implements QueryKillingStrategyFactory {
    @Override
    public QueryKillingStrategy create(QueryMonitorConfig config) {
      return new AlwaysKillStrategy();
    }

    @Override
    public String getName() {
      return "AlwaysKillStrategyFactory";
    }
  }
}
