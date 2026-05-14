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
import java.util.concurrent.atomic.AtomicReference;
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

import static org.mockito.Mockito.mock;
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

  // --- Test fixtures for pluggable strategy ---

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
