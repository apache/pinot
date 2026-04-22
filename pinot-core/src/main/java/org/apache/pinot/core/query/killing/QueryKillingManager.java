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

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central manager for scan-based query killing. Owns the guard rails and delegates the
 * actual kill decision to a {@link QueryKillingStrategy}.
 *
 * The default factory is {@link ScanEntriesThresholdStrategy.Factory}, which reads
 * scan thresholds from {@link QueryMonitorConfig}. Custom factories can be configured
 * via {@code accounting.scan.based.killing.strategy.factory.class.name}.
 *
 */
public class QueryKillingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryKillingManager.class);

  private static volatile QueryKillingManager _instance;

  private final AtomicReference<QueryMonitorConfig> _configRef;
  private final ServerMetrics _serverMetrics;

  /**
   * Null if: killing is disabled, config is insufficient, or factory failed to load.
   * Rebuilt when config changes via {@link #rebuildStrategy()}.
   */
  @Nullable
  private volatile QueryKillingStrategy _strategy;

  public QueryKillingManager(AtomicReference<QueryMonitorConfig> configRef, ServerMetrics serverMetrics) {
    _configRef = configRef;
    _serverMetrics = serverMetrics;
  }

  /**
   * Initializes the singleton instance and builds the strategy from config.
   * Called once during server startup.
   */
  public static void init(AtomicReference<QueryMonitorConfig> configRef, ServerMetrics serverMetrics) {
    QueryKillingManager manager = new QueryKillingManager(configRef, serverMetrics);
    manager.rebuildStrategy();
    _instance = manager;
  }

  @Nullable
  public static QueryKillingManager getInstance() {
    return _instance;
  }

  /**
   * Rebuilds the strategy from the current config. Called at init and when
   * cluster config changes (via the same onChange path that rebuilds QueryMonitorConfig).
   */
  public void rebuildStrategy() {
    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      _strategy = null;
      return;
    }

    try {
      QueryKillingStrategyFactory factory = loadFactory(config);
      _strategy = factory.create(config);
      if (_strategy == null) {
        LOGGER.warn("Scan-based killing is enabled but strategy factory '{}' returned null — "
            + "required configuration may be missing. Scan-based killing will be effectively disabled.",
            factory.getName());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to initialize scan-based killing strategy. "
          + "Scan-based killing will be disabled.", e);
      _strategy = null;
    }
  }

  /**
   * Loads the strategy factory from config. If a custom factory class name is configured,
   * loads it by reflection (following the same pattern as {@code ThreadAccountantUtils.createAccountant()}).
   * Otherwise, returns the default {@link ScanEntriesThresholdStrategy.Factory}.
   */
  private QueryKillingStrategyFactory loadFactory(QueryMonitorConfig config) {
    String factoryClassName = config.getScanBasedKillingStrategyFactoryClassName();
    if (factoryClassName != null && !factoryClassName.isEmpty()) {
      LOGGER.info("Loading custom query killing strategy factory: {}", factoryClassName);
      try {
        return (QueryKillingStrategyFactory) Class.forName(factoryClassName)
            .getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        LOGGER.error("Failed to load custom strategy factory '{}', falling back to default",
            factoryClassName, e);
      }
    }
    return new ScanEntriesThresholdStrategy.Factory();
  }

  /**
   * Returns the active strategy. Visible for testing.
   */
  @Nullable
  public QueryKillingStrategy getActiveStrategy() {
    return _strategy;
  }

  /**
   * Evaluates whether the query should be killed based on the active strategy.
   *
   * <p>Calls {@link QueryKillingStrategy#forQuery(QueryConfig, QueryMonitorConfig)}
   * to resolve table-level overrides before evaluating.</p>
   */
  public void checkAndKillIfNeeded(QueryExecutionContext executionContext,
      QueryScanCostContext scanCostContext, String queryId, String tableName,
      @Nullable QueryConfig queryConfig) {
    // no strategy means killing is disabled or unconfigured
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return;
    }

    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return;
    }

    // Prevent duplicate kills
    if (executionContext.getTerminateException() != null) {
      return;
    }

    try {
      // Resolve per-query table overrides (returns same instance if no overrides)
      QueryKillingStrategy queryStrategy = strategy.forQuery(queryConfig, config);

      String configSource = (queryStrategy != strategy) ? "table:" + tableName : "cluster";

      // Delegate to strategy
      if (!queryStrategy.shouldTerminate(scanCostContext)) {
        return;
      }

      QueryKillReport report = queryStrategy.buildKillReport(
          scanCostContext, queryId, tableName, configSource);

      if (config.isScanBasedKillingLogOnly()) {
        LOGGER.info("Query killed in LogOnly mode: {}", report.toInternalLogMessage());
        _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1);
        return;
      }

      LOGGER.warn("Query Killed in enforce mode: {}", report.toInternalLogMessage());
      executionContext.terminate(queryStrategy.getErrorCode(), report.toCustomerMessage());
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1);
    } catch (Exception e) {
      LOGGER.error("Error in scan-based killing evaluation for query {}", queryId, e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_ERROR, 1);
    }
  }
}
