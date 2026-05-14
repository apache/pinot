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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central manager for scan-based query killing. Owns the guard rails and delegates the
 * actual kill decision to a {@link QueryKillingStrategy}.
 *
 * <p>The strategy is built once at init via a {@link QueryKillingStrategyFactory} and rebuilt when
 * cluster config changes via {@link #onChange}. The default factory is
 * {@link ScanEntriesThresholdStrategy.Factory}.</p>
 *
 */
public class QueryKillingManager implements PinotClusterConfigChangeListener {
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
  public static QueryKillingManager init(PinotConfiguration schedulerConfig, ServerMetrics serverMetrics) {
    long maxHeapSize = Runtime.getRuntime().maxMemory();
    QueryMonitorConfig config = new QueryMonitorConfig(schedulerConfig, maxHeapSize);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, serverMetrics);
    manager.rebuildStrategy();
    _instance = manager;
    return manager;
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
   * Handles ZK cluster config changes. Rebuilds the {@link QueryMonitorConfig} from the delta
   * and refreshes the killing strategy if scan-killing-related keys changed.
   */
  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    QueryMonitorConfig updated = new QueryMonitorConfig(_configRef.get(), changedConfigs, clusterConfigs);
    _configRef.set(updated);
    rebuildStrategy();
  }

  /**
   * Convenience overload called from {@link org.apache.pinot.core.operator.BaseOperator#checkTermination()}.
   * Reads query context (table name, query id, cached strategy) from the execution context.
   */
  public void checkAndKillIfNeeded(QueryExecutionContext executionContext, QueryScanCostContext scanCostContext) {
    Object cached = executionContext.getCachedKillingStrategy();
    QueryKillingStrategy cachedStrategy;
    if (cached instanceof QueryKillingStrategy) {
      cachedStrategy = (QueryKillingStrategy) cached;
    } else {
      if (cached != null) {
        LOGGER.warn("Unexpected cached killing strategy type: {}", cached.getClass().getName());
      }
      cachedStrategy = null;
    }
    checkAndKillIfNeeded(executionContext, scanCostContext, cachedStrategy,
        executionContext.getQueryId(), executionContext.getTableName());
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
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return;
    }

    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return;
    }

    if (executionContext.getTerminateException() != null) {
      return;
    }

    try {
      QueryKillingStrategy queryStrategy = strategy.forQuery(queryConfig, config);
      String configSource = (queryStrategy != strategy) ? "table:" + tableName : "cluster";
      checkAndKillWithStrategy(executionContext, scanCostContext, queryStrategy, configSource, queryId, tableName,
          config);
    } catch (Exception e) {
      LOGGER.error("Error in scan-based killing evaluation for query {}", queryId, e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_ERROR, 1);
    }
  }

  /**
   * Resolves a per-query strategy (applying table-level overrides from {@code queryConfig}).
   * Returns null if killing is disabled. Caches the resolved strategy on the execution context.
   */
  @Nullable
  public QueryKillingStrategy resolveQueryStrategy(@Nullable QueryConfig queryConfig) {
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return null;
    }
    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return null;
    }
    return strategy.forQuery(queryConfig, config);
  }

  private void checkAndKillIfNeeded(QueryExecutionContext executionContext, QueryScanCostContext scanCostContext,
      @Nullable QueryKillingStrategy cachedStrategy, @Nullable String queryId, @Nullable String tableName) {
    QueryKillingStrategy currentStrategy = _strategy;
    QueryKillingStrategy strategy = cachedStrategy != null ? cachedStrategy : currentStrategy;
    if (strategy == null) {
      return;
    }
    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return;
    }
    if (executionContext.getTerminateException() != null) {
      return;
    }
    String resolvedQueryId = queryId != null ? queryId : "unknown";
    String resolvedTableName = tableName != null ? tableName : "unknown";
    String configSource = (cachedStrategy != null && cachedStrategy != currentStrategy) ? "table:" + resolvedTableName
        : "cluster";
    try {
      checkAndKillWithStrategy(executionContext, scanCostContext, strategy, configSource, resolvedQueryId,
          resolvedTableName, config);
    } catch (Exception e) {
      LOGGER.error("Error in scan-based killing evaluation for query {}", resolvedQueryId, e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_ERROR, 1);
    }
  }

  private void checkAndKillWithStrategy(QueryExecutionContext executionContext, QueryScanCostContext scanCostContext,
      QueryKillingStrategy queryStrategy, String configSource, String queryId, String tableName,
      QueryMonitorConfig config) {
    if (!queryStrategy.shouldTerminate(scanCostContext)) {
      return;
    }
    // Resolve effective mode: per-table override takes precedence over cluster config
    CommonConstants.Accounting.ScanKillingMode effectiveMode = executionContext.getEffectiveScanKillingMode();
    if (effectiveMode == CommonConstants.Accounting.ScanKillingMode.DISABLED) {
      return;
    }
    boolean logOnly = effectiveMode == CommonConstants.Accounting.ScanKillingMode.LOG_ONLY
        || (effectiveMode == null && config.isScanBasedKillingLogOnly());
    long requestId = executionContext.getRequestId();
    QueryKillReport report = queryStrategy.buildKillReport(scanCostContext, requestId, queryId, tableName,
        configSource);
    if (logOnly) {
      LOGGER.info("Query killed in LogOnly mode: {}", report.toInternalLogMessage());
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1);
      return;
    }
    LOGGER.warn("Query Killed in enforce mode: {}", report.toInternalLogMessage());
    executionContext.terminate(queryStrategy.getErrorCode(), report.toCustomerMessage());
    _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1);
  }
}
