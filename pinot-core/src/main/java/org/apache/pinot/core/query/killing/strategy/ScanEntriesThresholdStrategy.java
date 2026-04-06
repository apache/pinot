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
package org.apache.pinot.core.query.killing.strategy;

import javax.annotation.Nullable;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.query.killing.QueryKillReport;
import org.apache.pinot.core.query.killing.QueryKillingStrategy;
import org.apache.pinot.core.query.killing.QueryKillingStrategyFactory;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kills queries that exceed scan entry or doc thresholds.
 *
 * <p>Primary strategy for proactive query killing. Checks
 * {@code numEntriesScannedInFilter} (primary signal — catches expensive filter
 * predicates / missing indexes) and {@code numDocsScanned} (secondary signal —
 * catches large aggregations).</p>
 *
 * <p>A threshold of {@link Long#MAX_VALUE} disables that metric's check.</p>
 *
 *
 * <p>Supports table-level overrides via {@link #forQuery(QueryConfig, QueryMonitorConfig)}.
 * When a table has specific thresholds in its {@link QueryConfig}, a new instance is
 * created with the resolved values. Otherwise, the same instance is returned.</p>
 */
public class ScanEntriesThresholdStrategy implements QueryKillingStrategy {
  private static final String STRATEGY_NAME = "ScanEntriesThresholdStrategy";

  private final long _maxEntriesScannedInFilter;
  private final long _maxDocsScanned;

  public ScanEntriesThresholdStrategy(long maxEntriesScannedInFilter, long maxDocsScanned) {
    _maxEntriesScannedInFilter = maxEntriesScannedInFilter;
    _maxDocsScanned = maxDocsScanned;
  }

  @Override
  public boolean shouldTerminate(QueryScanCostContext ctx) {
    return (_maxEntriesScannedInFilter < Long.MAX_VALUE
            && ctx.getNumEntriesScannedInFilter() > _maxEntriesScannedInFilter)
        || (_maxDocsScanned < Long.MAX_VALUE
            && ctx.getNumDocsScanned() > _maxDocsScanned);
  }

  @Override
  public QueryKillReport buildKillReport(QueryScanCostContext ctx,
      String queryId, String tableName, String configSource) {
    String triggeringMetric;
    long actualValue;
    long thresholdValue;
    if (_maxEntriesScannedInFilter < Long.MAX_VALUE
        && ctx.getNumEntriesScannedInFilter() > _maxEntriesScannedInFilter) {
      triggeringMetric = "numEntriesScannedInFilter";
      actualValue = ctx.getNumEntriesScannedInFilter();
      thresholdValue = _maxEntriesScannedInFilter;
    } else {
      triggeringMetric = "numDocsScanned";
      actualValue = ctx.getNumDocsScanned();
      thresholdValue = _maxDocsScanned;
    }
    return new QueryKillReport(queryId, tableName, STRATEGY_NAME,
        triggeringMetric, actualValue, thresholdValue, configSource, ctx);
  }

  @Override
  public int priority() {
    return 10;
  }

  /**
   * Returns a query-specific variant with table-level threshold overrides applied.
   * If the table's {@link QueryConfig} has non-null threshold fields, they take precedence
   * over this strategy's thresholds. Otherwise, returns {@code this} (no allocation).
   */
  @Override
  public QueryKillingStrategy forQuery(@Nullable QueryConfig queryConfig,
      QueryMonitorConfig clusterConfig) {
    if (queryConfig == null) {
      return this;
    }
    Long tableEntries = queryConfig.getMaxEntriesScannedInFilter();
    Long tableDocs = queryConfig.getMaxDocsScanned();
    if (tableEntries == null && tableDocs == null) {
      return this;
    }
    return new ScanEntriesThresholdStrategy(
        tableEntries != null ? tableEntries : _maxEntriesScannedInFilter,
        tableDocs != null ? tableDocs : _maxDocsScanned);
  }

  public long getMaxEntriesScannedInFilter() {
    return _maxEntriesScannedInFilter;
  }

  public long getMaxDocsScanned() {
    return _maxDocsScanned;
  }

  /**
   * Factory that creates a {@link ScanEntriesThresholdStrategy} from
   * {@link QueryMonitorConfig}. This is the default factory used when no custom
   * strategy factory is configured.
   *
   * <p>Returns {@code null} if no scan thresholds are configured (all are
   * {@link Long#MAX_VALUE}), which causes the manager to log a warning that
   * scan-based killing is enabled but effectively unconfigured.</p>
   */
  public static class Factory implements QueryKillingStrategyFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Factory.class);

    @Override
    @Nullable
    public QueryKillingStrategy create(QueryMonitorConfig config) {
      long maxEntries = config.getScanBasedKillingMaxEntriesScannedInFilter();
      long maxDocs = config.getScanBasedKillingMaxDocsScanned();

      if (maxEntries == Long.MAX_VALUE && maxDocs == Long.MAX_VALUE) {
        LOGGER.warn("Scan-based killing is enabled but no thresholds are configured. "
            + "Set at least one of: accounting.scan.based.killing.max.entries.scanned.in.filter, "
            + "accounting.scan.based.killing.max.docs.scanned. "
            + "Scan-based killing will be effectively disabled until thresholds are set.");
        return null;
      }

      LOGGER.info("Initialized ScanEntriesThresholdStrategy with maxEntriesScannedInFilter={}, maxDocsScanned={}",
          maxEntries == Long.MAX_VALUE ? "disabled" : maxEntries,
          maxDocs == Long.MAX_VALUE ? "disabled" : maxDocs);
      return new ScanEntriesThresholdStrategy(maxEntries, maxDocs);
    }

    @Override
    public String getName() {
      return "ScanEntriesThresholdStrategyFactory";
    }
  }
}
