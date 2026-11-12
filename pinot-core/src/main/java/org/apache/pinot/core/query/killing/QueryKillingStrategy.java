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

import javax.annotation.Nullable;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryScanCostContext;


/// Determines whether a query should be terminated based on accumulated scan cost.
///
/// To create a custom strategy:
///
/// 1. Implement this interface
/// 2. Implement [QueryKillingStrategyFactory] to create it from config
/// 3. Set `accounting.scan.based.killing.strategy.factory.class.name`
///       to your factory class name
public interface QueryKillingStrategy {

  /// Returns true if the query should be terminated immediately.
  boolean shouldTerminate(QueryScanCostContext context);

  /// Builds a structured kill report with full context.
  /// Only called when [#shouldTerminate] returns true.
  QueryKillReport buildKillReport(QueryScanCostContext context,
      long requestId, String queryId, String tableName, String configSource);

  /// Error code for the termination response.
  default QueryErrorCode getErrorCode() {
    return QueryErrorCode.QUERY_SCAN_LIMIT_EXCEEDED;
  }

  /// Priority: lower number = checked first in composite strategies.
  default int priority() {
    return 100;
  }

  /// Returns a query-specific variant of this strategy with table-level overrides applied.
  /// If no table overrides are relevant, returns `this` (no new allocation).
  ///
  /// The default implementation returns `this`, meaning the strategy does not
  /// support table-level overrides. Strategies that support per-table thresholds
  /// (like [org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy])
  /// should override this method.
  ///
  /// @param queryConfig table-level query config (nullable — null means no table overrides)
  /// @param clusterConfig cluster-level config for resolving fallback thresholds
  /// @return this strategy or a new instance with resolved thresholds
  default QueryKillingStrategy forQuery(@Nullable QueryConfig queryConfig,
      QueryMonitorConfig clusterConfig) {
    return this;
  }
}
