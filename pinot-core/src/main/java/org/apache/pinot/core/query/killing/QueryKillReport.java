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

import org.apache.pinot.spi.query.QueryScanCostContext;


/**
 * Immutable snapshot of a query-kill event
 */
public final class QueryKillReport {
  private final String _queryId;
  private final String _tableName;
  private final String _strategyName;
  private final String _triggeringMetric;
  private final long _actualValue;
  private final long _thresholdValue;
  private final String _configSource;
  private final long _snapshotEntriesScannedInFilter;
  private final long _snapshotDocsScanned;
  private final long _snapshotEntriesScannedPostFilter;
  private final long _elapsedTimeMs;

  /**
   * Creates a {@code QueryKillReport} by snapshotting the current state of {@code context}.
   *
   * @param queryId          unique identifier of the killed query
   * @param tableName        fully-qualified table name (e.g. {@code myTable_OFFLINE})
   * @param strategyName     name of the kill strategy that triggered the kill
   * @param triggeringMetric name of the metric that exceeded the threshold
   * @param actualValue      observed metric value at kill time
   * @param thresholdValue   configured threshold that was exceeded
   * @param configSource     source of the threshold config (e.g. {@code TABLE_CONFIG})
   * @param context          live scan-cost context; values are snapshotted immediately
   */
  public QueryKillReport(String queryId, String tableName, String strategyName, String triggeringMetric,
      long actualValue, long thresholdValue, String configSource, QueryScanCostContext context) {
    _queryId = queryId;
    _tableName = tableName;
    _strategyName = strategyName;
    _triggeringMetric = triggeringMetric;
    _actualValue = actualValue;
    _thresholdValue = thresholdValue;
    _configSource = configSource;
    // Snapshot mutable LongAdder values immediately to decouple from live context
    _snapshotEntriesScannedInFilter = context.getNumEntriesScannedInFilter();
    _snapshotDocsScanned = context.getNumDocsScanned();
    _snapshotEntriesScannedPostFilter = context.getNumEntriesScannedPostFilter();
    _elapsedTimeMs = context.getElapsedTimeMs();
  }

  // ----- Getters -----

  public String getQueryId() {
    return _queryId;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getStrategyName() {
    return _strategyName;
  }

  public String getTriggeringMetric() {
    return _triggeringMetric;
  }

  public long getActualValue() {
    return _actualValue;
  }

  public long getThresholdValue() {
    return _thresholdValue;
  }

  public String getConfigSource() {
    return _configSource;
  }

  public long getSnapshotEntriesScannedInFilter() {
    return _snapshotEntriesScannedInFilter;
  }

  public long getSnapshotDocsScanned() {
    return _snapshotDocsScanned;
  }

  public long getSnapshotEntriesScannedPostFilter() {
    return _snapshotEntriesScannedPostFilter;
  }

  public long getElapsedTimeMs() {
    return _elapsedTimeMs;
  }

  // ----- Message formatters -----

  /**
   * Returns a user-facing message describing why the query was killed and what action to take.
   *
   * <p>Numbers are formatted with commas (e.g. {@code 1,234,567}) for readability.
   * Includes actionable advice about adding a missing index to reduce scan cost.</p>
   */
  public String toCustomerMessage() {
    return String.format(
        "Query '%s' on table '%s' was killed because '%s' (%,d) exceeded the threshold (%,d) "
            + "configured in %s. "
            + "At kill time: entriesScannedInFilter=%,d, docsScanned=%,d, "
            + "entriesScannedPostFilter=%,d, elapsedMs=%d. "
            + "To reduce scan cost, consider adding a missing index (e.g. inverted or range index) "
            + "on the filter columns.",
        _queryId, _tableName, _triggeringMetric, _actualValue, _thresholdValue, _configSource,
        _snapshotEntriesScannedInFilter, _snapshotDocsScanned, _snapshotEntriesScannedPostFilter, _elapsedTimeMs);
  }

  /**
   * Returns a structured log line suitable for grep and alerting pipelines.
   *
   * <p>Format: {@code QUERY_KILLED key=value ...} with plain (non-comma-formatted) numbers.</p>
   */
  public String toInternalLogMessage() {
    return String.format(
        "QUERY_KILLED queryId=%s table=%s strategy=%s metric=%s actual=%d threshold=%d "
            + "configSource=%s entriesScannedInFilter=%d docsScanned=%d "
            + "entriesScannedPostFilter=%d elapsedMs=%d",
        _queryId, _tableName, _strategyName, _triggeringMetric, _actualValue, _thresholdValue,
        _configSource, _snapshotEntriesScannedInFilter, _snapshotDocsScanned,
        _snapshotEntriesScannedPostFilter, _elapsedTimeMs);
  }
}
