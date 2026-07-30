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
package org.apache.pinot.broker.stats;

import java.util.OptionalLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Turns raw physical [StatsStore] data into honest logical statistics for the
/// cost-based query planner.
///
/// ### Name semantics
/// - A *suffixed* name (`foo_OFFLINE` / `foo_REALTIME`) requests the physical view for that
///   specific table type, with per-type confidence adjustments.
/// - A *raw* name (`foo`) requests the logical hybrid view: stats from both physical tables
///   are merged at the time boundary, avoiding double-counting.
///
/// ### Per-type confidence adjustments
/// 1. **Upsert / dedup tables (REALTIME):** physical doc count over-counts logical rows →
///    row-count confidence downgraded to [StatConfidence#LOW].
/// 1. **Consuming segments:** realtime row count excludes in-progress rows → row-count
///    confidence downgraded to [StatConfidence#ESTIMATED] (not LOW: bias is bounded).
///    LOW wins over ESTIMATED if both apply.
///
/// ### Hybrid merge
/// Row count is split at the time boundary:
/// `offline.estimateRows([MIN, boundary)) + realtime.estimateRows([boundary, MAX))`.
/// If no boundary is available, a plain sum is used with [StatConfidence#ESTIMATED]
/// (may double-count the overlap window — acceptable for cost purposes).
/// Table size is always a plain sum (bytes double-count is acceptable for cost purposes).
/// Merged confidence is the weakest of the two sides
/// (LOW > ESTIMATED > EXACT in weakness order).
///
/// ### Thread-safety
/// Read methods are safe for concurrent access. The two setters
/// ([#setTimeBoundaryMsProvider] and [#setTableConfigProvider]) are intended to be
/// called once during broker startup, before any query traffic, from a single thread.
public class LogicalTableStatsResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableStatsResolver.class);

  private final StatsStore _statsStore;

  /// Optional provider of the time-boundary in epoch milliseconds for a raw table name.
  /// Returns `null` when no boundary is available for the given table.
  /// Injected after the routing manager is fully initialized.
  @Nullable
  private volatile Function<String, Long> _timeBoundaryMsProvider;

  /// Optional provider of the [TableConfig] for a fully-qualified (suffixed) table name.
  /// Returns `null` when the config is not available.
  /// Injected after the table cache is initialized.
  @Nullable
  private volatile Function<String, TableConfig> _tableConfigProvider;

  /// Constructs a new resolver backed by the given [StatsStore].
  ///
  /// @param statsStore the physical stats store to read from
  public LogicalTableStatsResolver(StatsStore statsStore) {
    _statsStore = statsStore;
  }

  // ---------------------------------------------------------------------------
  // Post-construction wiring
  // ---------------------------------------------------------------------------

  /// Sets the provider used to look up the time boundary (in epoch milliseconds) for a raw table
  /// name. A `null` return value means no boundary is available.
  ///
  /// Must be called at most once, before query traffic starts.
  ///
  /// @param provider function from raw table name to boundary epoch-milliseconds, or `null`
  public void setTimeBoundaryMsProvider(@Nullable Function<String, Long> provider) {
    _timeBoundaryMsProvider = provider;
  }

  /// Sets the provider used to look up the [TableConfig] for a fully-qualified
  /// (type-suffixed) table name. A `null` return value means the config is unavailable.
  ///
  /// Must be called at most once, before query traffic starts.
  ///
  /// @param provider function from suffixed table name to TableConfig, or `null`
  public void setTableConfigProvider(@Nullable Function<String, TableConfig> provider) {
    _tableConfigProvider = provider;
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /// Returns aggregate statistics for the given table, applying all logical adjustments.
  /// Returns `null` if no statistics are available.
  ///
  /// @param tableName raw (logical hybrid) or suffixed (physical) table name
  @Nullable
  public TableStatistics getTableStats(String tableName) {
    TableType type = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (type != null) {
      // Suffixed physical lookup
      return getPhysicalTableStats(tableName, type);
    }
    // Raw name — hybrid logical view
    return getHybridTableStats(tableName);
  }

  /// Returns an estimate of the number of rows whose time column falls in
  /// `[startMs, endMs)`, applying the time-boundary split for hybrid tables.
  /// Returns an empty optional if the estimate cannot be produced.
  ///
  /// @param tableName raw or suffixed table name
  /// @param startMs   inclusive start in epoch milliseconds
  /// @param endMs     exclusive end in epoch milliseconds
  public OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    TableType type = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (type != null) {
      // Physical table — delegate directly
      return storeEstimate(tableName, startMs, endMs);
    }
    // Raw/hybrid — split at boundary
    String offlineTable = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTable = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    Long boundaryMs = timeBoundaryMs(tableName);
    if (boundaryMs != null) {
      // Offline: [startMs, min(endMs, boundary))
      long offEnd = Math.min(endMs, boundaryMs);
      OptionalLong offlineRows = startMs < offEnd ? storeEstimate(offlineTable, startMs, offEnd)
          : OptionalLong.of(0L);
      // Realtime: [max(startMs, boundary), endMs)
      long rtStart = Math.max(startMs, boundaryMs);
      OptionalLong realtimeRows = rtStart < endMs ? storeEstimate(realtimeTable, rtStart, endMs)
          : OptionalLong.of(0L);
      if (!offlineRows.isPresent() && !realtimeRows.isPresent()) {
        return OptionalLong.empty();
      }
      return OptionalLong.of(offlineRows.orElse(0L) + realtimeRows.orElse(0L));
    }
    // No boundary — plain sum over full range
    OptionalLong offline = storeEstimate(offlineTable, startMs, endMs);
    OptionalLong realtime = storeEstimate(realtimeTable, startMs, endMs);
    if (!offline.isPresent() && !realtime.isPresent()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(offline.orElse(0L) + realtime.orElse(0L));
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /// Returns physical stats for a suffixed table name, applying confidence adjustments.
  @Nullable
  private TableStatistics getPhysicalTableStats(String tableNameWithType, TableType type) {
    TableStatistics raw;
    try {
      raw = _statsStore.getTableStats(tableNameWithType);
    } catch (StatsStoreException e) {
      LOGGER.warn("Failed to read physical stats for {}: {}", tableNameWithType, e.getMessage());
      return null;
    }
    if (raw == null) {
      return null;
    }
    StatConfidence rowConf = raw.getRowCountConfidence();
    if (type == TableType.REALTIME) {
      rowConf = applyRealtimeAdjustments(tableNameWithType, rowConf);
    }
    if (rowConf == raw.getRowCountConfidence()) {
      return raw;
    }
    return TableStatistics.builder()
        .rowCount(raw.getRowCount(), rowConf)
        .tableSizeBytes(raw.getTableSizeBytes(), raw.getSizeConfidence())
        .updatedAtMs(raw.getUpdatedAtMs())
        .build();
  }

  /// Returns the logical hybrid stats for a raw (un-suffixed) table name.
  @Nullable
  private TableStatistics getHybridTableStats(String rawTableName) {
    String offlineTable = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String realtimeTable = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);

    TableStatistics offlineRaw = readPhysicalRaw(offlineTable);
    TableStatistics realtimeRaw = readPhysicalRaw(realtimeTable);

    if (offlineRaw == null && realtimeRaw == null) {
      return null;
    }

    // Apply per-type adjustments before merging
    StatConfidence offlineRowConf =
        offlineRaw != null ? offlineRaw.getRowCountConfidence() : null;
    StatConfidence realtimeRowConf =
        realtimeRaw != null ? applyRealtimeAdjustments(realtimeTable,
            realtimeRaw.getRowCountConfidence()) : null;

    if (offlineRaw == null) {
      // Only realtime
      return rebuildWithConf(realtimeRaw, realtimeRowConf, realtimeRaw.getSizeConfidence());
    }
    if (realtimeRaw == null) {
      // Only offline
      return rebuildWithConf(offlineRaw, offlineRowConf, offlineRaw.getSizeConfidence());
    }

    // Both sides present — merge at time boundary
    Long boundaryMs = timeBoundaryMs(rawTableName);
    long mergedRowCount;
    if (boundaryMs != null) {
      // Split: offline rows < boundary, realtime rows >= boundary
      OptionalLong offlineRows = storeEstimate(offlineTable, Long.MIN_VALUE, boundaryMs);
      OptionalLong realtimeRows = storeEstimate(realtimeTable, boundaryMs, Long.MAX_VALUE);

      // Fall back to full table count when interpolation yields nothing for a side
      long offlineCount =
          offlineRows.isPresent() ? offlineRows.getAsLong() : offlineRaw.getRowCount();
      long realtimeCount =
          realtimeRows.isPresent() ? realtimeRows.getAsLong() : realtimeRaw.getRowCount();
      mergedRowCount = safeAdd(offlineCount, realtimeCount);
    } else {
      // No boundary — plain sum; may double-count the overlap window (acceptable for cost)
      LOGGER.debug("No time boundary for hybrid table {}; using plain row-count sum "
          + "(may double-count overlap window)", rawTableName);
      mergedRowCount = safeAdd(offlineRaw.getRowCount(), realtimeRaw.getRowCount());
    }

    // Merged confidence is the weakest of both row confidences; always at most ESTIMATED
    // (time-boundary split or plain sum both introduce approximation)
    StatConfidence mergedRowConf = weakest(
        weakest(offlineRowConf, realtimeRowConf),
        StatConfidence.ESTIMATED);

    // Table size: plain sum; double-count is acceptable for cost purposes
    long mergedSizeBytes = safeAdd(offlineRaw.getTableSizeBytes(), realtimeRaw.getTableSizeBytes());
    long updatedAtMs = Math.max(offlineRaw.getUpdatedAtMs(), realtimeRaw.getUpdatedAtMs());

    return TableStatistics.builder()
        .rowCount(mergedRowCount, mergedRowConf)
        .tableSizeBytes(mergedSizeBytes, StatConfidence.ESTIMATED)
        .updatedAtMs(updatedAtMs)
        .build();
  }

  /// Applies REALTIME-specific confidence adjustments (upsert/dedup → LOW; consuming → ESTIMATED).
  /// LOW wins over ESTIMATED.
  private StatConfidence applyRealtimeAdjustments(String realtimeTableName,
      StatConfidence baseConf) {
    StatConfidence conf = baseConf;

    // Upsert / dedup: physical doc count over-counts logical rows
    if (isUpsertOrDedup(realtimeTableName)) {
      conf = weakest(conf, StatConfidence.LOW);
    }

    // Consuming segments: row count excludes in-progress rows → undercount
    if (hasConsumingSegments(realtimeTableName)) {
      conf = weakest(conf, StatConfidence.ESTIMATED);
    }

    return conf;
  }

  /// Returns true when the table has upsert or dedup enabled.
  private boolean isUpsertOrDedup(String realtimeTableName) {
    Function<String, TableConfig> provider = _tableConfigProvider;
    if (provider == null) {
      return false;
    }
    try {
      TableConfig cfg = provider.apply(realtimeTableName);
      if (cfg == null) {
        return false;
      }
      // Upsert: config present and mode != NONE
      if (cfg.getUpsertConfig() != null
          && cfg.getUpsertConfig().getMode()
          != org.apache.pinot.spi.config.table.UpsertConfig.Mode.NONE) {
        return true;
      }
      // Dedup: config present and enabled
      if (cfg.getDedupConfig() != null && cfg.getDedupConfig().isDedupEnabled()) {
        return true;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not determine upsert/dedup status for {}: {}", realtimeTableName,
          e.getMessage());
    }
    return false;
  }

  /// Returns true when the store has at least one consuming segment for this table.
  private boolean hasConsumingSegments(String tableNameWithType) {
    try {
      return _statsStore.hasConsumingSegments(tableNameWithType);
    } catch (StatsStoreException e) {
      LOGGER.debug("Could not check consuming segments for {}: {}", tableNameWithType,
          e.getMessage());
      return false;
    }
  }

  /// Returns the time boundary in epoch-milliseconds for `rawTableName`, or `null`.
  @Nullable
  private Long timeBoundaryMs(String rawTableName) {
    Function<String, Long> provider = _timeBoundaryMsProvider;
    if (provider == null) {
      return null;
    }
    try {
      return provider.apply(rawTableName);
    } catch (Exception e) {
      LOGGER.debug("Could not obtain time boundary for {}: {}", rawTableName, e.getMessage());
      return null;
    }
  }

  /// Reads raw physical stats from the store, suppressing errors.
  @Nullable
  private TableStatistics readPhysicalRaw(String tableNameWithType) {
    try {
      return _statsStore.getTableStats(tableNameWithType);
    } catch (StatsStoreException e) {
      LOGGER.warn("Failed to read physical stats for {}: {}", tableNameWithType, e.getMessage());
      return null;
    }
  }

  /// Delegates to store.estimateRowsInTimeRange, suppressing errors.
  private OptionalLong storeEstimate(String tableNameWithType, long startMs, long endMs) {
    try {
      return _statsStore.estimateRowsInTimeRange(tableNameWithType, startMs, endMs);
    } catch (StatsStoreException e) {
      LOGGER.warn("Failed to estimate rows in time range for {}: {}", tableNameWithType,
          e.getMessage());
      return OptionalLong.empty();
    }
  }

  /// Rebuilds a [TableStatistics] with potentially updated row-count confidence.
  /// Returns the original instance unchanged when the confidence is already correct.
  private static TableStatistics rebuildWithConf(TableStatistics original,
      StatConfidence rowConf, StatConfidence sizeConf) {
    if (rowConf == original.getRowCountConfidence() && sizeConf == original.getSizeConfidence()) {
      return original;
    }
    return TableStatistics.builder()
        .rowCount(original.getRowCount(), rowConf)
        .tableSizeBytes(original.getTableSizeBytes(), sizeConf)
        .updatedAtMs(original.getUpdatedAtMs())
        .build();
  }

  /// Returns the weaker of two confidence levels.
  /// Weakness order (strongest→weakest): EXACT, ESTIMATED, LOW, UNKNOWN.
  static StatConfidence weakest(StatConfidence a, StatConfidence b) {
    return weaknessOrdinal(a) >= weaknessOrdinal(b) ? a : b;
  }

  private static int weaknessOrdinal(StatConfidence c) {
    switch (c) {
      case EXACT:
        return 0;
      case ESTIMATED:
        return 1;
      case LOW:
        return 2;
      case UNKNOWN:
        return 3;
      default:
        return 3;
    }
  }

  /// Adds two row/byte counts, treating `-1` (unknown) as 0 for the purposes of summation.
  /// Returns -1 only when both inputs are -1.
  private static long safeAdd(long a, long b) {
    if (a < 0 && b < 0) {
      return -1;
    }
    return Math.max(a, 0L) + Math.max(b, 0L);
  }
}
