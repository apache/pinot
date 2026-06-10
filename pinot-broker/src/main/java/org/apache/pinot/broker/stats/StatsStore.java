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

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;


/// Durable, broker-local persistence for table and segment statistics used by the cost-based query
/// planner.
///
/// Implementations must support a single writer with concurrent readers. READ failures must be
/// cheap to detect so callers can degrade to a no-stats path; callers must never fail a query
/// because of a store error.
///
/// Lifecycle: call [#init()] once before any other method; call [#close()] to release resources.
///
/// Thread-safety: implementations must be safe for concurrent reads with a single concurrent
/// writer.
public interface StatsStore extends Closeable {

  /// Opens the store and migrates the schema if necessary.
  ///
  /// On unrecoverable corruption, implementations must drop and recreate an empty store rather
  /// than propagating an error.
  ///
  /// @throws StatsStoreException if the store cannot be opened or initialized
  void init()
      throws StatsStoreException;

  /// Inserts or updates segment-level statistics for the given table.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param rows              segment statistics rows to upsert
  /// @throws StatsStoreException if the write fails
  void upsertSegmentStats(String tableNameWithType, List<SegmentStatsRow> rows)
      throws StatsStoreException;

  /// Inserts or updates per-column statistics for individual segments of the given table.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param rows              segment column statistics rows to upsert
  /// @throws StatsStoreException if the write fails
  void upsertSegmentColumnStats(String tableNameWithType, List<SegmentColumnStatsRow> rows)
      throws StatsStoreException;

  /// Removes all stored statistics for the specified segments of the given table.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param segmentNames      names of segments to remove
  /// @throws StatsStoreException if the removal fails
  void removeSegments(String tableNameWithType, Collection<String> segmentNames)
      throws StatsStoreException;

  /// Returns a map from segment name to CRC for all segments of the given table.
  ///
  /// Used for restart reconciliation to detect stale or missing entries.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @return map of segment name to CRC; empty map if no segments are stored
  /// @throws StatsStoreException if the read fails
  Map<String, Long> getSegmentCrcs(String tableNameWithType)
      throws StatsStoreException;

  /// Returns aggregated table-level statistics derived from all non-consuming segments, or
  /// `null` if no statistics are available.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @throws StatsStoreException if the read fails
  @Nullable
  TableStatistics getTableStats(String tableNameWithType)
      throws StatsStoreException;

  /// Returns per-column statistics aggregated across all segments for the given table and column,
  /// or `null` if no statistics are available.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param columnName        name of the column
  /// @throws StatsStoreException if the read fails
  @Nullable
  ColumnStatistics getColumnStats(String tableNameWithType, String columnName)
      throws StatsStoreException;

  /// Returns an estimate of the number of rows whose time column falls in the half-open interval
  /// `[startMs, endMs)`, or an empty optional if the estimate cannot be produced.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param startMs           start of the time range, inclusive, in epoch milliseconds
  /// @param endMs             end of the time range, exclusive, in epoch milliseconds
  /// @throws StatsStoreException if the read fails
  OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
      throws StatsStoreException;

  /// Removes all stored statistics for the given table.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @throws StatsStoreException if the purge fails
  void purgeTable(String tableNameWithType)
      throws StatsStoreException;

  /// Removes all stored statistics for all tables.
  ///
  /// @throws StatsStoreException if the purge fails
  void purgeAll()
      throws StatsStoreException;

  /// Returns `true` if the given table has at least one consuming (REALTIME IN_PROGRESS)
  /// segment in the store.
  ///
  /// Used to detect whether realtime row counts may undercount fresh, un-committed data.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @throws StatsStoreException if the read fails
  boolean hasConsumingSegments(String tableNameWithType)
      throws StatsStoreException;
}
