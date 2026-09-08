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
package org.apache.pinot.query.planner.spi.stats;

import java.util.OptionalLong;
import javax.annotation.Nullable;


/// Planner-facing read API for table and column statistics.
///
/// Implementations MUST be cheap — this interface is called multiple times per query on the
/// planning hot path. Implementations MUST be thread-safe; they expose the merged logical view
/// of a table.
///
/// Table name semantics:
/// - A raw table name (no type suffix) requests the logical hybrid view: implementations must
///   merge OFFLINE and REALTIME statistics at the time boundary without double-counting rows.
/// - A name with a type suffix (e.g. `myTable_OFFLINE`) requests the physical view for
///   that specific table type.
///
/// Thread-safety: implementations must be thread-safe.
public interface PinotStatisticsProvider {

  /// Returns aggregate statistics for the given table, or `null` if no statistics are available.
  ///
  /// @param tableName raw table name (logical hybrid view) or a name with type suffix (physical
  ///                  view)
  @Nullable
  TableStatistics getTableStatistics(String tableName);

  /// Returns per-column statistics for the given table and column, or `null` if no statistics
  /// are available.
  ///
  /// @param tableName  raw table name (logical hybrid view) or a name with type suffix (physical
  ///                   view)
  /// @param columnName name of the column
  @Nullable
  ColumnStatistics getColumnStatistics(String tableName, String columnName);

  /// Returns an estimate of the number of rows whose time column falls in the half-open interval
  /// `[startMs, endMs)`, or an empty optional if the estimate cannot be produced.
  ///
  /// Used for time-predicate selectivity estimation. Implementations may return an empty
  /// optional when time-range metadata is not available.
  ///
  /// @param tableName raw table name or name with type suffix
  /// @param startMs   start of the time range, inclusive, in epoch milliseconds
  /// @param endMs     end of the time range, exclusive, in epoch milliseconds
  default OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    return OptionalLong.empty();
  }
}
