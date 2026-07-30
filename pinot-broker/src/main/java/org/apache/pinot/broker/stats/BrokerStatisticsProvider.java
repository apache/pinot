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
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;


/// [PinotStatisticsProvider] backed by the broker's [BrokerTableStatsManager].
///
/// All calls delegate directly to the manager. The manager already handles the
/// disabled/error cases by returning `null` or empty, so this class is a
/// thin adapter with no additional logic.
///
/// Thread-safety: unconditionally thread-safe. All state is held in the immutable
/// reference to `_statsManager`, which is itself thread-safe.
public class BrokerStatisticsProvider implements PinotStatisticsProvider {

  private final BrokerTableStatsManager _statsManager;

  /// Constructs a provider backed by the given [BrokerTableStatsManager].
  ///
  /// @param statsManager the broker stats manager; must not be `null`
  public BrokerStatisticsProvider(BrokerTableStatsManager statsManager) {
    _statsManager = statsManager;
  }

  @Nullable
  @Override
  public TableStatistics getTableStatistics(String tableName) {
    return _statsManager.getTableStats(tableName);
  }

  /// Column-level statistics are not yet collected in T1 of the CBO initiative.
  /// Returns `null` unconditionally; will be implemented in a future task.
  @Nullable
  @Override
  public ColumnStatistics getColumnStatistics(String tableName, String columnName) {
    return null;
  }

  @Override
  public OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    return _statsManager.estimateRowsInTimeRange(tableName, startMs, endMs);
  }
}
