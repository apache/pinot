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

import java.util.List;
import java.util.Map;
import java.util.Set;


/// Source of per-segment column statistics fetched on behalf of the broker: the acquisition side
/// of the statistics subsystem, paired with [StatsStore] (where statistics are kept) and
/// [PinotStatisticsProvider] (how the planner reads them).
///
/// This is an extension point on purpose. A server fan-out, a push feed and a vendor metadata
/// service have very different cost profiles, so the choice of source must not be baked into the
/// collection logic.
///
/// Implementations own their own bounding strategy (rate limits, jitter, debounce) and are
/// responsible for not overwhelming downstream services.
///
/// The result is keyed by segment name; segments for which statistics could not be obtained
/// may be absent from the returned map.
///
/// Thread-safety: implementations must be thread-safe.
public interface ColumnStatsSource {

  /// Fetches per-column statistics for the specified segments of the given table.
  ///
  /// Segments for which statistics are unavailable may be absent from the result map.
  /// Implementations may return a partial result on partial failure.
  ///
  /// @param tableNameWithType fully-qualified table name including type suffix
  /// @param segmentNames      names of the segments for which statistics are requested
  /// @return map from segment name to a list of per-column statistics rows; missing segments are
  ///         absent from the map
  /// @throws ColumnStatsFetchException if fetching fails and no partial result can be returned
  Map<String, List<SegmentColumnStatsRow>> fetchColumnStats(String tableNameWithType,
      Set<String> segmentNames)
      throws ColumnStatsFetchException;
}
