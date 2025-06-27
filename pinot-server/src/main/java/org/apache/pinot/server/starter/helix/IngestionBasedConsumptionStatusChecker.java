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

package org.apache.pinot.server.starter.helix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class IngestionBasedConsumptionStatusChecker {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  private final InstanceDataManager _instanceDataManager;
  private final Map<String, Set<String>> _consumingSegmentsByTable;
  private final Map<String, Set<String>> _caughtUpSegmentsByTable = new HashMap<>();
  private final Function<String, Set<String>> _consumingSegmentsSupplier;

  /**
   * Both consumingSegmentsByTable and consumingSegmentsSupplier are provided as it can be costly to get
   * consumingSegmentsByTable via the supplier, so only use it when any missing segment is detected.
   */
  public IngestionBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Map<String, Set<String>> consumingSegmentsByTable, Function<String, Set<String>> consumingSegmentsSupplier) {
    _instanceDataManager = instanceDataManager;
    _consumingSegmentsByTable = consumingSegmentsByTable;
    _consumingSegmentsSupplier = consumingSegmentsSupplier;
  }

  // This might be called by multiple threads, thus synchronized to be correct.
  public synchronized int getNumConsumingSegmentsNotReachedIngestionCriteria() {
    // If the checker found any consuming segments are missing or committed for a table, it should reset the set of
    // consuming segments for the table to continue to monitor the freshness, otherwise the checker might get stuck
    // on deleted segments or tables, or miss new consuming segments created in the table and get ready prematurely.
    Set<String> tablesToRefresh = new HashSet<>();
    Iterator<Map.Entry<String, Set<String>>> itr = _consumingSegmentsByTable.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<String, Set<String>> tableSegments = itr.next();
      String tableNameWithType = tableSegments.getKey();
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
      if (tableDataManager == null) {
        _logger.info("No tableDataManager for table: {}. Refresh table's consuming segments", tableNameWithType);
        tablesToRefresh.add(tableNameWithType);
        continue;
      }
      Set<String> consumingSegments = tableSegments.getValue();
      Set<String> caughtUpSegments = _caughtUpSegmentsByTable.computeIfAbsent(tableNameWithType, k -> new HashSet<>());
      for (String segName : consumingSegments) {
        if (caughtUpSegments.contains(segName)) {
          continue;
        }
        SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segName);
        if (segmentDataManager == null) {
          _logger.info("No segmentDataManager for segment: {} from table: {}. Refresh table's consuming segments",
              segName, tableNameWithType);
          tablesToRefresh.add(tableNameWithType);
          continue;
        }
        try {
          if (!(segmentDataManager instanceof RealtimeSegmentDataManager)) {
            // It's possible that the consuming segment has been committed by another server. In this case, we should
            // get the new consuming segments for the table and continue to monitor their consumption status, until the
            // current server catches up the consuming segments.
            _logger.info("Segment: {} from table: {} is already committed. Refresh table's consuming segments.",
                segName, tableNameWithType);
            tablesToRefresh.add(tableNameWithType);
            continue;
          }
          RealtimeSegmentDataManager rtSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
          if (isSegmentCaughtUp(segName, rtSegmentDataManager)) {
            caughtUpSegments.add(segName);
          }
        } finally {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
      int numLaggingSegments = consumingSegments.size() - caughtUpSegments.size();
      if (numLaggingSegments == 0) {
        _logger.info("Consuming segments from table: {} have all caught up", tableNameWithType);
        itr.remove();
        _caughtUpSegmentsByTable.remove(tableNameWithType);
      }
    }
    if (!tablesToRefresh.isEmpty()) {
      for (String tableNameWithType : tablesToRefresh) {
        Set<String> updatedConsumingSegments = _consumingSegmentsSupplier.apply(tableNameWithType);
        if (updatedConsumingSegments == null || updatedConsumingSegments.isEmpty()) {
          _consumingSegmentsByTable.remove(tableNameWithType);
          _caughtUpSegmentsByTable.remove(tableNameWithType);
          _logger.info("Found no consuming segments from table: {}, which is probably removed", tableNameWithType);
        } else {
          _consumingSegmentsByTable.put(tableNameWithType, updatedConsumingSegments);
          _caughtUpSegmentsByTable.computeIfAbsent(tableNameWithType, k -> new HashSet<>())
              .retainAll(updatedConsumingSegments);
          _logger.info(
              "Updated consumingSegments: {} and caughtUpSegments: {} for table: {}, as consuming segments were "
                  + "missing or committed", updatedConsumingSegments, _caughtUpSegmentsByTable.get(tableNameWithType),
              tableNameWithType);
        }
      }
    }
    int numLaggingSegments = 0;
    for (Map.Entry<String, Set<String>> tableSegments : _consumingSegmentsByTable.entrySet()) {
      String tableNameWithType = tableSegments.getKey();
      Set<String> consumingSegments = tableSegments.getValue();
      Set<String> caughtUpSegments = _caughtUpSegmentsByTable.computeIfAbsent(tableNameWithType, k -> new HashSet<>());
      numLaggingSegments += consumingSegments.size() - caughtUpSegments.size();
    }
    return numLaggingSegments;
  }

  protected abstract boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager);
}
