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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class IngestionBasedConsumptionStatusChecker {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  // constructor parameters
  protected final InstanceDataManager _instanceDataManager;
  protected final Map<String, Set<String>> _consumingSegments;
  protected final Function<String, Set<String>> _consumingSegmentsSupplier;

  // helper variable, which is thread safe, as the method might be called from multiple threads when the health check
  // endpoint is called by many probes.
  private final Set<String> _caughtUpSegments = ConcurrentHashMap.newKeySet();

  /**
   * Both consumingSegments and consumingSegmentsSupplier are provided as it can be costly to get consumingSegments
   * via the supplier, so only use it when any missing segment is detected.
   */
  public IngestionBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Map<String, Set<String>> consumingSegments, @Nullable Function<String, Set<String>> consumingSegmentsSupplier) {
    _instanceDataManager = instanceDataManager;
    _consumingSegments = new ConcurrentHashMap<>(consumingSegments);
    _consumingSegmentsSupplier = consumingSegmentsSupplier;
  }

  public int getNumConsumingSegmentsNotReachedIngestionCriteria() {
    Set<String> tablesWithMissingSegment = new HashSet<>();
    for (Map.Entry<String, Set<String>> tableSegments : _consumingSegments.entrySet()) {
      String tableNameWithType = tableSegments.getKey();
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
      if (tableDataManager == null) {
        _logger.info("No tableDataManager for table: {}. Will check consumption status later", tableNameWithType);
        tablesWithMissingSegment.add(tableNameWithType);
        continue;
      }
      for (String segName : tableSegments.getValue()) {
        if (_caughtUpSegments.contains(segName)) {
          continue;
        }
        SegmentDataManager segmentDataManager = null;
        try {
          segmentDataManager = tableDataManager.acquireSegment(segName);
          if (segmentDataManager == null) {
            _logger.info("No SegmentDataManager for segment: {}. Will check consumption status later", segName);
            tablesWithMissingSegment.add(tableNameWithType);
            continue;
          }
          if (!(segmentDataManager instanceof RealtimeSegmentDataManager)) {
            // There's a possibility that a consuming segment has converted to a committed segment. If that's the case,
            // segment data manager will not be of type RealtimeSegmentDataManager.
            _logger.info("Segment: {} is already committed and is considered caught up.", segName);
            _caughtUpSegments.add(segName);
            continue;
          }
          RealtimeSegmentDataManager rtSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
          if (isSegmentCaughtUp(segName, rtSegmentDataManager)) {
            _caughtUpSegments.add(segName);
          }
        } finally {
          if (segmentDataManager != null) {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      }
    }
    if (!tablesWithMissingSegment.isEmpty() && _consumingSegmentsSupplier != null) {
      for (String tableName : tablesWithMissingSegment) {
        Set<String> consumingSegments = _consumingSegmentsSupplier.apply(tableName);
        if (consumingSegments == null || consumingSegments.isEmpty()) {
          _consumingSegments.remove(tableName);
        } else {
          _consumingSegments.put(tableName, consumingSegments);
        }
        _logger.info("Found missing segments in table: {}. Updated its consumingSegments: {}", tableName,
            consumingSegments);
      }
    }
    Set<String> currentConsumingSegments = new HashSet<>();
    _consumingSegments.forEach((k, v) -> currentConsumingSegments.addAll(v));
    _caughtUpSegments.retainAll(currentConsumingSegments);
    return currentConsumingSegments.size() - _caughtUpSegments.size();
  }

  protected abstract boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager);
}
