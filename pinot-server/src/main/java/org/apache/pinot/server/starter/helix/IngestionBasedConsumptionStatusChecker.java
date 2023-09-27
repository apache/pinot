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
import java.util.Set;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class IngestionBasedConsumptionStatusChecker {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  // constructor parameters
  protected final InstanceDataManager _instanceDataManager;
  protected final Set<String> _consumingSegments;

  // helper variable
  private final Set<String> _caughtUpSegments = new HashSet<>();

  public IngestionBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Set<String> consumingSegments) {
    _instanceDataManager = instanceDataManager;
    _consumingSegments = consumingSegments;
  }

  public int getNumConsumingSegmentsNotReachedIngestionCriteria() {
    for (String segName : _consumingSegments) {
      if (_caughtUpSegments.contains(segName)) {
        continue;
      }
      TableDataManager tableDataManager = getTableDataManager(segName);
      if (tableDataManager == null) {
        _logger.info("TableDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        continue;
      }
      SegmentDataManager segmentDataManager = null;
      try {
        segmentDataManager = tableDataManager.acquireSegment(segName);
        if (segmentDataManager == null) {
          _logger.info("SegmentDataManager is not yet setup for segment {}. Will check consumption status later",
              segName);
          continue;
        }
        if (!(segmentDataManager instanceof RealtimeSegmentDataManager)) {
          // There's a possibility that a consuming segment has converted to a committed segment. If that's the case,
          // segment data manager will not be of type RealtimeSegmentDataManager.
          _logger.info("Segment {} is already committed and is considered caught up.", segName);
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
    return _consumingSegments.size() - _caughtUpSegments.size();
  }

  protected abstract boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager);

  private TableDataManager getTableDataManager(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableName = llcSegmentName.getTableName();
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    return _instanceDataManager.getTableDataManager(tableNameWithType);
  }
}
