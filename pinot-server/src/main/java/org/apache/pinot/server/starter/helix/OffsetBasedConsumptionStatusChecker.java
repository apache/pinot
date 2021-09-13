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
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query execution
 * happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called, {@link #haveAllConsumingSegmentsReachedStreamLatestOffset},
 * for each consuming segment, we check if segment's latest ingested offset has reached the latest stream offset.
 * To prevent chasing a moving target, once the latest stream offset is fetched, it will not be fetched again and
 * subsequent status check calls compare latest ingested offset with the already fetched stream offset.
 */
public class OffsetBasedConsumptionStatusChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetBasedConsumptionStatusChecker.class);

  // constructor parameters
  private final InstanceDataManager _instanceDataManager;
  private final Set<String> _consumingSegments;

  // helper variable
  private final Set<String> _caughtUpSegments = new HashSet<>();

  public OffsetBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager, Set<String> consumingSegments) {
    _instanceDataManager = instanceDataManager;
    _consumingSegments = consumingSegments;
  }

  public boolean haveAllConsumingSegmentsReachedStreamLatestOffset() {
    boolean allSegsReachedLatest = true;
    for (String segName : _consumingSegments) {
      if (_caughtUpSegments.contains(segName)) {
        continue;
      }
      TableDataManager tableDataManager = getTableDataManager(segName);
      if (tableDataManager == null) {
        LOGGER.info("TableDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        return false;
      }
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segName);
      if (segmentDataManager == null) {
        LOGGER.info("SegmentDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        return false;
      }
      if (!(segmentDataManager instanceof LLRealtimeSegmentDataManager)) {
        // There's a possibility that a consuming segment has converted to a committed segment. If that's the case,
        // segment data manager will not be of type LLRealtime.
        LOGGER.info("Segment {} is already committed and is considered caught up.", segName);
        _caughtUpSegments.add(segName);
        releaseSegment(tableDataManager, segmentDataManager);
        continue;
      }
      LLRealtimeSegmentDataManager rtSegmentDataManager = (LLRealtimeSegmentDataManager) segmentDataManager;
      StreamPartitionMsgOffset latestIngestedOffset = rtSegmentDataManager.getCurrentOffset();
      StreamPartitionMsgOffset latestStreamOffset = rtSegmentDataManager.getLatestStreamOffsetAtStartupTime();
      releaseSegment(tableDataManager, segmentDataManager);
      if (latestStreamOffset == null || latestIngestedOffset == null) {
        LOGGER.info("Null offset found for segment {} - latest stream offset: {}, latest ingested offset: {}. "
            + "Will check consumption status later", segName, latestStreamOffset, latestIngestedOffset);
        return false;
      }
      if (latestIngestedOffset.compareTo(latestStreamOffset) < 0) {
        LOGGER.info("Latest ingested offset {} in segment {} is smaller than stream latest available offset {} ",
            latestIngestedOffset, segName, latestStreamOffset);
        allSegsReachedLatest = false;
        continue;
      }
      LOGGER.info("Segment {} with latest ingested offset {} has caught up to the latest stream offset {}", segName,
            latestIngestedOffset, latestStreamOffset);
      _caughtUpSegments.add(segName);
    }
    return allSegsReachedLatest;
  }

  void releaseSegment(TableDataManager tableDataManager, SegmentDataManager segmentDataManager) {
    try {
      tableDataManager.releaseSegment(segmentDataManager);
    } catch (Exception e) {
      LOGGER.error("Cannot release segment data manager for segment " + segmentDataManager.getSegmentName(), e);
    }
  }

  private TableDataManager getTableDataManager(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableName = llcSegmentName.getTableName();
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    return _instanceDataManager.getTableDataManager(tableNameWithType);
  }
}
