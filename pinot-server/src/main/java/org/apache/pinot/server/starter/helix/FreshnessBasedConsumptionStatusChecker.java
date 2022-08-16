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
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query execution
 * happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called - {@link #getNumConsumingSegmentsNotReachedMinFreshness} -
 * for each consuming segment, we check if either:
 *   - the segment's latest ingested offset has reached the current stream offset that's
 *   - the last ingested message is within {@link #_minFreshnessMs} of the current system time
 */
public class FreshnessBasedConsumptionStatusChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(FreshnessBasedConsumptionStatusChecker.class);

  // constructor parameters
  private final InstanceDataManager _instanceDataManager;
  private final Set<String> _consumingSegments;
  private final Long _minFreshnessMs;

  // helper variable
  private final Set<String> _caughtUpSegments = new HashSet<>();

  public FreshnessBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager, Set<String> consumingSegments,
      Long minFreshnessMs) {
    _instanceDataManager = instanceDataManager;
    _consumingSegments = consumingSegments;
    _minFreshnessMs = minFreshnessMs;
  }

  private boolean isOffsetCaughtUp(StreamPartitionMsgOffset currentOffset, StreamPartitionMsgOffset latestOffset) {
    if (currentOffset != null && latestOffset != null) {
      if (currentOffset.compareTo(latestOffset) == 0) {
        return true;
      }
      long currentOffsetLong = ((LongMsgOffset) currentOffset).getOffset();
      long latestOffsetLong = ((LongMsgOffset) latestOffset).getOffset();
      // Kafka's "latest" offset is actually the next available offset. Therefore it will be 1 ahead of the
      // current offset in the case we are caught up.
      // We expect currentOffset == latestOffset if no messages have ever been published. Both will be 0.
      // Otherwise, we never expect currentOffset > latestOffset, but we allow this to be caught up in case
      // it ever happens so we're not stuck starting up.
      return currentOffsetLong >= latestOffsetLong - 1;
    }
    return false;
  }

  protected Long now() {
    return System.currentTimeMillis();
  }

  public int getNumConsumingSegmentsNotReachedMinFreshness() {
    for (String segName : _consumingSegments) {
      if (_caughtUpSegments.contains(segName)) {
        continue;
      }
      TableDataManager tableDataManager = getTableDataManager(segName);
      if (tableDataManager == null) {
        LOGGER.info("TableDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        continue;
      }
      SegmentDataManager segmentDataManager = null;
      try {
        segmentDataManager = tableDataManager.acquireSegment(segName);
        if (segmentDataManager == null) {
          LOGGER.info("SegmentDataManager is not yet setup for segment {}. Will check consumption status later",
              segName);
          continue;
        }
        if (!(segmentDataManager instanceof LLRealtimeSegmentDataManager)) {
          // There's a possibility that a consuming segment has converted to a committed segment. If that's the case,
          // segment data manager will not be of type LLRealtime.
          LOGGER.info("Segment {} is already committed and is considered caught up.", segName);
          _caughtUpSegments.add(segName);
          continue;
        }
        LLRealtimeSegmentDataManager rtSegmentDataManager = (LLRealtimeSegmentDataManager) segmentDataManager;
        Long now = now();
        Long latestIngestionTimestamp =
            rtSegmentDataManager.getSegment().getSegmentMetadata().getLatestIngestionTimestamp();
        Long freshnessMs = now - latestIngestionTimestamp;

        // We check latestIngestionTimestamp >= 0 because the default freshness when unknown is Long.MIN_VALUE
        if (latestIngestionTimestamp >= 0 && freshnessMs <= _minFreshnessMs) {
          LOGGER.info("Segment {} with freshness {}ms has caught up within min freshness {}", segName, freshnessMs,
              _minFreshnessMs);
          _caughtUpSegments.add(segName);
          continue;
        }

        // For stream partitions that see very low volume, it's possible we're already caught up but the oldest
        // message is too old to pass the freshness check. We check this condition separately to avoid hitting
        // the stream consumer to check partition count if we're already caught up.
        StreamPartitionMsgOffset currentOffset = rtSegmentDataManager.getCurrentOffset();
        StreamPartitionMsgOffset latestStreamOffset = rtSegmentDataManager.fetchLatestStreamOffset(5000L);
        if (isOffsetCaughtUp(currentOffset, latestStreamOffset)) {
          LOGGER.info(
              "Segment {} with freshness {}ms has not caught up within min freshness {}. But the current ingested offset is equal to the latest available offset {}.",
              segName, freshnessMs, _minFreshnessMs, currentOffset);
          _caughtUpSegments.add(segName);
          continue;
        }

        LOGGER.info(
            "Segment {} with freshness {}ms has not caught up within min freshness {}. At offset {}. Latest offset {}.",
            segName, freshnessMs, _minFreshnessMs, currentOffset, latestStreamOffset);
      } finally {
        if (segmentDataManager != null) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return _consumingSegments.size() - _caughtUpSegments.size();
  }

  private TableDataManager getTableDataManager(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableName = llcSegmentName.getTableName();
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    return _instanceDataManager.getTableDataManager(tableNameWithType);
  }
}
