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

import java.util.Set;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query
 * execution happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called - {@link #getNumConsumingSegmentsNotReachedIngestionCriteria} -
 * for each consuming segment, we check if either:
 *   - the segment's latest ingested offset has reached the current stream offset that's
 *   - the last ingested message is within {@link #_minFreshnessMs} of the current system time
 */
public class FreshnessBasedConsumptionStatusChecker extends IngestionBasedConsumptionStatusChecker {
  private final long _minFreshnessMs;
  private final long _idleTimeoutMs;

  public FreshnessBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager, Set<String> consumingSegments,
      long minFreshnessMs, long idleTimeoutMs) {
    super(instanceDataManager, consumingSegments);
    _minFreshnessMs = minFreshnessMs;
    _idleTimeoutMs = idleTimeoutMs;
  }

  private boolean isOffsetCaughtUp(StreamPartitionMsgOffset currentOffset, StreamPartitionMsgOffset latestOffset) {
    if (currentOffset != null && latestOffset != null) {
      // Kafka's "latest" offset is actually the next available offset. Therefore it will be 1 ahead of the
      // current offset in the case we are caught up.
      // TODO: implement a way to have this work correctly for kafka consumers
      return currentOffset.compareTo(latestOffset) >= 0;
    }
    return false;
  }

  private boolean segmentHasBeenIdleLongerThanThreshold(long segmentIdleTime) {
    return _idleTimeoutMs > 0 && segmentIdleTime > _idleTimeoutMs;
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  @Override
  protected boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager) {
    long now = now();
    long latestIngestionTimestamp =
        rtSegmentDataManager.getSegment().getSegmentMetadata().getLatestIngestionTimestamp();
    long freshnessMs = now - latestIngestionTimestamp;

    // We check latestIngestionTimestamp >= 0 because the default freshness when unknown is Long.MIN_VALUE
    if (latestIngestionTimestamp >= 0 && freshnessMs <= _minFreshnessMs) {
      _logger.info("Segment {} with freshness {}ms has caught up within min freshness {}", segmentName, freshnessMs,
          _minFreshnessMs);
      return true;
    }

    // For stream partitions that see very low volume, it's possible we're already caught up but the oldest
    // message is too old to pass the freshness check. We check this condition separately to avoid hitting
    // the stream consumer to check partition count if we're already caught up.
    StreamPartitionMsgOffset currentOffset = rtSegmentDataManager.getCurrentOffset();
    StreamPartitionMsgOffset latestStreamOffset = rtSegmentDataManager.fetchLatestStreamOffset(5000);
    if (isOffsetCaughtUp(currentOffset, latestStreamOffset)) {
      _logger.info("Segment {} with freshness {}ms has not caught up within min freshness {}. "
              + "But the current ingested offset is equal to the latest available offset {}.", segmentName, freshnessMs,
          _minFreshnessMs, currentOffset);
      return true;
    }

    StreamPartitionMsgOffset earliestStreamOffset = rtSegmentDataManager.fetchEarliestStreamOffset(5000);

    long idleTimeMs = rtSegmentDataManager.getTimeSinceEventLastConsumedMs();
    if (segmentHasBeenIdleLongerThanThreshold(idleTimeMs)) {
      _logger.warn("Segment {} with freshness {}ms has not caught up within min freshness {}. "
              + "But the current ingested offset {} has been idle for {}ms. At offset {}. Earliest offset {}. "
              + "Latest offset {}.", segmentName, freshnessMs, _minFreshnessMs, currentOffset, idleTimeMs,
          currentOffset,
          earliestStreamOffset, latestStreamOffset);
      return true;
    }

    _logger.info("Segment {} with freshness {}ms has not caught up within "
            + "min freshness {}. At offset {}. Earliest offset {}. Latest offset {}.", segmentName, freshnessMs,
        _minFreshnessMs, currentOffset, earliestStreamOffset, latestStreamOffset);
    return false;
  }
}
