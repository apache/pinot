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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentMetadataUtils;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query
 * execution happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called - {@link #getNumConsumingSegmentsNotReachedIngestionCriteria} -
 * for each consuming segment, we check if either:
 *   - the segment's minimum ingestion lag is within {@link #_minFreshnessMs}
 *   - the segment's latest ingested offset has reached the current stream offset
 *   - the segment has been idle longer than {@link #_idleTimeoutMs}
 */
public class FreshnessBasedConsumptionStatusChecker extends IngestionBasedConsumptionStatusChecker {
  private final long _minFreshnessMs;
  private final long _idleTimeoutMs;

  public FreshnessBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Map<String, Set<String>> consumingSegments, Function<String, Set<String>> consumingSegmentsSupplier,
      long minFreshnessMs, long idleTimeoutMs) {
    super(instanceDataManager, consumingSegments, consumingSegmentsSupplier);
    _minFreshnessMs = minFreshnessMs;
    _idleTimeoutMs = idleTimeoutMs;
    _logger.info("FreshnessBasedConsumptionStatusChecker initialized with min_freshness={}ms, idle_timeout={}ms",
        _minFreshnessMs, _idleTimeoutMs);
  }

  private boolean segmentHasBeenIdleLongerThanThreshold(long segmentIdleTime) {
    return _idleTimeoutMs > 0 && segmentIdleTime > _idleTimeoutMs;
  }

  @Override
  protected boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager,
      RealtimeTableDataManager realtimeTableDataManager) {
    SegmentMetadata segmentMetadata = rtSegmentDataManager.getSegment().getSegmentMetadata();
    long minimumIngestionLagMs = segmentMetadata.getMinimumIngestionLagMs();

    // Simple freshness check - if minimum lag ever seen is within threshold, we're caught up.
    // Note: default value is Long.MAX_VALUE when unknown, which will correctly fail this check.
    if (minimumIngestionLagMs <= _minFreshnessMs) {
      _logger.info("Segment {} with minimum ingestion lag {}ms has caught up within min freshness {}ms",
          segmentName, minimumIngestionLagMs, _minFreshnessMs);
      return true;
    }

    // Fallback: Check offset catchup.
    // message is too old to pass the freshness check. We check this condition separately to avoid hitting
    // the stream consumer to check partition count if we're already caught up.
    StreamPartitionMsgOffset currentOffset = rtSegmentDataManager.getCurrentOffset();

    StreamPartitionMsgOffset latestStreamOffset = null;
    try {
      StreamMetadataProvider streamMetadataProvider =
          realtimeTableDataManager.getStreamMetadataProvider(rtSegmentDataManager);
      latestStreamOffset =
          RealtimeSegmentMetadataUtils.fetchLatestStreamOffset(rtSegmentDataManager, streamMetadataProvider);
    } catch (Exception e) {
      _logger.warn("Failed to fetch latest stream offset for segment: {}. Will continue with other checks.",
          segmentName, e);
    }

    if (isOffsetCaughtUp(segmentName, currentOffset, latestStreamOffset)) {
      _logger.info("Segment {} with minimum ingestion lag {}ms has not caught up within min freshness {}ms. "
              + "But the current ingested offset is equal to the latest available offset {}.",
          segmentName, minimumIngestionLagMs, _minFreshnessMs, currentOffset);
      return true;
    }

    // Fallback: Check idle timeout.
    long idleTimeMs = rtSegmentDataManager.getTimeSinceEventLastConsumedMs();
    if (segmentHasBeenIdleLongerThanThreshold(idleTimeMs)) {
      _logger.warn("Segment {} with minimum ingestion lag {}ms has not caught up within min freshness {}ms. "
              + "But has been idle for {}ms. At offset {}. Latest offset {}.",
          segmentName, minimumIngestionLagMs, _minFreshnessMs, idleTimeMs, currentOffset, latestStreamOffset);
      return true;
    }

    _logger.info("Segment {} with minimum ingestion lag {}ms has not caught up within min freshness {}ms. "
            + "At offset {}. Latest offset {}. Idle time ms {}",
        segmentName, minimumIngestionLagMs, _minFreshnessMs, currentOffset, latestStreamOffset, idleTimeMs);
    return false;
  }
}
