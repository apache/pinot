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
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query execution
 * happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called - {@link #getNumConsumingSegmentsNotReachedIngestionCriteria} -
 * for each consuming segment, we check if segment's latest ingested offset has reached the latest stream offset that's
 * fetched once at startup time.
 */
public class OffsetBasedConsumptionStatusChecker extends IngestionBasedConsumptionStatusChecker {

  public OffsetBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager, Set<String> consumingSegments) {
    super(instanceDataManager, consumingSegments);
  }

  @Override
  protected boolean isSegmentCaughtUp(String segmentName, RealtimeSegmentDataManager rtSegmentDataManager) {
    StreamPartitionMsgOffset latestIngestedOffset = rtSegmentDataManager.getCurrentOffset();
    StreamPartitionMsgOffset latestStreamOffset = rtSegmentDataManager.getLatestStreamOffsetAtStartupTime();
    if (latestStreamOffset == null || latestIngestedOffset == null) {
      _logger.info("Null offset found for segment {} - latest stream offset: {}, latest ingested offset: {}. "
          + "Will check consumption status later", segmentName, latestStreamOffset, latestIngestedOffset);
      return false;
    }
    if (latestIngestedOffset.compareTo(latestStreamOffset) < 0) {
      _logger.info("Latest ingested offset {} in segment {} is smaller than stream latest available offset {} ",
          latestIngestedOffset, segmentName, latestStreamOffset);
      return false;
    }
    _logger.info("Segment {} with latest ingested offset {} has caught up to the latest stream offset {}", segmentName,
        latestIngestedOffset, latestStreamOffset);
    return true;
  }
}
