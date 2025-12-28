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
package org.apache.pinot.core.data.manager.realtime;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for realtime segment metadata operations.
 */
public class RealtimeSegmentMetadataUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentMetadataUtils.class);
  private static final long STREAM_METADATA_FETCH_TIMEOUT_MS = 5000;

  private RealtimeSegmentMetadataUtils() {
  }

  /**
   * Fetches the latest stream offset for a segment's partition using the provided metadata provider.
   * This encapsulates the common pattern of getting the partition ID from the segment and fetching
   * the offset from the metadata provider.
   *
   * @param realtimeSegmentDataManager The segment data manager to get partition ID from
   * @param streamMetadataProvider The stream metadata provider to use for fetching
   * @return The latest stream offset for the partition, or null if not available
   * @throws RuntimeException if fetching fails
   */
  @Nullable
  public static StreamPartitionMsgOffset fetchLatestStreamOffset(RealtimeSegmentDataManager realtimeSegmentDataManager,
      StreamMetadataProvider streamMetadataProvider) {
    try {
      int partitionId = realtimeSegmentDataManager.getStreamPartitionId();
      Map<Integer, StreamPartitionMsgOffset> partitionMsgOffsetMap =
          streamMetadataProvider.fetchLatestStreamOffset(Collections.singleton(partitionId),
              STREAM_METADATA_FETCH_TIMEOUT_MS);
      return partitionMsgOffsetMap.get(partitionId);
    } catch (Exception e) {
      String segmentName = realtimeSegmentDataManager.getSegmentName();
      LOGGER.error("Failed to fetch latest stream offset for segment: {}", segmentName, e);
      throw new RuntimeException("Failed to fetch latest stream offset for segment: " + segmentName, e);
    }
  }
}
