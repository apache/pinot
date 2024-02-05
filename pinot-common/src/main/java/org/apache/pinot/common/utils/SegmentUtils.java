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
package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;


// Util functions related to segments.
public class SegmentUtils {
  private SegmentUtils() {
  }

  // Returns the partition id of a realtime segment based segment name and segment metadata info retrieved via Helix.
  // Important: The method is costly because it may read data from zookeeper. Do not use it in any query execution
  // path.
  @Nullable
  public static Integer getRealtimeSegmentPartitionId(String segmentName, String realtimeTableName,
      HelixManager helixManager, @Nullable String partitionColumn) {
    // A fast path if the segmentName is an LLC segment name: get the partition id from the name directly
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      return llcSegmentName.getPartitionGroupId();
    }
    // Otherwise, retrieve the partition id from the segment zk metadata.
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), realtimeTableName, segmentName);
    Preconditions.checkState(segmentZKMetadata != null,
        "Failed to find segment ZK metadata for segment: %s of table: %s", segmentName, realtimeTableName);
    return getRealtimeSegmentPartitionId(segmentZKMetadata, partitionColumn);
  }

  @Nullable
  public static Integer getRealtimeSegmentPartitionId(String segmentName, SegmentZKMetadata segmentZKMetadata,
      @Nullable String partitionColumn) {
    // A fast path if the segmentName is an LLC segment name: get the partition id from the name directly
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      return llcSegmentName.getPartitionGroupId();
    }
    // Otherwise, retrieve the partition id from the segment zk metadata.
    return getRealtimeSegmentPartitionId(segmentZKMetadata, partitionColumn);
  }

  @Nullable
  public static Integer getRealtimeSegmentPartitionId(SegmentZKMetadata segmentZKMetadata,
      @Nullable String partitionColumn) {
    SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
    if (segmentPartitionMetadata != null) {
      Map<String, ColumnPartitionMetadata> columnPartitionMap = segmentPartitionMetadata.getColumnPartitionMap();
      ColumnPartitionMetadata columnPartitionMetadata = null;
      if (partitionColumn != null) {
        columnPartitionMetadata = columnPartitionMap.get(partitionColumn);
      } else {
        if (columnPartitionMap.size() == 1) {
          columnPartitionMetadata = columnPartitionMap.values().iterator().next();
        }
      }
      if (columnPartitionMetadata != null && columnPartitionMetadata.getPartitions().size() == 1) {
        return columnPartitionMetadata.getPartitions().iterator().next();
      }
    }
    return null;
  }

  /**
   * Returns the creation time of a segment based on its ZK metadata. This is the time when the segment is created in
   * the cluster, instead of when the segment file is created.
   * - For uploaded segments, creation time in ZK metadata is the time when the segment file is created, use push time
   * instead. Push time is the first time a segment being uploaded. When a segment is refreshed, push time won't change.
   * - For real-time segments (not uploaded), push time does not exist, use creation time.
   */
  public static long getSegmentCreationTimeMs(SegmentZKMetadata segmentZKMetadata) {
    // Check push time first, then creation time
    long pushTimeMs = segmentZKMetadata.getPushTime();
    if (pushTimeMs > 0) {
      return pushTimeMs;
    }
    return segmentZKMetadata.getCreationTime();
  }
}
