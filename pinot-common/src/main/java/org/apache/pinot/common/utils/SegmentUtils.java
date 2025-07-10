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

import com.google.common.annotations.VisibleForTesting;
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

  /// Returns the partition id of a segment based on segment name or ZK metadata.
  /// Can return `null` if the partition id cannot be determined.
  /// Important: The method is costly because it may read data from zookeeper. Do not use it in query execution path.
  @Nullable
  public static Integer getSegmentPartitionId(String segmentName, String tableNameWithType, HelixManager helixManager,
      @Nullable String partitionColumn) {
    // Try to get the partition id from the segment name first.
    Integer partitionId = getPartitionIdFromSegmentName(segmentName);
    if (partitionId != null) {
      return partitionId;
    }
    // Otherwise, retrieve the partition id from the segment zk metadata.
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), tableNameWithType, segmentName);
    Preconditions.checkState(segmentZKMetadata != null,
        "Failed to find segment ZK metadata for segment: %s of table: %s", segmentName, tableNameWithType);
    return getPartitionIdFromSegmentZKMetadata(segmentZKMetadata, partitionColumn);
  }

  /// Returns the partition id of a segment based on segment name or ZK metadata.
  /// Can return `null` if the partition id cannot be determined.
  @Nullable
  public static Integer getSegmentPartitionId(SegmentZKMetadata segmentZKMetadata, @Nullable String partitionColumn) {
    // Try to get the partition id from the segment name first.
    Integer partitionId = getPartitionIdFromSegmentName(segmentZKMetadata.getSegmentName());
    if (partitionId != null) {
      return partitionId;
    }
    // Otherwise, retrieve the partition id from the segment zk metadata.
    return getPartitionIdFromSegmentZKMetadata(segmentZKMetadata, partitionColumn);
  }

  /// Returns the partition id of a segment based on segment name.
  /// Can return `null` if the partition id cannot be determined.
  @Nullable
  public static Integer getPartitionIdFromSegmentName(String segmentName) {
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      return llcSegmentName.getPartitionGroupId();
    }
    UploadedRealtimeSegmentName uploadedRealtimeSegmentName = UploadedRealtimeSegmentName.of(segmentName);
    if (uploadedRealtimeSegmentName != null) {
      return uploadedRealtimeSegmentName.getPartitionId();
    }
    return null;
  }

  /// Returns the partition id of a segment based on segment ZK metadata.
  /// Can return `null` if the partition id cannot be determined.
  @Nullable
  private static Integer getPartitionIdFromSegmentZKMetadata(SegmentZKMetadata segmentZKMetadata,
      @Nullable String partitionColumn) {
    SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
    return segmentPartitionMetadata != null ? getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata,
        partitionColumn) : null;
  }

  /// Returns the partition id of a segment based on [SegmentPartitionMetadata].
  /// Can return `null` if the partition id cannot be determined.
  @VisibleForTesting
  @Nullable
  static Integer getPartitionIdFromSegmentPartitionMetadata(SegmentPartitionMetadata segmentPartitionMetadata,
      @Nullable String partitionColumn) {
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
    } else {
      return null;
    }
  }

  /// Returns the partition id of a segment based on segment name or ZK metadata, or a default partition id based on the
  /// hash of the segment name.
  /// Important: The method is costly because it may read data from zookeeper. Do not use it in query execution path.
  public static int getSegmentPartitionIdOrDefault(String segmentName, String tableNameWithType,
      HelixManager helixManager, @Nullable String partitionColumn) {
    Integer partitionId = getSegmentPartitionId(segmentName, tableNameWithType, helixManager, partitionColumn);
    return partitionId != null ? partitionId : getDefaultPartitionId(segmentName);
  }

  /// Returns the partition id of a segment based on segment name or ZK metadata, or a default partition id based on the
  /// hash of the segment name.
  public static int getSegmentPartitionIdOrDefault(SegmentZKMetadata segmentZKMetadata,
      @Nullable String partitionColumn) {
    Integer partitionId = getSegmentPartitionId(segmentZKMetadata, partitionColumn);
    return partitionId != null ? partitionId : getDefaultPartitionId(segmentZKMetadata.getSegmentName());
  }

  /// Returns a default partition id based on the hash of the segment name.
  public static int getDefaultPartitionId(String segmentName) {
    // A random, but consistent, partition id is calculated based on the hash code of the segment name.
    // Note that '% 10K' is used to prevent having partition ids with large value which will be problematic later in
    // instance assignment formula.
    return Math.abs(segmentName.hashCode() % 10_000);
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

  /**
   * Return table name without type from segment name
   */
  public static String getTableNameFromSegmentName(String segmentName) {
    return segmentName.substring(0, segmentName.indexOf("__"));
  }
}
