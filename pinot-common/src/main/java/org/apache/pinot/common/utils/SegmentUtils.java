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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.TableType;


// Util functions related to segments.
public class SegmentUtils {
  private SegmentUtils() {
  }

  // Returns the partition id of a realtime segment based segment name and segment metadata info retrieved via Helix.
  // Can return null if the partitionId is not found based on the above
  // Important: The method is costly because it may read data from zookeeper. Do not use it in any query execution
  // path.
  @Nullable
  public static Integer getRealtimeSegmentPartitionId(String segmentName, String realtimeTableName,
      HelixManager helixManager, @Nullable String partitionColumn) {
    Integer partitionId = getPartitionIdFromRealtimeSegmentName(segmentName);
    if (partitionId != null) {
      return partitionId;
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
    Integer partitionId = getPartitionIdFromRealtimeSegmentName(segmentName);
    if (partitionId != null) {
      return partitionId;
    }
    // Otherwise, retrieve the partition id from the segment zk metadata.
    return getRealtimeSegmentPartitionId(segmentZKMetadata, partitionColumn);
  }

  @Nullable
  public static Integer getPartitionIdFromRealtimeSegmentName(String segmentName) {
    // A fast path to get partition id if the segmentName is in a known format like LLC.
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

  @Nullable
  private static Integer getRealtimeSegmentPartitionId(SegmentZKMetadata segmentZKMetadata,
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
   * Return the partitionId for an OFFLINE or COMPLETED instance partitions of a REALTIME table with relocation enabled
   * The partitionId will be calculated as:
   * <ul>
   *   <li>
   *     1. If numPartitions = 1, return partitionId = 0
   *   </li>
   *   <li>
   *     2. Otherwise, fallback to either the OFFLINE or REALTIME partitionId calculation logic
   *   </li>
   * </ul>
   */
  public static int getOfflineOrCompletedPartitionId(String segmentName, String tableName, TableType tableType,
      HelixManager helixManager, int numPartitions, @Nullable String partitionColumn) {
    int partitionId;
    if (numPartitions == 1) {
      partitionId = 0;
    } else {
      // Uniformly spray the segment partitions over the instance partitions
      if (tableType == TableType.OFFLINE) {
        partitionId = SegmentUtils
            .getOfflineSegmentPartitionId(segmentName, tableName, helixManager, partitionColumn);
      } else {
        partitionId = SegmentUtils
            .getRealtimeSegmentPartitionIdOrDefault(segmentName, tableName, helixManager, partitionColumn);
      }
    }
    return partitionId;
  }

  /**
   * Return the OFFLINE table partitionId based on the segment metadata if available. If partitionColumn is null, return
   * a default partitionId calculated based on the segment name
   */
  private static int getOfflinePartitionId(SegmentZKMetadata segmentZKMetadata, String offlineTableName,
      @Nullable String partitionColumn) {
    String segmentName = segmentZKMetadata.getSegmentName();
    if (partitionColumn == null) {
      // Use the same logic to calculate the partitionId if partitionColumn or calculated partitionId is null for both
      // OFFLINE and REALTIME tables
      return getDefaultPartitionId(segmentName);
    }
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s",
        segmentName, offlineTableName, partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s", segmentName,
        offlineTableName, partitionColumn);
    return partitions.iterator().next();
  }

  /**
   * Returns a partition id for offline table
   * The partitionId will be calculated as:
   * <ul>
   *   <li>
   *     1. If partitionColumn == null, return a default partitionId calculated based on the hashCode of the segmentName
   *   </li>
   *   <li>
   *     2. Otherwise, fetch the partitionMetadata from the SegmentZkMetadata related to the partitionColumn and return
   *        that as the partitionId
   *   </li>
   * </ul>
   */
  public static int getOfflineSegmentPartitionId(String segmentName, String offlineTableName, HelixManager helixManager,
      @Nullable String partitionColumn) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), offlineTableName, segmentName);
    Preconditions.checkState(segmentZKMetadata != null,
        "Failed to find segment ZK metadata for segment: %s of table: %s", segmentName, offlineTableName);
    return SegmentUtils.getOfflinePartitionId(segmentZKMetadata, offlineTableName, partitionColumn);
  }

  /**
   * Returns map of instance partition id to segments for offline tables
   */
  public static Map<Integer, List<String>> getOfflineInstancePartitionIdToSegmentsMap(Set<String> segments,
      int numInstancePartitions, String offlineTableName, HelixManager helixManager, @Nullable String partitionColumn) {
    // Fetch partition id from segment ZK metadata
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(helixManager.getHelixPropertyStore(), offlineTableName);

    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    Set<String> segmentsWithoutZKMetadata = new HashSet<>(segments);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      String segmentName = segmentZKMetadata.getSegmentName();
      if (segmentsWithoutZKMetadata.remove(segmentName)) {
        int partitionId = SegmentUtils.getOfflinePartitionId(segmentZKMetadata, offlineTableName, partitionColumn);
        int instancePartitionId = partitionId % numInstancePartitions;
        instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>()).add(segmentName);
      }
    }
    Preconditions.checkState(segmentsWithoutZKMetadata.isEmpty(), "Failed to find ZK metadata for segments: %s",
        segmentsWithoutZKMetadata);

    return instancePartitionIdToSegmentsMap;
  }

  /**
   * Returns a partition id for realtime table
   * The partitionId will be calculated as:
   * <ul>
   *   <li>
   *     1. Try to fetch the partitionId based on the segmentName
   *   </li>
   *   <li>
   *     2. Otherwise, try to fetch the partitionId from the SegmentZkMetadata using the partitionColumn
   *   </li>
   *   <li>
   *     3. Otherwise, if the returned segmentPartitionId = null, return a default partitionId calculated based on the
   *        hashCode of the segmentName
   *   </li>
   * </ul>
   */
  public static int getRealtimeSegmentPartitionIdOrDefault(String segmentName, String realtimeTableName,
      HelixManager helixManager, @Nullable String partitionColumn) {
    Integer segmentPartitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, realtimeTableName, helixManager, partitionColumn);
    if (segmentPartitionId == null) {
      // This case is for the uploaded segments for which there's no partition information.
      segmentPartitionId = SegmentUtils.getDefaultPartitionId(segmentName);
    }
    return segmentPartitionId;
  }

  /**
   * Returns map of instance partition id to segments for realtime tables
   */
  public static Map<Integer, List<String>> getRealtimeInstancePartitionIdToSegmentsMap(Set<String> segments,
      int numInstancePartitions, String realtimeTableName, HelixManager helixManager,
      @Nullable String partitionColumn) {
    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : segments) {
      int instancePartitionId =
          getRealtimeSegmentPartitionIdOrDefault(segmentName, realtimeTableName, helixManager, partitionColumn)
              % numInstancePartitions;
      instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>()).add(segmentName);
    }
    return instancePartitionIdToSegmentsMap;
  }

  /**
   * Return a default partitionId based on the hashCode of the segment name. This can be used for both REALTIME and
   * OFFLINE tables if a partitionId is not available to more evenly spread out these segments
   */
  @VisibleForTesting
  static int getDefaultPartitionId(String segmentName) {
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
}
