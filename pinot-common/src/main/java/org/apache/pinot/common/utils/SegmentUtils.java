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
import java.util.Set;
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
  public static int getRealtimeSegmentPartitionId(String segmentName, String realtimeTableName, HelixManager helixManager,
      String partitionColumn) {
    // A fast path if the segmentName is a LLC segment name and we can get the partition id from the name directly.
    if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
      return new LLCSegmentName(segmentName).getPartitionGroupId();
    }
    // Otherwise, retrieve the partition id from the segment zk metadata. Currently only realtime segments from upsert
    // enabled tables have partition ids in their segment metadata.
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), realtimeTableName, segmentName);
    Preconditions.checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s", segmentName,
        realtimeTableName);
    SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
    Preconditions.checkState(segmentPartitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata", segmentName, realtimeTableName);
    ColumnPartitionMetadata columnPartitionMetadata = segmentPartitionMetadata.getColumnPartitionMap().get(partitionColumn);
    Preconditions.checkState(columnPartitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s. Check "
            + "if the table is an upsert table.", segmentName, realtimeTableName, partitionColumn);
    Set<Integer> partitions = columnPartitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s with %s", segmentName,
        realtimeTableName, partitionColumn, partitions);
    return partitions.iterator().next();
  }
}
