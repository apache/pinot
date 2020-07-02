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
package org.apache.pinot.controller.helix.core.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.utils.CommonConstants.Segment.SegmentType;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;


public class ZKMetadataUtils {
  private ZKMetadataUtils() {
  }

  public static void updateSegmentMetadata(OfflineSegmentZKMetadata offlineSegmentZKMetadata,
      SegmentMetadata segmentMetadata) {
    offlineSegmentZKMetadata.setSegmentName(segmentMetadata.getName());
    offlineSegmentZKMetadata.setTableName(segmentMetadata.getTableName());
    offlineSegmentZKMetadata.setIndexVersion(segmentMetadata.getVersion());
    offlineSegmentZKMetadata.setSegmentType(SegmentType.OFFLINE);
    if (segmentMetadata.getTimeInterval() != null) {
      offlineSegmentZKMetadata.setStartTime(segmentMetadata.getStartTime());
      offlineSegmentZKMetadata.setEndTime(segmentMetadata.getEndTime());
      offlineSegmentZKMetadata.setTimeUnit(segmentMetadata.getTimeUnit());
    }
    offlineSegmentZKMetadata.setTotalDocs(segmentMetadata.getTotalDocs());
    offlineSegmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
    offlineSegmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));

    // Extract column partition metadata (if any), and set it into segment ZK metadata.
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();
    if (segmentMetadata instanceof SegmentMetadataImpl) {
      SegmentMetadataImpl metadata = (SegmentMetadataImpl) segmentMetadata;
      for (Map.Entry<String, ColumnMetadata> entry : metadata.getColumnMetadataMap().entrySet()) {
        String column = entry.getKey();
        ColumnMetadata columnMetadata = entry.getValue();
        PartitionFunction partitionFunction = columnMetadata.getPartitionFunction();

        if (partitionFunction != null) {
          ColumnPartitionMetadata columnPartitionMetadata =
              new ColumnPartitionMetadata(partitionFunction.toString(), columnMetadata.getNumPartitions(),
                  columnMetadata.getPartitions());
          columnPartitionMap.put(column, columnPartitionMetadata);
        }
      }
    }

    if (!columnPartitionMap.isEmpty()) {
      offlineSegmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(columnPartitionMap));
    }
  }
}
