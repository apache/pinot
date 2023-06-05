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
package org.apache.pinot.broker.routing.segmentpartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPartitionUtils {
  private SegmentPartitionUtils() {
  }

  public static final SegmentPartitionInfo INVALID_PARTITION_INFO = new SegmentPartitionInfo(null, null, null);
  public static final Map<String, SegmentPartitionInfo> INVALID_COLUMN_PARTITION_INFO_MAP = Collections.emptyMap();

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPartitionUtils.class);

  /**
   * Returns the partition info for a given segment with single partition column.
   *
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       {@link #INVALID_PARTITION_INFO} when the segment does not have valid partition metadata in its ZK metadata,
   *       in which case we won't retry later.
   */
  @Nullable
  public static SegmentPartitionInfo extractPartitionInfo(String tableNameWithType, String partitionColumn,
      String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(CommonConstants.Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          tableNameWithType, e);
      return INVALID_PARTITION_INFO;
    }

    ColumnPartitionMetadata columnPartitionMetadata =
        segmentPartitionMetadata.getColumnPartitionMap().get(partitionColumn);
    if (columnPartitionMetadata == null) {
      LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", partitionColumn,
          segment, tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    return new SegmentPartitionInfo(partitionColumn,
        PartitionFunctionFactory.getPartitionFunction(columnPartitionMetadata.getFunctionName(),
            columnPartitionMetadata.getNumPartitions(), columnPartitionMetadata.getFunctionConfig()),
        columnPartitionMetadata.getPartitions());
  }

  /**
   * Returns a map from partition column name to partition info for a given segment with multiple partition columns.
   *
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       {@link #INVALID_COLUMN_PARTITION_INFO_MAP} when the segment does not have valid partition metadata in its ZK
   *       metadata, in which case we won't retry later.
   */
  @Nullable
  public static Map<String, SegmentPartitionInfo> extractPartitionInfoMap(String tableNameWithType,
      Set<String> partitionColumns, String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(CommonConstants.Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, tableNameWithType);
      return INVALID_COLUMN_PARTITION_INFO_MAP;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          tableNameWithType, e);
      return INVALID_COLUMN_PARTITION_INFO_MAP;
    }

    Map<String, SegmentPartitionInfo> columnSegmentPartitionInfoMap = new HashMap<>();
    for (String partitionColumn : partitionColumns) {
      ColumnPartitionMetadata columnPartitionMetadata =
          segmentPartitionMetadata.getColumnPartitionMap().get(partitionColumn);
      if (columnPartitionMetadata == null) {
        LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", partitionColumn,
            segment, tableNameWithType);
        continue;
      }
      SegmentPartitionInfo segmentPartitionInfo = new SegmentPartitionInfo(partitionColumn,
          PartitionFunctionFactory.getPartitionFunction(columnPartitionMetadata.getFunctionName(),
              columnPartitionMetadata.getNumPartitions(), columnPartitionMetadata.getFunctionConfig()),
          columnPartitionMetadata.getPartitions());
      columnSegmentPartitionInfoMap.put(partitionColumn, segmentPartitionInfo);
    }
    if (columnSegmentPartitionInfoMap.size() == 1) {
      String partitionColumn = columnSegmentPartitionInfoMap.keySet().iterator().next();
      return Collections.singletonMap(partitionColumn, columnSegmentPartitionInfoMap.get(partitionColumn));
    }
    return columnSegmentPartitionInfoMap.isEmpty() ? INVALID_COLUMN_PARTITION_INFO_MAP : columnSegmentPartitionInfoMap;
  }
}
