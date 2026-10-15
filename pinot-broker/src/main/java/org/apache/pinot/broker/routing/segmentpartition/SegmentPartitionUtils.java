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

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPartitionUtils {
  private SegmentPartitionUtils() {
  }

  public static final SegmentPartitionInfo INVALID_PARTITION_INFO = new SegmentPartitionInfo(null, null, null);
  public static final Map<String, SegmentPartitionInfo> INVALID_COLUMN_PARTITION_INFO_MAP = Map.of();

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPartitionUtils.class);

  // Shared partition functions, keyed by the metadata they are built from. Every segment of a table usually carries the
  // same partition function metadata, so sharing one instance avoids creating one per segment on every routing
  // rebuild. Sharing is safe because PartitionFunction implementations must be stateless and thread-safe (see the
  // PartitionFunction contract). The cache stops growing at MAX_CACHED_PARTITION_FUNCTIONS distinct entries, after
  // which new functions are created without being cached.
  private static final int MAX_CACHED_PARTITION_FUNCTIONS = 1024;
  private static final Map<PartitionFunctionKey, PartitionFunction> PARTITION_FUNCTION_CACHE =
      new ConcurrentHashMap<>();

  // Shared immutable one-element partition sets. Most segments hold a single partition, so sharing them avoids one set
  // per segment.
  private static final int NUM_CACHED_SINGLE_PARTITION_SETS = 1024;
  private static final IntSet[] SINGLE_PARTITION_SETS = new IntSet[NUM_CACHED_SINGLE_PARTITION_SETS];

  static {
    for (int i = 0; i < NUM_CACHED_SINGLE_PARTITION_SETS; i++) {
      SINGLE_PARTITION_SETS[i] = IntSets.singleton(i);
    }
  }

  /// Returns the partition info for a given segment with single partition column.
  ///
  /// NOTE: Returns `null` when the ZNRecord is missing (could be transient Helix issue). Returns
  ///       [#INVALID_PARTITION_INFO] when the segment does not have valid partition metadata in its ZK metadata,
  ///       in which case we won't retry later.
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

    return new SegmentPartitionInfo(partitionColumn, getPartitionFunction(columnPartitionMetadata),
        getPartitions(columnPartitionMetadata));
  }

  /// Returns a partition function for the given metadata, shared with the other segments with equal metadata.
  private static PartitionFunction getPartitionFunction(ColumnPartitionMetadata columnPartitionMetadata) {
    PartitionFunctionKey key = new PartitionFunctionKey(columnPartitionMetadata.getFunctionName(),
        columnPartitionMetadata.getNumPartitions(), columnPartitionMetadata.getFunctionConfig());
    PartitionFunction partitionFunction = PARTITION_FUNCTION_CACHE.get(key);
    if (partitionFunction != null) {
      return partitionFunction;
    }
    partitionFunction = PartitionFunctionFactory.getPartitionFunction(columnPartitionMetadata);
    if (PARTITION_FUNCTION_CACHE.size() < MAX_CACHED_PARTITION_FUNCTIONS) {
      PartitionFunction existing = PARTITION_FUNCTION_CACHE.putIfAbsent(key, partitionFunction);
      if (existing != null) {
        return existing;
      }
    }
    return partitionFunction;
  }

  /// Returns the partitions of the given metadata, replacing a one-element set with a shared immutable one.
  private static Set<Integer> getPartitions(ColumnPartitionMetadata columnPartitionMetadata) {
    Set<Integer> partitions = columnPartitionMetadata.getPartitions();
    if (partitions.size() == 1) {
      int partition = partitions.iterator().next();
      if (partition >= 0 && partition < NUM_CACHED_SINGLE_PARTITION_SETS) {
        return SINGLE_PARTITION_SETS[partition];
      }
    }
    return partitions;
  }

  private record PartitionFunctionKey(String functionName, int numPartitions,
                                      @Nullable Map<String, String> functionConfig) {
  }

  /// Returns a map from partition column name to partition info for a given segment with multiple partition columns.
  ///
  /// NOTE: Returns `null` when the ZNRecord is missing (could be transient Helix issue). Returns
  ///       [#INVALID_COLUMN_PARTITION_INFO_MAP] when the segment does not have valid partition metadata in its ZK
  ///       metadata, in which case we won't retry later.
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
          getPartitionFunction(columnPartitionMetadata), getPartitions(columnPartitionMetadata));
      columnSegmentPartitionInfoMap.put(partitionColumn, segmentPartitionInfo);
    }
    if (columnSegmentPartitionInfoMap.size() == 1) {
      String partitionColumn = columnSegmentPartitionInfoMap.keySet().iterator().next();
      return Map.of(partitionColumn, columnSegmentPartitionInfoMap.get(partitionColumn));
    }
    return columnSegmentPartitionInfoMap.isEmpty() ? INVALID_COLUMN_PARTITION_INFO_MAP : columnSegmentPartitionInfoMap;
  }
}
