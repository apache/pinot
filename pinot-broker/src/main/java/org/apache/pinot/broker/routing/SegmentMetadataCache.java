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
package org.apache.pinot.broker.routing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMetadataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataCache.class);
  private static final PartitionInfo INVALID_PARTITION_INFO = new PartitionInfo();
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Map<String, PartitionInfo> _segmentToPartitionInfoMap;
  private final String _segmentZKMetadataPathPrefix;

  public SegmentMetadataCache(String tableNameWithType, String partitionColumn,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _propertyStore = propertyStore;
    _segmentToPartitionInfoMap = new ConcurrentHashMap<>();
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
  }

  public void init(Set<String> onlineSegments) {
    for (String segment : onlineSegments) {
      PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment,
          _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
      _segmentToPartitionInfoMap.put(segment, partitionInfo);
    }
  }

  public synchronized void onAssignmentChange(Set<String> onlineSegments) {
    for (String segment : onlineSegments) {
      _segmentToPartitionInfoMap.computeIfAbsent(segment, k -> extractPartitionInfoFromSegmentZKMetadataZNRecord(k,
          _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT)));
    }
    _segmentToPartitionInfoMap.keySet().retainAll(onlineSegments);
  }

  public synchronized void refreshSegment(String segment) {
    PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    if (partitionInfo != null) {
      _segmentToPartitionInfoMap.put(segment, partitionInfo);
    } else {
      _segmentToPartitionInfoMap.remove(segment);
    }
  }

  public int getPartitionId(String segmentName) {
    PartitionInfo partitionInfo = _segmentToPartitionInfoMap.get(segmentName);
    if (partitionInfo == null || partitionInfo._partitionId == null) {
      return -1;
    }
    return partitionInfo._partitionId;
  }

  @Nullable
  private PartitionInfo extractPartitionInfoFromSegmentZKMetadataZNRecord(String segment, @Nullable
      ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(CommonConstants.Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          _tableNameWithType, e);
      return INVALID_PARTITION_INFO;
    }

    ColumnPartitionMetadata columnPartitionMetadata =
        segmentPartitionMetadata.getColumnPartitionMap().get(_partitionColumn);
    if (columnPartitionMetadata == null) {
      LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", _partitionColumn,
          segment, _tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    Set<Integer> partitions = columnPartitionMetadata.getPartitions();
    if (partitions != null && partitions.size() != 1) {
      LOGGER.warn("Failed to find singleton partition for column: {}, segment: {}, table: {}. Found partition Ids: {}",
          _partitionColumn, segment, _tableNameWithType, partitions);
      return INVALID_PARTITION_INFO;
    }

    return new PartitionInfo(PartitionFunctionFactory.getPartitionFunction(columnPartitionMetadata.getFunctionName(),
        columnPartitionMetadata.getNumPartitions(), columnPartitionMetadata.getFunctionConfig()),
        partitions.iterator().next());
  }

  static class PartitionInfo {
    private PartitionFunction _partitionFunction;
    private Integer _partitionId;

    public PartitionInfo() {
    }

    public PartitionInfo(PartitionFunction partitionFunction, Integer partitionId) {
      _partitionFunction = partitionFunction;
      _partitionId = partitionId;
    }
  }
}
