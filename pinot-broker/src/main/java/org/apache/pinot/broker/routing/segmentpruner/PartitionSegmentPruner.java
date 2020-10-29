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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Segment;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.data.partition.PartitionFunctionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code PartitionSegmentPruner} prunes segments based on the their partition metadata stored in ZK. The pruner
 * supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class PartitionSegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionSegmentPruner.class);
  private static final PartitionInfo INVALID_PARTITION_INFO = new PartitionInfo(null, null);

  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final Map<String, PartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();

  public PartitionSegmentPruner(String tableNameWithType, String partitionColumn,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    // Bulk load partition info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(onlineSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment, znRecords.get(i));
      if (partitionInfo != null) {
        _partitionInfoMap.put(segment, partitionInfo);
      }
    }
  }

  /**
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       {@link #INVALID_PARTITION_INFO} when the segment does not have valid partition metadata in its ZK metadata,
   *       in which case we won't retry later.
   */
  @Nullable
  private PartitionInfo extractPartitionInfoFromSegmentZKMetadataZNRecord(String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(Segment.PARTITION_METADATA);
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

    return new PartitionInfo(PartitionFunctionFactory
        .getPartitionFunction(columnPartitionMetadata.getFunctionName(), columnPartitionMetadata.getNumPartitions()),
        columnPartitionMetadata.getPartitions());
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (String segment : onlineSegments) {
      _partitionInfoMap.computeIfAbsent(segment, k -> extractPartitionInfoFromSegmentZKMetadataZNRecord(k,
          _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT)));
    }
    _partitionInfoMap.keySet().retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    if (partitionInfo != null) {
      _partitionInfoMap.put(segment, partitionInfo);
    } else {
      _partitionInfoMap.remove(segment);
    }
  }

  @Override
  public List<String> prune(BrokerRequest brokerRequest, List<String> segments) {
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (filterQueryTree == null) {
      return segments;
    }
    List<String> selectedSegments = new ArrayList<>();
    for (String segment : segments) {
      PartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == INVALID_PARTITION_INFO || isPartitionMatch(filterQueryTree,
          partitionInfo)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  private boolean isPartitionMatch(FilterQueryTree filterQueryTree, PartitionInfo partitionInfo) {
    switch (filterQueryTree.getOperator()) {
      case AND:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (!isPartitionMatch(child, partitionInfo)) {
            return false;
          }
        }
        return true;
      case OR:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (isPartitionMatch(child, partitionInfo)) {
            return true;
          }
        }
        return false;
      case EQUALITY:
      case IN:
        if (filterQueryTree.getColumn().equals(_partitionColumn)) {
          for (String value : filterQueryTree.getValue()) {
            if (partitionInfo._partitions.contains(partitionInfo._partitionFunction.getPartition(value))) {
              return true;
            }
          }
          return false;
        }
      default:
        return true;
    }
  }

  private static class PartitionInfo {
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;

    PartitionInfo(PartitionFunction partitionFunction, Set<Integer> partitions) {
      _partitionFunction = partitionFunction;
      _partitions = partitions;
    }
  }
}
