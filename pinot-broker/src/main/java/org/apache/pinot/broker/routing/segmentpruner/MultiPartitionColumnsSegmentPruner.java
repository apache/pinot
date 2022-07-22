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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.sql.FilterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code MultiPartitionColumnsSegmentPruner} prunes segments based on their partition metadata stored in ZK. The
 * pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class MultiPartitionColumnsSegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiPartitionColumnsSegmentPruner.class);
  private static final Map<String, PartitionInfo> INVALID_COLUMN_PARTITION_INFO_MAP = Collections.emptyMap();

  private final String _tableNameWithType;
  private final Set<String> _partitionColumns;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final Map<String, Map<String, PartitionInfo>> _segmentColumnPartitionInfoMap = new ConcurrentHashMap<>();

  public MultiPartitionColumnsSegmentPruner(String tableNameWithType, Set<String> partitionColumns,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _partitionColumns = partitionColumns;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    // Bulk load partition info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      Map<String, PartitionInfo> columnPartitionInfoMap =
          extractColumnPartitionInfoMapFromSegmentZKMetadataZNRecord(segment, znRecords.get(i));
      if (columnPartitionInfoMap != null) {
        _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfoMap);
      }
    }
  }

  /**
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       {@link #INVALID_COLUMN_PARTITION_INFO_MAP} when the segment does not have valid partition metadata in its ZK
   *       metadata, in which case we won't retry later.
   */
  @Nullable
  private Map<String, PartitionInfo> extractColumnPartitionInfoMapFromSegmentZKMetadataZNRecord(String segment,
      @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return INVALID_COLUMN_PARTITION_INFO_MAP;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          _tableNameWithType, e);
      return INVALID_COLUMN_PARTITION_INFO_MAP;
    }

    Map<String, PartitionInfo> columnPartitionInfoMap = new HashMap<>();
    for (String partitionColumn : _partitionColumns) {
      ColumnPartitionMetadata columnPartitionMetadata =
          segmentPartitionMetadata.getColumnPartitionMap().get(partitionColumn);
      if (columnPartitionMetadata == null) {
        LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", partitionColumn,
            segment, _tableNameWithType);
        continue;
      }
      PartitionInfo partitionInfo = new PartitionInfo(
          PartitionFunctionFactory.getPartitionFunction(columnPartitionMetadata.getFunctionName(),
              columnPartitionMetadata.getNumPartitions(), columnPartitionMetadata.getFunctionConfig()),
          columnPartitionMetadata.getPartitions());
      columnPartitionInfoMap.put(partitionColumn, partitionInfo);
    }
    if (columnPartitionInfoMap.size() == 1) {
      String partitionColumn = columnPartitionInfoMap.keySet().iterator().next();
      return Collections.singletonMap(partitionColumn, columnPartitionInfoMap.get(partitionColumn));
    }
    return columnPartitionInfoMap.isEmpty() ? INVALID_COLUMN_PARTITION_INFO_MAP : columnPartitionInfoMap;
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (String segment : onlineSegments) {
      _segmentColumnPartitionInfoMap.computeIfAbsent(segment,
          k -> extractColumnPartitionInfoMapFromSegmentZKMetadataZNRecord(k,
              _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT)));
    }
    _segmentColumnPartitionInfoMap.keySet().retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    Map<String, PartitionInfo> columnPartitionInfo = extractColumnPartitionInfoMapFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    if (columnPartitionInfo != null) {
      _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfo);
    } else {
      _segmentColumnPartitionInfoMap.remove(segment);
    }
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = new HashSet<>();
    for (String segment : segments) {
      Map<String, PartitionInfo> columnPartitionInfoMap = _segmentColumnPartitionInfoMap.get(segment);
      if (columnPartitionInfoMap == null || columnPartitionInfoMap == INVALID_COLUMN_PARTITION_INFO_MAP
          || isPartitionMatch(filterExpression, columnPartitionInfoMap)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  @VisibleForTesting
  public Set<String> getPartitionColumns() {
    return _partitionColumns;
  }

  private boolean isPartitionMatch(Expression filterExpression, Map<String, PartitionInfo> columnPartitionInfoMap) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, columnPartitionInfoMap)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, columnPartitionInfoMap)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null) {
          PartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          return partitionInfo == null || partitionInfo._partitions.contains(
              partitionInfo._partitionFunction.getPartition(operands.get(1).getLiteral().getFieldValue()));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null) {
          PartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          if (partitionInfo == null) {
            return true;
          }
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo._partitions.contains(partitionInfo._partitionFunction.getPartition(
                operands.get(i).getLiteral().getFieldValue().toString()))) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
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
