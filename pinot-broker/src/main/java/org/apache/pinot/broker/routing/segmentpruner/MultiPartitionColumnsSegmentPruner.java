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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code MultiPartitionColumnsSegmentPruner} prunes segments based on their partition metadata stored in ZK. The
 * pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class MultiPartitionColumnsSegmentPruner implements SegmentPruner {
  private final String _tableNameWithType;
  private final Set<String> _partitionColumns;
  private final Map<String, Map<String, SegmentPartitionInfo>> _segmentColumnPartitionInfoMap =
      new ConcurrentHashMap<>();

  public MultiPartitionColumnsSegmentPruner(String tableNameWithType, Set<String> partitionColumns) {
    _tableNameWithType = tableNameWithType;
    _partitionColumns = partitionColumns;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    // Bulk load partition info for all online segments
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap =
          SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, segment,
              znRecords.get(idx));
      if (columnPartitionInfoMap != null) {
        _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfoMap);
      }
    }
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (int idx = 0; idx < pulledSegments.size(); idx++) {
      String segment = pulledSegments.get(idx);
      ZNRecord znRecord = znRecords.get(idx);
      _segmentColumnPartitionInfoMap.computeIfAbsent(segment,
          k -> SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, k, znRecord));
    }
    _segmentColumnPartitionInfoMap.keySet().retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    Map<String, SegmentPartitionInfo> columnPartitionInfo =
        SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, segment, znRecord);
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
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap = _segmentColumnPartitionInfoMap.get(segment);
      if (columnPartitionInfoMap == null
          || columnPartitionInfoMap == SegmentPartitionUtils.INVALID_COLUMN_PARTITION_INFO_MAP || isPartitionMatch(
          filterExpression, columnPartitionInfoMap)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  @VisibleForTesting
  public Set<String> getPartitionColumns() {
    return _partitionColumns;
  }

  private boolean isPartitionMatch(Expression filterExpression,
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap) {
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
          SegmentPartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          return partitionInfo == null || partitionInfo.getPartitions().contains(
              partitionInfo.getPartitionFunction().getValueToPartition(operands.get(1).getLiteral().getFieldValue()));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null) {
          SegmentPartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          if (partitionInfo == null) {
            return true;
          }
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
                .getValueToPartition(operands.get(i).getLiteral().getFieldValue().toString()))) {
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
}
