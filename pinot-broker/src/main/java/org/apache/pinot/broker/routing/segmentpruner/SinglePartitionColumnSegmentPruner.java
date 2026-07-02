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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionUtils;
import org.apache.pinot.broker.routing.segmentpartition.SinglePartitionInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.sql.FilterKind;


public class SinglePartitionColumnSegmentPruner implements SegmentPruner {
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final Map<SinglePartitionInfo, Set<String>> _partitionToSegmentMap = new ConcurrentHashMap<>();
  private final Set<String> _unPartitionedSegments = ConcurrentHashMap.newKeySet();
  private final ReadWriteLock _mapLock = new ReentrantReadWriteLock();

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn) {
    _partitionColumn = partitionColumn;
    _tableNameWithType = tableNameWithType;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    // Bulk load partition info for all online segments
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      SegmentPartitionInfo partitionInfo =
          SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecords.get(idx));
      addSegmentToPartitionIndex(segment, partitionInfo);
    }
  }

  private void addSegmentToPartitionIndex(String segment, SegmentPartitionInfo partitionInfo) {
    if (partitionInfo == null || partitionInfo.getPartitionColumn() == null) {
      _unPartitionedSegments.add(segment);
      return;
    }
    _unPartitionedSegments.remove(segment);
    Set<SinglePartitionInfo> singlePartitionInfos = partitionInfo.getPartitions()
        .stream()
        .map(p -> new SinglePartitionInfo(partitionInfo.getPartitionColumn(), partitionInfo.getPartitionFunction(), p))
        .collect(Collectors.toSet());
    _mapLock.readLock().lock();
    try {
      for (SinglePartitionInfo singlePartitionInfo : singlePartitionInfos) {
        _partitionToSegmentMap.computeIfAbsent(singlePartitionInfo, k -> ConcurrentHashMap.newKeySet()).add(segment);
      }
      _partitionToSegmentMap.entrySet()
          .stream()
          .filter(entry -> !singlePartitionInfos.contains(entry.getKey()))
          .forEach(e -> e.getValue().remove(segment));
    } finally {
      _mapLock.readLock().unlock();
    }
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    for (int idx = 0; idx < pulledSegments.size(); idx++) {
      String segment = pulledSegments.get(idx);
      ZNRecord znRecord = znRecords.get(idx);
      if (_partitionToSegmentMap.values().stream().noneMatch(set -> set.contains(segment))) {
        addSegmentToPartitionIndex(segment,
            SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord));
      }
    }
    _mapLock.writeLock().lock();
    try {
      _unPartitionedSegments.retainAll(onlineSegments);
      _partitionToSegmentMap.values().forEach(set -> set.retainAll(onlineSegments));
      _partitionToSegmentMap.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    } finally {
      _mapLock.writeLock().unlock();
    }
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo partitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord);
    if (partitionInfo != null) {
      addSegmentToPartitionIndex(segment, partitionInfo);
    } else {
      _unPartitionedSegments.add(segment);
      _partitionToSegmentMap.values().forEach(set -> set.remove(segment));
      _partitionToSegmentMap.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = _partitionToSegmentMap.entrySet()
        .stream()
        .filter(entry -> isPartitionMatch(filterExpression, entry.getKey()))
        .map(Map.Entry::getValue)
        .flatMap(Set::stream)
        .filter(segments::contains)
        .collect(Collectors.toSet());
    _unPartitionedSegments.stream().filter(segments::contains).forEach(selectedSegments::add);
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, SinglePartitionInfo partitionInfo) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, partitionInfo)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, partitionInfo)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          return partitionInfo.getPartition() == partitionInfo.getPartitionFunction()
              .getPartition(RequestContextUtils.getStringValue(operands.get(1)));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartition() == (partitionInfo.getPartitionFunction()
                .getPartition(RequestContextUtils.getStringValue(operands.get(i))))) {
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
