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
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.sql.FilterKind;


/// The `SinglePartitionColumnSegmentPruner` prunes segments based on their partition metadata stored in ZK. The
/// pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
public class SinglePartitionColumnSegmentPruner implements SegmentPruner {
  // Preparing predicates costs more than it saves for small inputs, especially with cheap partition functions.
  private static final int MIN_SEGMENTS_FOR_PREPARATION = 256;
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final Map<String, SegmentPartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    // Bulk load partition info for all online segments
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      SegmentPartitionInfo partitionInfo =
          SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecords.get(idx));
      if (partitionInfo != null) {
        _partitionInfoMap.put(segment, partitionInfo);
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
      _partitionInfoMap.computeIfAbsent(segment,
          k -> SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, k, znRecord));
    }
    _partitionInfoMap.keySet().retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo partitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord);
    if (partitionInfo != null) {
      _partitionInfoMap.put(segment, partitionInfo);
    } else {
      _partitionInfoMap.remove(segment);
    }
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    int numSegments = segments.size();
    if (numSegments == 0) {
      return segments;
    }
    if (numSegments >= MIN_SEGMENTS_FOR_PREPARATION) {
      return pruneWithPreparedPredicate(filterExpression, segments);
    }
    Set<String> selectedSegments = new HashSet<>();
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO || isPartitionMatch(
          filterExpression, partitionInfo)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  private Set<String> pruneWithPreparedPredicate(Expression filterExpression, Set<String> segments) {
    Set<String> selectedSegments = new HashSet<>();
    PreparedPredicate predicate = null;
    PartitionFunction cachedFunction = null;
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
        selectedSegments.add(segment);
        continue;
      }
      PartitionFunction function = partitionInfo.getPartitionFunction();
      if (predicate == null) {
        predicate = new PreparedPredicate(filterExpression);
        cachedFunction = function;
      }
      // Reuse the filter structure even when the functions cannot share partition ids.
      boolean reusePartitionIds = cachedFunction.canReusePartitionIds(function);
      if (predicate.matches(partitionInfo.getPartitions(), function, reusePartitionIds)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, SegmentPartitionInfo partitionInfo) {
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
          return partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
              .getPartition(RequestContextUtils.getStringValue(operands.get(1))));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
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

  /// Lazily prepares only visited expressions and values. Instances belong to one prune call, never shared by queries.
  private final class PreparedPredicate {
    private final Expression _expression;
    private FilterKind _kind;
    private List<Expression> _operands;
    private PreparedPredicate[] _children;
    private List<Integer> _partitionIds;

    private PreparedPredicate(Expression expression) {
      _expression = expression;
    }

    private boolean matches(Set<Integer> partitions, PartitionFunction partitionFunction, boolean reusePartitionIds) {
      if (_kind == null) {
        Function function = _expression.getFunctionCall();
        _kind = FilterKind.valueOf(function.getOperator());
        _operands = function.getOperands();
        if (_kind == FilterKind.AND || _kind == FilterKind.OR) {
          _children = new PreparedPredicate[_operands.size()];
          for (int i = 0; i < _children.length; i++) {
            _children[i] = new PreparedPredicate(_operands.get(i));
          }
        } else if (_kind == FilterKind.EQUALS || _kind == FilterKind.IN) {
          Identifier identifier = _operands.get(0).getIdentifier();
          if (identifier != null && identifier.getName().equals(_partitionColumn)) {
            _partitionIds = new ArrayList<>(1);
          }
        }
      }
      switch (_kind) {
        case AND:
          for (PreparedPredicate child : _children) {
            if (!child.matches(partitions, partitionFunction, reusePartitionIds)) {
              return false;
            }
          }
          return true;
        case OR:
          for (PreparedPredicate child : _children) {
            if (child.matches(partitions, partitionFunction, reusePartitionIds)) {
              return true;
            }
          }
          return false;
        case EQUALS:
        case IN:
          if (_partitionIds != null) {
            int numValues = _kind == FilterKind.EQUALS ? 1 : _operands.size() - 1;
            if (!reusePartitionIds) {
              for (int i = 0; i < numValues; i++) {
                if (partitions.contains(
                    partitionFunction.getPartition(RequestContextUtils.getStringValue(_operands.get(i + 1))))) {
                  return true;
                }
              }
              return false;
            }
            for (int i = 0; i < numValues; i++) {
              Integer partitionId;
              if (i < _partitionIds.size()) {
                partitionId = _partitionIds.get(i);
              } else {
                partitionId = partitionFunction.getPartition(RequestContextUtils.getStringValue(_operands.get(i + 1)));
                _partitionIds.add(partitionId);
              }
              if (partitions.contains(partitionId)) {
                return true;
              }
            }
            return false;
          }
          return true;
        default:
          return true;
      }
    }
  }
}
