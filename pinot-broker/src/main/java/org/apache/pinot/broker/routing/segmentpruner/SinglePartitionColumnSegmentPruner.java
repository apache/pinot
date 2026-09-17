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

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
    if (filterExpression == null || segments.isEmpty()) {
      return segments;
    }
    Set<String> selectedSegments = new HashSet<>();
    // Prepared predicates and hashes are shared across equivalent functions only within this prune call.
    Map<Object, PreparedPredicate> predicates = new IdentityHashMap<>();
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
        selectedSegments.add(segment);
        continue;
      }
      Object key = partitionInfo.getPartitionFunctionKey();
      PreparedPredicate predicate = predicates.get(key);
      if (predicate == null) {
        predicate = new PreparedPredicate(filterExpression, partitionInfo.getPartitionFunction());
        predicates.put(key, predicate);
      }
      if (predicate.matches(partitionInfo.getPartitions())) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  /// Interprets each visited predicate once and hashes IN values only as far as short-circuit evaluation requires.
  private final class PreparedPredicate {
    private final PartitionFunction _partitionFunction;
    private final FilterKind _filterKind;
    private final List<Expression> _operands;
    private final PreparedPredicate[] _children;
    private final IntSet _partitionIds;
    private final int _numValues;
    private int _numEvaluatedValues;

    private PreparedPredicate(Expression expression, PartitionFunction partitionFunction) {
      _partitionFunction = partitionFunction;
      Function function = expression.getFunctionCall();
      _filterKind = FilterKind.valueOf(function.getOperator());
      _operands = function.getOperands();
      _children = _filterKind == FilterKind.AND || _filterKind == FilterKind.OR
          ? new PreparedPredicate[_operands.size()]
          : null;
      if (_filterKind == FilterKind.EQUALS || _filterKind == FilterKind.IN) {
        Identifier identifier = _operands.get(0).getIdentifier();
        _partitionIds = identifier != null && identifier.getName().equals(_partitionColumn)
            ? new IntOpenHashSet()
            : null;
        _numValues = _filterKind == FilterKind.EQUALS ? 1 : _operands.size() - 1;
      } else {
        _partitionIds = null;
        _numValues = 0;
      }
    }

    private PreparedPredicate child(int index) {
      // Construct a child only when visited, preserving short-circuit behavior even for invalid later expressions.
      PreparedPredicate child = _children[index];
      if (child == null) {
        child = new PreparedPredicate(_operands.get(index), _partitionFunction);
        _children[index] = child;
      }
      return child;
    }

    private boolean matches(Set<Integer> partitions) {
      switch (_filterKind) {
        case AND:
          for (int i = 0; i < _children.length; i++) {
            if (!child(i).matches(partitions)) {
              return false;
            }
          }
          return true;
        case OR:
          for (int i = 0; i < _children.length; i++) {
            if (child(i).matches(partitions)) {
              return true;
            }
          }
          return false;
        case EQUALS:
        case IN:
          if (_partitionIds == null) {
            return true;
          }
          for (IntIterator iterator = _partitionIds.iterator(); iterator.hasNext();) {
            if (partitions.contains(iterator.nextInt())) {
              return true;
            }
          }
          while (_numEvaluatedValues < _numValues) {
            int partitionId = _partitionFunction.getPartition(
                RequestContextUtils.getStringValue(_operands.get(_numEvaluatedValues + 1)));
            _numEvaluatedValues++;
            if (_partitionIds.add(partitionId) && partitions.contains(partitionId)) {
              return true;
            }
          }
          return false;
        default:
          return true;
      }
    }
  }
}
