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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = new HashSet<>();
    // A singleton has no repeated work to reuse. Keep its evaluation free of predicate/cache setup.
    QueryPartitionMatcher matcher = segments.size() > 1 ? new QueryPartitionMatcher(filterExpression) : null;
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO
          || (matcher == null ? isPartitionMatch(filterExpression, partitionInfo) : matcher.matches(partitionInfo))) {
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
        return identifier == null || !identifier.getName().equals(_partitionColumn)
            || partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
                .getPartition(RequestContextUtils.getStringValue(operands.get(1))));
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier == null || !identifier.getName().equals(_partitionColumn)) {
          return true;
        }
        for (int i = 1; i < operands.size(); i++) {
          if (partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
              .getPartition(RequestContextUtils.getStringValue(operands.get(i))))) {
            return true;
          }
        }
        return false;
      }
      default:
        return true;
    }
  }

  /// All prepared predicates and hashes belong to one prune call; refreshes and other queries share none of this state.
  private final class QueryPartitionMatcher {
    private final Expression _filterExpression;
    private final Map<PartitionFunctionKey, PreparedPredicate> _predicates = new HashMap<>();
    private final PartitionFunctionLookup _lookup = new PartitionFunctionLookup();
    private PreparedPredicate _lastPredicate;

    private QueryPartitionMatcher(Expression filterExpression) {
      _filterExpression = filterExpression;
    }

    private boolean matches(SegmentPartitionInfo partitionInfo) {
      // Segment metadata contains distinct function instances. Avoid allocating a key per segment for the common
      // case where those instances have identical configuration.
      if (_lastPredicate == null || !_lookup.matches(partitionInfo)) {
        // This reusable lookup probe is never inserted. Only a previously unseen configuration allocates a stored key.
        _lookup._partitionInfo = partitionInfo;
        _lastPredicate = _predicates.get(_lookup);
        if (_lastPredicate == null) {
          _lastPredicate = new PreparedPredicate(_filterExpression, partitionInfo.getPartitionFunction());
          _predicates.put(new CachedPartitionFunctionKey(partitionInfo), _lastPredicate);
        }
      }
      return _lastPredicate.matches(partitionInfo.getPartitions());
    }
  }

  /// Interprets each visited predicate once and hashes IN values only as far as short-circuit evaluation requires.
  private final class PreparedPredicate {
    private final Expression _expression;
    private final PartitionFunction _partitionFunction;
    private FilterKind _filterKind;
    private List<Expression> _operands;
    private PreparedPredicate[] _children;
    private Integer[] _partitionIds;

    private PreparedPredicate(Expression expression, PartitionFunction partitionFunction) {
      _expression = expression;
      _partitionFunction = partitionFunction;
    }

    private void prepare() {
      Function function = _expression.getFunctionCall();
      _filterKind = FilterKind.valueOf(function.getOperator());
      _operands = function.getOperands();
      if (_filterKind == FilterKind.AND || _filterKind == FilterKind.OR) {
        _children = new PreparedPredicate[_operands.size()];
        for (int i = 0; i < _children.length; i++) {
          _children[i] = new PreparedPredicate(_operands.get(i), _partitionFunction);
        }
      } else if (_filterKind == FilterKind.EQUALS || _filterKind == FilterKind.IN) {
        Identifier identifier = _operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          _partitionIds = new Integer[_filterKind == FilterKind.EQUALS ? 1 : _operands.size() - 1];
        }
      }
    }

    private boolean matches(Set<Integer> partitions) {
      if (_filterKind == null) {
        prepare();
      }
      switch (_filterKind) {
        case AND:
          for (PreparedPredicate child : _children) {
            if (!child.matches(partitions)) {
              return false;
            }
          }
          return true;
        case OR:
          for (PreparedPredicate child : _children) {
            if (child.matches(partitions)) {
              return true;
            }
          }
          return false;
        case EQUALS:
        case IN:
          if (_partitionIds == null) {
            return true;
          }
          for (int i = 0; i < _partitionIds.length; i++) {
            Integer partitionId = _partitionIds[i];
            if (partitionId == null) {
              partitionId = _partitionFunction.getPartition(RequestContextUtils.getStringValue(_operands.get(i + 1)));
              _partitionIds[i] = partitionId;
            }
            if (partitions.contains(partitionId)) {
              return true;
            }
          }
          return false;
        default:
          return true;
      }
    }
  }

  /// Uses all recorded constructor inputs; legacy metadata without those inputs is reusable only by instance identity.
  private abstract static class PartitionFunctionKey {
    abstract SegmentPartitionInfo getPartitionInfo();

    final boolean matches(SegmentPartitionInfo partitionInfo) {
      SegmentPartitionInfo current = getPartitionInfo();
      PartitionFunction function = current.getPartitionFunction();
      PartitionFunction otherFunction = partitionInfo.getPartitionFunction();
      if (current.hasPartitionFunctionConfig() != partitionInfo.hasPartitionFunctionConfig()) {
        return false;
      }
      if (!current.hasPartitionFunctionConfig()) {
        return function == otherFunction;
      }
      return function.getClass() == otherFunction.getClass() && function.getName().equals(otherFunction.getName())
          && function.getNumPartitions() == otherFunction.getNumPartitions()
          && function.getPartitionIdNormalizer() == otherFunction.getPartitionIdNormalizer()
          && Objects.equals(current.getPartitionFunctionConfig(), partitionInfo.getPartitionFunctionConfig());
    }

    @Override
    public final boolean equals(Object other) {
      return other instanceof PartitionFunctionKey && matches(((PartitionFunctionKey) other).getPartitionInfo());
    }

    @Override
    public final int hashCode() {
      SegmentPartitionInfo partitionInfo = getPartitionInfo();
      PartitionFunction function = partitionInfo.getPartitionFunction();
      if (!partitionInfo.hasPartitionFunctionConfig()) {
        return System.identityHashCode(function);
      }
      // Unlike Objects.hash, this creates neither a varargs array nor a boxed partition count on each lookup.
      int hash = function.getClass().hashCode();
      hash = 31 * hash + function.getName().hashCode();
      hash = 31 * hash + function.getNumPartitions();
      hash = 31 * hash + function.getPartitionIdNormalizer().hashCode();
      return 31 * hash + Objects.hashCode(partitionInfo.getPartitionFunctionConfig());
    }
  }

  /// Immutable keys are retained only for configurations encountered during the current prune call.
  private static final class CachedPartitionFunctionKey extends PartitionFunctionKey {
    private final SegmentPartitionInfo _partitionInfo;

    private CachedPartitionFunctionKey(SegmentPartitionInfo partitionInfo) {
      _partitionInfo = partitionInfo;
    }

    @Override
    SegmentPartitionInfo getPartitionInfo() {
      return _partitionInfo;
    }
  }

  /// Mutable lookup state owned by one query; never stored in the predicate map.
  private static final class PartitionFunctionLookup extends PartitionFunctionKey {
    private SegmentPartitionInfo _partitionInfo;

    @Override
    SegmentPartitionInfo getPartitionInfo() {
      return _partitionInfo;
    }
  }
}
