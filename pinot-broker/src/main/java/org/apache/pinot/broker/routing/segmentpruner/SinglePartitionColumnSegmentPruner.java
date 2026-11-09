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
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
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
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.FilterKind;


/// The `SinglePartitionColumnSegmentPruner` prunes segments based on their partition metadata stored in ZK. The
/// pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
public class SinglePartitionColumnSegmentPruner implements SegmentPruner {
  private static final int MAX_LINEAR_FUNCTIONS = 8;
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final IntSupplier _preparationThreshold;
  private final Map<String, SegmentPartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn) {
    this(tableNameWithType, partitionColumn, Broker.DEFAULT_PARTITION_PRUNING_PREPARATION_THRESHOLD);
  }

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn,
      int minSegmentsForPreparation) {
    this(tableNameWithType, partitionColumn, () -> minSegmentsForPreparation);
  }

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn,
      IntSupplier preparationThreshold) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _preparationThreshold = preparationThreshold;
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
    Integer queryMinSegments =
        QueryOptionsUtils.getPartitionPruningPreparationThreshold(brokerRequest.getPinotQuery().getQueryOptions());
    int minSegmentsForPreparation = queryMinSegments != null ? queryMinSegments : _preparationThreshold.getAsInt();
    if (minSegmentsForPreparation >= 0 && segments.size() >= minSegmentsForPreparation) {
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
    List<PreparedPartitionIds> predicates = new ArrayList<>(2);
    Map<PartitionFunction, PreparedPartitionIds> predicateMap = null;
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
        selectedSegments.add(segment);
        continue;
      }
      PartitionFunction function = partitionInfo.getPartitionFunction();
      if (!function.supportsPartitionIdPreparation()) {
        if (isPartitionMatch(filterExpression, partitionInfo)) {
          selectedSegments.add(segment);
        }
        continue;
      }
      PreparedPartitionIds predicate = null;
      if (predicateMap == null) {
        for (PreparedPartitionIds candidate : predicates) {
          if (function.equals(candidate._partitionFunction)) {
            predicate = candidate;
            break;
          }
        }
      } else {
        predicate = predicateMap.get(function);
      }
      if (predicate == null) {
        try {
          predicate = new PreparedPartitionIds(function, preparePartitionIds(filterExpression, function), false);
        } catch (RuntimeException e) {
          // Preparation can reach a value or branch that per-segment short-circuiting would skip. Defer errors
          // to the original evaluator, and remember the fallback so this function is not prepared again.
          predicate = new PreparedPartitionIds(function, null, true);
        }
        if (predicateMap == null && predicates.size() < MAX_LINEAR_FUNCTIONS) {
          predicates.add(predicate);
        } else {
          if (predicateMap == null) {
            predicateMap = new HashMap<>();
            for (PreparedPartitionIds candidate : predicates) {
              predicateMap.put(candidate._partitionFunction, candidate);
            }
          }
          predicateMap.put(function, predicate);
        }
      }
      if (predicate._preparationFailed ? isPartitionMatch(filterExpression, partitionInfo)
          : predicate.matches(partitionInfo.getPartitions())) {
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

  /// Computes conservative candidate IDs for the whole predicate. Null represents every partition.
  /// Partition columns are single-valued, so a row satisfying AND must belong to every child's candidate set.
  @Nullable
  private IntSet preparePartitionIds(Expression expression, PartitionFunction partitionFunction) {
    Function function = expression.getFunctionCall();
    FilterKind kind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (kind) {
      case AND: {
        IntSet ids = null;
        for (Expression child : operands) {
          IntSet childIds = preparePartitionIds(child, partitionFunction);
          if (childIds != null) {
            if (ids == null) {
              ids = childIds;
            } else {
              // Singleton leaves are immutable; composite and IN sets can be intersected in place.
              if (!(ids instanceof IntOpenHashSet)) {
                ids = new IntOpenHashSet(ids);
              }
              ids.retainAll(childIds);
            }
          }
        }
        return ids;
      }
      case OR: {
        IntSet ids = new IntOpenHashSet();
        for (Expression child : operands) {
          IntSet childIds = preparePartitionIds(child, partitionFunction);
          if (childIds == null) {
            return null;
          }
          ids.addAll(childIds);
        }
        return ids;
      }
      case EQUALS:
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier == null || !identifier.getName().equals(_partitionColumn)) {
          return null;
        }
        int numValues = kind == FilterKind.EQUALS ? 1 : operands.size() - 1;
        if (numValues == 1) {
          return IntSets.singleton(partitionFunction.getPartition(RequestContextUtils.getStringValue(operands.get(1))));
        }
        IntSet ids = new IntOpenHashSet(Math.min(numValues, partitionFunction.getNumPartitions()));
        for (int i = 1; i <= numValues; i++) {
          ids.add(partitionFunction.getPartition(RequestContextUtils.getStringValue(operands.get(i))));
        }
        return ids;
      }
      default:
        return null;
    }
  }

  /// Prepared IDs belong to one prune call. Their sets are read-only after construction and never shared by queries.
  private static final class PreparedPartitionIds {
    private final PartitionFunction _partitionFunction;
    @Nullable
    private final IntSet _partitionIds;
    @Nullable
    private final Integer _singlePartitionId;
    private final boolean _preparationFailed;

    private PreparedPartitionIds(PartitionFunction partitionFunction, @Nullable IntSet partitionIds,
        boolean preparationFailed) {
      _partitionFunction = partitionFunction;
      _partitionIds = partitionIds;
      _singlePartitionId = partitionIds != null && partitionIds.size() == 1 ? partitionIds.iterator().nextInt() : null;
      _preparationFailed = preparationFailed;
    }

    private boolean matches(Set<Integer> partitions) {
      if (_partitionIds == null) {
        return true;
      }
      if (_singlePartitionId != null) {
        return partitions.contains(_singlePartitionId);
      }
      // Probe the smaller set; a segment can itself contain many partitions.
      if (_partitionIds.size() <= partitions.size()) {
        IntIterator iterator = _partitionIds.iterator();
        while (iterator.hasNext()) {
          if (partitions.contains(iterator.nextInt())) {
            return true;
          }
        }
      } else {
        for (int partition : partitions) {
          if (_partitionIds.contains(partition)) {
            return true;
          }
        }
      }
      return false;
    }
  }
}
