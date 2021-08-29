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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.segmentmetadata.PartitionInfo;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentBrokerView;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.segment.spi.partition.PartitionFunction;

import static org.apache.pinot.broker.routing.segmentmetadata.PartitionInfo.INVALID_PARTITION_INFO;


/**
 * The {@code PartitionSegmentPruner} prunes segments based on the their partition metadata stored in ZK. The pruner
 * supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class PartitionSegmentPruner implements SegmentPruner {

  private final String _partitionColumn;
  private Set<PartitionInfo> _partitionInfoSeen = new HashSet<>();
  private boolean _coversAllPartitions = false;

  public PartitionSegmentPruner(String tableNameWithType, String partitionColumn,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<SegmentBrokerView> onlineSegments) {
    onExternalViewChange(externalView, idealState, onlineSegments);
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<SegmentBrokerView> onlineSegments) {
    _partitionInfoSeen.clear();
    _partitionInfoSeen = onlineSegments.stream()
      .filter(segmentBrokerView -> segmentBrokerView.getPartitionInfo() != null)
      .map(SegmentBrokerView::getPartitionInfo)
      .collect(Collectors.toSet());
    _coversAllPartitions = false;
    if (!_partitionInfoSeen.isEmpty()) {
      PartitionInfo sample = _partitionInfoSeen.iterator().next();
      int totalPartitions = sample._partitionFunction.getNumPartitions();
      _coversAllPartitions = _partitionInfoSeen.size() == totalPartitions;
    }
  }

  @Override
  public synchronized void refreshSegment(SegmentBrokerView segment) {
  }

  @Override
  public Set<SegmentBrokerView> prune(BrokerRequest brokerRequest, Set<SegmentBrokerView> segments) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    final CachedPartitionFunction cachedPartitionFunction = new CachedPartitionFunction();
    if (pinotQuery != null) {
      // SQL

      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo) -> isPartitionMatch(filterExpression,
        partitionInfo, cachedPartitionFunction));
    } else {
      // PQL
      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      if (filterQueryTree == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo) -> isPartitionMatch(filterQueryTree, partitionInfo, cachedPartitionFunction));
    }
  }

  private Set<SegmentBrokerView> pruneSegments(Set<SegmentBrokerView> segments, java.util.function.Function<PartitionInfo, Boolean> partitionMatchLambda) {
    // Some segments may not be refreshed/notified via Zookeeper yet, but broker may have found it
    // in this case we need to include the new segments.
    Set<SegmentBrokerView> selectedSegments = new HashSet<>();
    Set<PartitionInfo> validPartitions = _partitionInfoSeen
      .stream()
      .filter(partitionMatchLambda::apply)
      .collect(Collectors.toSet());
    for (SegmentBrokerView segmentBrokerView : segments) {
      PartitionInfo partitionInfo = segmentBrokerView.getPartitionInfo();
      // _partitionInfoSegments is always a reverse map of _partitionInfoMap
      // so if we found a PartitionInfo it has to be in the valid partitions to be eligible
      if (partitionInfo == null
        || partitionInfo == INVALID_PARTITION_INFO
        || validPartitions.contains(partitionInfo)
        || (!_coversAllPartitions && !_partitionInfoSeen.contains(partitionInfo))
      ) {
        selectedSegments.add(segmentBrokerView);
      }
    }
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, PartitionInfo partitionInfo, CachedPartitionFunction cachedPartitionFunction) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          return partitionInfo._partitions.contains(
            cachedPartitionFunction.getPartition(partitionInfo._partitionFunction, operands.get(1).getLiteral().getFieldValue().toString()));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo._partitions.contains(cachedPartitionFunction.getPartition(partitionInfo._partitionFunction,
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

  @Deprecated
  private boolean isPartitionMatch(FilterQueryTree filterQueryTree, PartitionInfo partitionInfo, CachedPartitionFunction cachedPartitionFunction)
  {
    switch (filterQueryTree.getOperator()) {
      case AND:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (!isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return false;
          }
        }
        return true;
      case OR:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return true;
          }
        }
        return false;
      case EQUALITY:
      case IN:
        if (filterQueryTree.getColumn().equals(_partitionColumn)) {
          for (String value : filterQueryTree.getValue()) {
            if (partitionInfo._partitions.contains(cachedPartitionFunction.getPartition(partitionInfo._partitionFunction, value))) {
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

  /**
   * This class is created to speed up partition number computations, on our Expression match evaluation
   * In a worst case scenario, for an expression like <code>WHERE account_key = 'a73de713d'</code>
   * the call to <code>MurmurFunction.getPartition("a73de713d")</code> will be invoked for each segments we have.
   * If we have 30k+ segments, the same partition call are invoked 30k+ times.
   * Computing partition values may be CPU intensive, caching the computation result here will speed up at least 2x
   *
   * For each partition function, we have one map created transiently for each Segment Prune job. The map here
   * stores the HashFunction (mostly always the same), and then the Map<String, Integer> stores the lookup table
   * of each String and its hash partitions.
   */
  private static class CachedPartitionFunction {
    final Map<PartitionFunction, Map<String, Integer>> cache = new HashMap<>();

    public int getPartition(final PartitionFunction func, String partitionKey) {
      Map<String, Integer> valueToPartition = cache.computeIfAbsent(func, (ignored) -> new HashMap<>());
      if (!valueToPartition.containsKey(partitionKey)) {
        valueToPartition.put(partitionKey, func.getPartition(partitionKey));
      }
      return valueToPartition.get(partitionKey);
    }
  }

}
