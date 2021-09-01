/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
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
 * The {@code PartitionSegmentPruner} prunes segments based on the their partition metadata stored
 * in ZK. The pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class PartitionSegmentPruner implements SegmentPruner {

  private final String _partitionColumn;
  private final Map<PartitionFunction, Set<PartitionInfo>> _partitionInfoSeen = new HashMap<>();
  private Set<SegmentBrokerView> _lastOnlineSegments;

  public PartitionSegmentPruner(String tableNameWithType, String partitionColumn,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<SegmentBrokerView> onlineSegments) {
    _lastOnlineSegments = new HashSet<>(onlineSegments);
    rebuildHelpers(_lastOnlineSegments);
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<SegmentBrokerView> onlineSegments) {
    if (!onlineSegments.equals(_lastOnlineSegments)) {
      rebuildHelpers(_lastOnlineSegments);
      _lastOnlineSegments = onlineSegments;
    }
  }

  @Override
  public synchronized void refreshSegment(SegmentBrokerView segment) {
    _lastOnlineSegments.add(segment);
    rebuildHelpers(_lastOnlineSegments);
  }

  private void rebuildHelpers(Set<SegmentBrokerView> onlineSegments) {
    _partitionInfoSeen.clear();
    List<PartitionInfo> nonNullPartitions =
        onlineSegments.stream().filter(segmentBrokerView -> segmentBrokerView.getPartitionInfo() != null)
            .map(SegmentBrokerView::getPartitionInfo).collect(Collectors.toList());
    nonNullPartitions.forEach(partitionInfo -> {
      Set<PartitionInfo> partitionInfoForFunc = _partitionInfoSeen.computeIfAbsent(partitionInfo._partitionFunction,
          (partitionFunction) -> new HashSet<>(partitionFunction.getNumPartitions()));
      partitionInfoForFunc.add(partitionInfo);
    });
  }

  @Override
  public Set<SegmentBrokerView> prune(BrokerRequest brokerRequest, Set<SegmentBrokerView> segments) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      // SQL

      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo) -> isPartitionMatch(filterExpression, partitionInfo));
    } else {
      // PQL
      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      if (filterQueryTree == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo) -> isPartitionMatch(filterQueryTree, partitionInfo));
    }
  }

  private Set<SegmentBrokerView> pruneSegments(Set<SegmentBrokerView> segments,
      java.util.function.Function<PartitionInfo, Boolean> partitionMatchLambda) {
    // Some segments may not be refreshed/notified via Zookeeper yet, but broker may have found it
    // in this case we need to include the new segments.
    Set<SegmentBrokerView> selectedSegments = new HashSet<>();
    Map<PartitionFunction, Set<PartitionInfo>> validPartitionsByFunc = new HashMap<>();
    _partitionInfoSeen.keySet().forEach(partitionFunction -> {
      validPartitionsByFunc.put(partitionFunction,
          _partitionInfoSeen.get(partitionFunction).stream().filter(partitionMatchLambda::apply)
              .collect(Collectors.toSet()));
    });
    Set<PartitionInfo> validPartitions = null;
    PartitionFunction lastPartitionFunction = null;
    Set<PartitionInfo> lastSeenPartitions = null;
    for (SegmentBrokerView segmentBrokerView : segments) {
      PartitionInfo partitionInfo = segmentBrokerView.getPartitionInfo();
      if (partitionInfo == null || partitionInfo == INVALID_PARTITION_INFO) {
        selectedSegments.add(segmentBrokerView);
      } else {
        /// optimization to avoid calling into hashmap query.
        if (lastPartitionFunction == null || !lastPartitionFunction.equals(partitionInfo._partitionFunction)) {
          lastPartitionFunction = partitionInfo._partitionFunction;
          validPartitions = validPartitionsByFunc.get(lastPartitionFunction);
          lastSeenPartitions = _partitionInfoSeen.get(partitionInfo._partitionFunction);
        }
        boolean needLambdaTesting = false;
        if (validPartitions == null) {
          // this is a brand new Partition Function, we should include the segment to avoid error
          needLambdaTesting = true;
        } else {
//          // this partition function is recognized
          if (lastSeenPartitions == null || !lastSeenPartitions.contains(partitionInfo)) {
            needLambdaTesting = true;
          } else if (validPartitions.contains(partitionInfo)) {
            selectedSegments.add(segmentBrokerView);
          }
        }
        if (needLambdaTesting) {
          if (partitionMatchLambda.apply(partitionInfo)) {
            selectedSegments.add(segmentBrokerView);
          }
        }
      }
    }
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, PartitionInfo partitionInfo) {
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
          return partitionInfo._partitions.contains(
              partitionInfo._partitionFunction.getPartition(operands.get(1).getLiteral().getFieldValue().toString()));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo._partitions.contains(partitionInfo._partitionFunction
                .getPartition(operands.get(i).getLiteral().getFieldValue().toString()))) {
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
        return true;
      default:
        return true;
    }
  }
}
