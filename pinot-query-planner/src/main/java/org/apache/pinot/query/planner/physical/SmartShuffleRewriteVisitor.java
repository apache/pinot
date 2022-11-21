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
package org.apache.pinot.query.planner.physical;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.logical.PhysicalStageTraversalVisitor;
import org.apache.pinot.query.planner.logical.PhysicalStageTraversalVisitor.PhysicalStageInfo;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Underlying assumptions:
 * 1. If a stage A depends on stage B, stageId(A) < stageId(B)
 * 2. Leaf stage can either be a MailboxReceiveNode or TableScanNode.
 */
public class SmartShuffleRewriteVisitor implements StageNodeVisitor<DisjointSet<Integer>, PhysicalStageInfo> {
  private final TableCache _tableCache;
  private final Map<Integer, StageMetadata> _stageMetadataMap;
  private boolean _canSkipShuffleForJoin;

  public static void optimizeShuffles(StageNode rootStageNode, Map<Integer, StageMetadata> stageMetadataMap,
      TableCache tableCache) {
    PhysicalStageInfo info = PhysicalStageTraversalVisitor.go(rootStageNode);
    for (int stageId = stageMetadataMap.size() - 1; stageId >= 0; stageId--) {
      StageNode stageNode = info.getRootStageNode().get(stageId);
      stageNode.visit(new SmartShuffleRewriteVisitor(tableCache, stageMetadataMap), info);
    }
  }

  private SmartShuffleRewriteVisitor(TableCache tableCache, Map<Integer, StageMetadata> stageMetadataMap) {
    _tableCache = tableCache;
    _stageMetadataMap = stageMetadataMap;
    _canSkipShuffleForJoin = false;
  }

  @Override
  public DisjointSet<Integer> visitAggregate(AggregateNode node, PhysicalStageInfo context) {
    DisjointSet<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    Map<Integer, Integer> oldNameMap = new HashMap<>();
    Map<Integer, Integer> newNameMap = new HashMap<>();

    // any input reference directly carries over in group set of aggregation
    // should still be a partition key
    DisjointSet<Integer> partitionKeys = new DisjointSet<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          oldNameMap.put(i, ((RexExpression.InputRef) rex).getIndex());
          newNameMap.put(((RexExpression.InputRef) rex).getIndex(), i);
          partitionKeys.add(i);
        }
      }
    }
    updateDSU(oldPartitionKeys, partitionKeys, oldNameMap, newNameMap);

    return partitionKeys;
  }

  @Override
  public DisjointSet<Integer> visitFilter(FilterNode node, PhysicalStageInfo context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public DisjointSet<Integer> visitJoin(JoinNode node, PhysicalStageInfo context) {
    List<MailboxReceiveNode> innerLeafNodes = context.getLeafNodes().get(node.getStageId()).stream()
        .map(x -> (MailboxReceiveNode) x).collect(Collectors.toList());
    Preconditions.checkState(innerLeafNodes.size() == 2);

    if (canJoinBeColocated(node)) {
      if (canServerAssignmentAllowShuffleSkip(node.getStageId(), innerLeafNodes.get(0).getSenderStageId(),
          innerLeafNodes.get(1).getSenderStageId())) {
        if (canSkipShuffleForJoin(innerLeafNodes.get(0),
            (MailboxSendNode) innerLeafNodes.get(0).getSender(), context)) {
          if (canSkipShuffleForJoin(innerLeafNodes.get(1),
              (MailboxSendNode) innerLeafNodes.get(1).getSender(), context)) {
            _stageMetadataMap.get(node.getStageId()).setServerInstances(
                _stageMetadataMap.get(innerLeafNodes.get(0).getSenderStageId()).getServerInstances());
            _canSkipShuffleForJoin = true;
          }
        }
      }
    }
    // Currently, JOIN criteria is guaranteed to only have one FieldSelectionKeySelector
    FieldSelectionKeySelector leftJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getLeftJoinKeySelector();
    FieldSelectionKeySelector rightJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getRightJoinKeySelector();

    DisjointSet<Integer> leftPKs = node.getInputs().get(0).visit(this, context);
    DisjointSet<Integer> rightPks = node.getInputs().get(1).visit(this, context);

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    DisjointSet<Integer> partitionKeys = new DisjointSet<>();
    for (int i = 0; i < leftJoinKey.getColumnIndices().size(); i++) {
      int leftIdx = leftJoinKey.getColumnIndices().get(i);
      int rightIdx = rightJoinKey.getColumnIndices().get(i);
      int cnt = 0;
      if (leftPKs.contains(leftIdx)) {
        partitionKeys.add(leftIdx);
        cnt++;
      }
      if (rightPks.contains(rightIdx)) {
        // combined schema will have all the left fields before the right fields
        // so add the leftDataSchemaSize before adding the key
        partitionKeys.add(leftDataSchemaSize + rightIdx);
        cnt++;
      }
      if (cnt == 2) {
        partitionKeys.merge(leftIdx, leftDataSchemaSize + rightIdx);
      }
    }
    for (Integer newKey : partitionKeys.getAllMembers()) {
      if (newKey >= leftDataSchemaSize) {
        for (Integer members : rightPks.getMembers(newKey - leftDataSchemaSize)) {
          if (partitionKeys.contains(members + leftDataSchemaSize)) {
            partitionKeys.merge(newKey, members + leftDataSchemaSize);
          }
        }
      } else {
        for (Integer members : leftPKs.getMembers(newKey)) {
          if (partitionKeys.contains(members)) {
            partitionKeys.merge(newKey, members);
          }
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public DisjointSet visitMailboxReceive(MailboxReceiveNode node, PhysicalStageInfo context) {
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();
    DisjointSet<Integer> oldPartitionKeys = context.getPartitionKeys(node.getSenderStageId());
    // If the current stage is not a join-stage, then we already know sender's distribution
    if (!context.isJoinStage(node.getStageId())) {
      if (selector == null) {
        return new DisjointSet<>();
      } else if (canSkipShuffle(oldPartitionKeys, selector)) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        return oldPartitionKeys;
      }
      return new DisjointSet<>(((FieldSelectionKeySelector) selector).getColumnIndices());
    }
    // If the current stage is a join-stage then we haven't determined distribution for sender and we already know
    // whether shuffle can be skipped.
    if (_canSkipShuffleForJoin) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      ((MailboxSendNode) node.getSender()).setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else if (selector == null) {
      return new DisjointSet<>();
    }
    return new DisjointSet<>(((FieldSelectionKeySelector) selector).getColumnIndices());
  }

  @Override
  public DisjointSet<Integer> visitMailboxSend(MailboxSendNode node, PhysicalStageInfo context) {
    DisjointSet<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    boolean canSkipShuffleBasic = canSkipShuffle(oldPartitionKeys, selector);
    // If receiver is not a join-stage, then we can determine distribution type now.
    if (!context.isJoinStage(node.getReceiverStageId())) {
      DisjointSet<Integer> partitionKeys;
      if (canSkipShuffleBasic) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        partitionKeys = oldPartitionKeys;
      } else {
        partitionKeys = new DisjointSet<>();
      }
      context.setPartitionKeys(node.getStageId(), partitionKeys);
      return partitionKeys;
    }
    // If receiver is a join-stage, remember partition-keys of the child node of MailboxSendNode.
    DisjointSet<Integer> mailboxSendPartitionKeys = canSkipShuffleBasic ? oldPartitionKeys : new DisjointSet<>();
    context.setPartitionKeys(node.getStageId(), mailboxSendPartitionKeys);
    return mailboxSendPartitionKeys;
  }

  @Override
  public DisjointSet<Integer> visitProject(ProjectNode node, PhysicalStageInfo context) {
    DisjointSet<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // all inputs carry over if they're still in the projection result
    DisjointSet<Integer> partitionKeys = new DisjointSet<>();
    Map<Integer, Integer> oldNameMap = new HashMap<>();
    Map<Integer, Integer> newNameMap = new HashMap<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          oldNameMap.put(i, ((RexExpression.InputRef) rex).getIndex());
          newNameMap.put(((RexExpression.InputRef) rex).getIndex(), i);
          partitionKeys.add(i);
        }
      }
    }
    updateDSU(oldPartitionKeys, partitionKeys, oldNameMap, newNameMap);

    return partitionKeys;
  }

  @Override
  public DisjointSet<Integer> visitSort(SortNode node, PhysicalStageInfo context) {
    // sort doesn't change the partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public DisjointSet<Integer> visitTableScan(TableScanNode node, PhysicalStageInfo context) {
    TableConfig tableConfig =
        _tableCache.getTableConfig(node.getTableName());
    Preconditions.checkNotNull(tableConfig, "table config is null");
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.getSegmentPartitionConfig() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap =
          indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap();
      if (columnPartitionMap != null) {
        Set<String> partitionColumns = columnPartitionMap.keySet();
        DisjointSet<Integer> newPartitionKeys = new DisjointSet<>();
        for (int i = 0; i < node.getTableScanColumns().size(); i++) {
          if (partitionColumns.contains(node.getTableScanColumns().get(i))) {
            newPartitionKeys.add(i);
          }
        }
        return newPartitionKeys;
      }
    }
    return new DisjointSet<>();
  }

  @Override
  public DisjointSet<Integer> visitValue(ValueNode node, PhysicalStageInfo context) {
    return new DisjointSet<>();
  }

  private boolean canServerAssignmentAllowShuffleSkip(int currentStageId, int leftStageId, int rightStageId) {
    Set<ServerInstance> leftServerInstances = new HashSet<>(_stageMetadataMap.get(leftStageId).getServerInstances());
    List<ServerInstance> rightServerInstances = _stageMetadataMap.get(rightStageId).getServerInstances();
    List<ServerInstance> currentServerInstances = _stageMetadataMap.get(currentStageId).getServerInstances();
    return leftServerInstances.containsAll(rightServerInstances)
        && leftServerInstances.size() == rightServerInstances.size()
        && currentServerInstances.containsAll(leftServerInstances);
  }

  private boolean canSkipShuffleForJoin(MailboxReceiveNode mailboxReceiveNode, MailboxSendNode mailboxSendNode,
      PhysicalStageInfo context) {
    DisjointSet<Integer> oldPartitionKeys = context.getPartitionKeys(mailboxSendNode.getStageId());
    KeySelector<Object[], Object[]> selector = mailboxSendNode.getPartitionKeySelector();
    if (!canSkipShuffle(oldPartitionKeys, selector)) {
      return false;
    }
    // Since shuffle can be skipped, oldPartitionsKeys == senderPartitionKeys
    selector = mailboxReceiveNode.getPartitionKeySelector();
    return canSkipShuffle(oldPartitionKeys, selector);
  }

  private void updateDSU(DisjointSet<Integer> oldPartitionKeys, DisjointSet<Integer> newPartitionKeys,
      Map<Integer, Integer> oldNameMap,
      Map<Integer, Integer> newNameMap) {
    for (Integer retainedKey : newPartitionKeys.getAllMembers()) {
      int oldName = oldNameMap.get(retainedKey);
      for (Integer otherMember : oldPartitionKeys.getMembers(oldName)) {
        int newOtherMemberName = newNameMap.getOrDefault(otherMember, -1);
        if (newOtherMemberName != -1) {
          newPartitionKeys.merge(retainedKey, newOtherMemberName);
        }
      }
    }
  }

  // Equality joins can be colocated.
  private boolean canJoinBeColocated(JoinNode joinNode) {
    return joinNode.getJoinClauses().size() == 1 && joinNode.getJoinClauses().get(0).getKind().equals(SqlKind.EQUALS);
  }

  private boolean canSkipShuffle(DisjointSet<Integer> partitionKeys, KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      Set<Integer> targetSet = new HashSet<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (Integer senderPkey : partitionKeys.getAllMembers()) {
        if (!targetSet.contains(senderPkey)) {
          boolean containsEquivalentKey = false;
          for (Integer equivalentKey : partitionKeys.getMembers(senderPkey)) {
            if (targetSet.contains(equivalentKey)) {
              containsEquivalentKey = true;
              break;
            }
          }
          if (!containsEquivalentKey) {
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }

  static class PartitionKey {
    private int _index;
    private int _numPartitions;
    private String _hashAlgorithm;

    public PartitionKey(int index, int numPartitions) {
      _index = index;
      _numPartitions = numPartitions;
      _hashAlgorithm = "GREAT_ALGO"; // TODO: fix this.
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionKey that = (PartitionKey) o;
      return _index == that._index && _numPartitions == that._numPartitions && _hashAlgorithm
          .equals(that._hashAlgorithm);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_index, _numPartitions, _hashAlgorithm);
    }
  }
}
