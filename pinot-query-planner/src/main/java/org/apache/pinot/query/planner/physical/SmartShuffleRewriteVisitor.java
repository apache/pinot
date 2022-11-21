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
import org.apache.commons.collections.CollectionUtils;
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
 * Some trivial implementation assumptions (very easy to remove):
 * 1. If a stage A depends on stage B, stageId(A) < stageId(B)
 * 2. Leaf stage can either be a MailboxReceiveNode or TableScanNode.
 */
public class SmartShuffleRewriteVisitor
    implements StageNodeVisitor<DisjointSet<SmartShuffleRewriteVisitor.PartitionKey>, PhysicalStageInfo> {
  private final TableCache _tableCache;
  private final Map<Integer, StageMetadata> _stageMetadataMap;
  private boolean _canSkipShuffleForJoin;

  /**
   * A shuffle optimizer that can avoid shuffles by taking into account all of the following:
   * 1. Servers assigned to the stages. The optimizer may also choose to change the server assignment if skipping
   *    shuffles is possible.
   * 2. The hash-algorithm and physical number of partitions of the data in sender/receiver nodes
   *    So for instance if we do a join on two tables where the left table is partitioned using Murmur but the
   *    right table is partitioned using hashCode, then this optimizer can detect this case and keep the shuffle.
   * 3. Equivalent partitioning introduced by doing a join. e.g. if we do a join on two tables on a user_uuid column,
   *    then the join-stage will have two partitioning keys: leftTable.userUUID and rightTable.userUUID. If the parent
   *    of the join stage is another join with the userUUID column of the leftTable, then we should be able to skip
   *    shuffle again. However to do that we need to detect that two keys are equivalent, and for that we use a
   *    DisjointSet.
   */
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
  public DisjointSet<PartitionKey> visitAggregate(AggregateNode node, PhysicalStageInfo context) {
    DisjointSet<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    Map<Integer, Integer> oldNameMap = new HashMap<>();
    Map<Integer, Integer> newNameMap = new HashMap<>();

    // any input reference directly carries over in group set of aggregation
    // should still be a partition key
    DisjointSet<PartitionKey> partitionKeys = new DisjointSet<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        List<PartitionKey> oldPartitionKey =
            oldPartitionKeys.getAllMembers().stream().filter(x -> x.getIndex() == index).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(oldPartitionKey)) {
          oldNameMap.put(i, index);
          newNameMap.put(index, i);
          partitionKeys.add(oldPartitionKey.get(0).withIndex(i));
        }
      }
    }
    updateDSU(oldPartitionKeys, partitionKeys, oldNameMap, newNameMap);

    return partitionKeys;
  }

  @Override
  public DisjointSet<PartitionKey> visitFilter(FilterNode node, PhysicalStageInfo context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public DisjointSet<PartitionKey> visitJoin(JoinNode node, PhysicalStageInfo context) {
    List<MailboxReceiveNode> innerLeafNodes = context.getLeafNodes().get(node.getStageId()).stream()
        .map(x -> (MailboxReceiveNode) x).collect(Collectors.toList());
    Preconditions.checkState(innerLeafNodes.size() == 2);

    // Multiple checks need to be made to ensure that shuffle can be skipped for a join.
    // Step-1: Join can be skipped only for equality joins.
    boolean canColocate = canJoinBeColocated(node);
    // Step-2: Only if the servers assigned to both left and right nodes are equal and the servers assigned to the join
    //         stage are a superset of those servers, can we skip shuffles.
    canColocate &= canServerAssignmentAllowShuffleSkip(node.getStageId(), innerLeafNodes.get(0).getSenderStageId(),
        innerLeafNodes.get(1).getSenderStageId());
    // Step-3: For both left/right MailboxReceiveNode/MailboxSendNode pairs, check whether the key partitioning can
    //         allow shuffle skip.
    canColocate &= canSkipShuffleForJoin(innerLeafNodes.get(0),
        (MailboxSendNode) innerLeafNodes.get(0).getSender(), context);
    canColocate &= canSkipShuffleForJoin(innerLeafNodes.get(1),
        (MailboxSendNode) innerLeafNodes.get(1).getSender(), context);
    // Step-4: Finally, ensure that the number of partitions and the hash algorithm is same for pkeys of both children.
    canColocate &= checkPartitionScheme(innerLeafNodes.get(0), innerLeafNodes.get(1), context);
    if (canColocate) {
      // If shuffle can be skipped, reassign servers.
      _stageMetadataMap.get(node.getStageId()).setServerInstances(
          _stageMetadataMap.get(innerLeafNodes.get(0).getSenderStageId()).getServerInstances());
      _canSkipShuffleForJoin = true;
    }
    // Currently, JOIN criteria is guaranteed to only have one FieldSelectionKeySelector
    FieldSelectionKeySelector leftJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getLeftJoinKeySelector();
    FieldSelectionKeySelector rightJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getRightJoinKeySelector();

    DisjointSet<PartitionKey> leftPKs = node.getInputs().get(0).visit(this, context);
    DisjointSet<PartitionKey> rightPks = node.getInputs().get(1).visit(this, context);

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    DisjointSet<PartitionKey> partitionKeys = new DisjointSet<>();
    for (int i = 0; i < leftJoinKey.getColumnIndices().size(); i++) {
      int leftIdx = leftJoinKey.getColumnIndices().get(i);
      int rightIdx = rightJoinKey.getColumnIndices().get(i);
      List<PartitionKey> leftPartitionKey = leftPKs.getMatchingElements((x) -> x.getIndex() == leftIdx);
      if (CollectionUtils.isNotEmpty(leftPartitionKey)) {
        partitionKeys.add(leftPartitionKey.get(0));
      }
      List<PartitionKey> rightPartitionKey = rightPks.getMatchingElements((x) -> x.getIndex() == rightIdx);
      if (CollectionUtils.isNotEmpty(rightPartitionKey)) {
        // combined schema will have all the left fields before the right fields
        // so add the leftDataSchemaSize before adding the key
        partitionKeys.add(rightPartitionKey.get(0).withIndex(leftDataSchemaSize + rightIdx));
      }
      if (CollectionUtils.isNotEmpty(leftPartitionKey) && CollectionUtils.isNotEmpty(rightPartitionKey)) {
        partitionKeys.merge(leftPartitionKey.get(0), rightPartitionKey.get(0).withIndex(leftDataSchemaSize + rightIdx));
      }
    }
    for (PartitionKey newKey : partitionKeys.getAllMembers()) {
      if (newKey.getIndex() >= leftDataSchemaSize) {
        for (PartitionKey members : rightPks.getMembers(newKey.withIndex(newKey.getIndex() - leftDataSchemaSize))) {
          List<PartitionKey> temp =
              partitionKeys.getMatchingElements((x) -> x.getIndex() == members.getIndex() + leftDataSchemaSize);
          if (CollectionUtils.isNotEmpty(temp)) {
            partitionKeys.merge(newKey, temp.get(0));
          }
        }
      } else {
        for (PartitionKey members : leftPKs.getMembers(newKey)) {
          if (partitionKeys.contains(members)) {
            partitionKeys.merge(newKey, members);
          }
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public DisjointSet<PartitionKey> visitMailboxReceive(MailboxReceiveNode node, PhysicalStageInfo context) {
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();
    DisjointSet<PartitionKey> oldPartitionKeys = context.getPartitionKeys(node.getSenderStageId());
    // If the current stage is not a join-stage, then we already know sender's distribution
    if (!context.isJoinStage(node.getStageId())) {
      if (selector == null) {
        return new DisjointSet<>();
      } else if (canMailboxOpSkipShuffle(oldPartitionKeys, selector)) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        return oldPartitionKeys;
      }
      // This means we can't skip shuffle and there's a partitioning enforced by receiver.
      int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
      List<PartitionKey> partitionKeys =
          ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
              .map(x -> new PartitionKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
      return new DisjointSet<>(partitionKeys);
    }
    // If the current stage is a join-stage then we already know whether shuffle can be skipped.
    if (_canSkipShuffleForJoin) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      ((MailboxSendNode) node.getSender()).setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else if (selector == null) {
      return new DisjointSet<>();
    }
    // This means we can't skip shuffle and there's a partitioning enforced by receiver.
    int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
    List<PartitionKey> partitionKeys =
        ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
            .map(x -> new PartitionKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
    return new DisjointSet<>(partitionKeys);
  }

  @Override
  public DisjointSet<PartitionKey> visitMailboxSend(MailboxSendNode node, PhysicalStageInfo context) {
    DisjointSet<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    boolean canSkipShuffleBasic = canMailboxOpSkipShuffle(oldPartitionKeys, selector);
    // If receiver is not a join-stage, then we can determine distribution type now.
    if (!context.isJoinStage(node.getReceiverStageId())) {
      DisjointSet<PartitionKey> partitionKeys;
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
    DisjointSet<PartitionKey> mailboxSendPartitionKeys = canSkipShuffleBasic ? oldPartitionKeys : new DisjointSet<>();
    context.setPartitionKeys(node.getStageId(), mailboxSendPartitionKeys);
    return mailboxSendPartitionKeys;
  }

  @Override
  public DisjointSet<PartitionKey> visitProject(ProjectNode node, PhysicalStageInfo context) {
    DisjointSet<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // all inputs carry over if they're still in the projection result
    DisjointSet<PartitionKey> partitionKeys = new DisjointSet<>();
    Map<Integer, Integer> oldNameMap = new HashMap<>();
    Map<Integer, Integer> newNameMap = new HashMap<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        List<PartitionKey> temp = oldPartitionKeys.getMatchingElements((x) -> x.getIndex() == index);
        if (!temp.isEmpty()) {
          oldNameMap.put(i, index);
          newNameMap.put(index, i);
          partitionKeys.add(temp.get(0).withIndex(i));
        }
      }
    }
    updateDSU(oldPartitionKeys, partitionKeys, oldNameMap, newNameMap);

    return partitionKeys;
  }

  @Override
  public DisjointSet<PartitionKey> visitSort(SortNode node, PhysicalStageInfo context) {
    // sort doesn't change the partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public DisjointSet<PartitionKey> visitTableScan(TableScanNode node, PhysicalStageInfo context) {
    TableConfig tableConfig =
        _tableCache.getTableConfig(node.getTableName());
    Preconditions.checkNotNull(tableConfig, "table config is null");
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.getSegmentPartitionConfig() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap =
          indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap();
      if (columnPartitionMap != null) {
        Set<String> partitionColumns = columnPartitionMap.keySet();
        DisjointSet<PartitionKey> newPartitionKeys = new DisjointSet<>();
        for (int i = 0; i < node.getTableScanColumns().size(); i++) {
          String columnName = node.getTableScanColumns().get(i);
          if (partitionColumns.contains(node.getTableScanColumns().get(i))) {
            int numPartitions = columnPartitionMap.get(columnName).getNumPartitions();
            String hashAlgorithm = columnPartitionMap.get(columnName).getFunctionName();
            newPartitionKeys.add(new PartitionKey(i, numPartitions, hashAlgorithm));
          }
        }
        return newPartitionKeys;
      }
    }
    return new DisjointSet<>();
  }

  @Override
  public DisjointSet<PartitionKey> visitValue(ValueNode node, PhysicalStageInfo context) {
    return new DisjointSet<>();
  }

  /*
   * We allow shuffle skip only when both of the following conditions are met:
   * 1. Left and right stage have the same servers (say S).
   * 2. Servers assigned to the join-stage are a superset of S.
   */
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
    DisjointSet<PartitionKey> oldPartitionKeys = context.getPartitionKeys(mailboxSendNode.getStageId());
    KeySelector<Object[], Object[]> selector = mailboxSendNode.getPartitionKeySelector();
    if (!canMailboxOpSkipShuffle(oldPartitionKeys, selector)) {
      return false;
    }
    // Since shuffle can be skipped, oldPartitionsKeys == senderPartitionKeys
    selector = mailboxReceiveNode.getPartitionKeySelector();
    return canMailboxOpSkipShuffle(oldPartitionKeys, selector);
  }

  private boolean checkPartitionScheme(MailboxReceiveNode leftReceiveNode, MailboxReceiveNode rightReceiveNode,
      PhysicalStageInfo context) {
    int leftSender = leftReceiveNode.getSenderStageId();
    int rightSender = rightReceiveNode.getSenderStageId();
    PartitionKey leftKey = context.getPartitionKeys(leftSender).getAllMembers().get(0);
    PartitionKey rightKey = context.getPartitionKeys(rightSender).getAllMembers().get(0);
    if (leftKey.getNumPartitions() != rightKey.getNumPartitions()) {
      return false;
    }
    return leftKey.getHashAlgorithm().equals(rightKey.getHashAlgorithm());
  }

  private void updateDSU(DisjointSet<PartitionKey> oldPartitionKeys, DisjointSet<PartitionKey> newPartitionKeys,
      Map<Integer, Integer> oldNameMap,
      Map<Integer, Integer> newNameMap) {
    for (PartitionKey retainedKey : newPartitionKeys.getAllMembers()) {
      int oldName = oldNameMap.get(retainedKey.getIndex());
      for (PartitionKey otherMember : oldPartitionKeys.getMembers(retainedKey.withIndex(oldName))) {
        int newOtherMemberName = newNameMap.getOrDefault(otherMember.getIndex(), -1);
        if (newOtherMemberName != -1) {
          newPartitionKeys.merge(retainedKey, otherMember.withIndex(newOtherMemberName));
        }
      }
    }
  }

  // TODO: Only equality joins can be colocated. We don't have join clause right now.
  private boolean canJoinBeColocated(JoinNode joinNode) {
    return true;
  }

  private boolean canMailboxOpSkipShuffle(DisjointSet<PartitionKey> partitionKeys,
      KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      Set<Integer> targetSet = new HashSet<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (PartitionKey senderPkey : partitionKeys.getAllMembers()) {
        if (!targetSet.contains(senderPkey.getIndex())) {
          boolean containsEquivalentKey = false;
          for (PartitionKey equivalentKey : partitionKeys.getMembers(senderPkey)) {
            if (targetSet.contains(equivalentKey.getIndex())) {
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

  public static class PartitionKey {
    private int _index;
    private int _numPartitions;
    private String _hashAlgorithm;

    public int getIndex() {
      return _index;
    }

    public int getNumPartitions() {
      return _numPartitions;
    }

    public String getHashAlgorithm() {
      return _hashAlgorithm;
    }

    public PartitionKey(int index, int numPartitions, String algorithm) {
      _index = index;
      _numPartitions = numPartitions;
      _hashAlgorithm = algorithm;
    }

    public PartitionKey withIndex(int index) {
      return new PartitionKey(index, _numPartitions, _hashAlgorithm);
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
