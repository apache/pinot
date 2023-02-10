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
package org.apache.pinot.query.planner.physical.colocated;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
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
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This shuffle optimizer can avoid shuffles by taking into account all of the following:
 *
 * 1. Servers assigned to the stages. The optimizer may also choose to change the server assignment if skipping
 *    shuffles is possible.
 * 2. The hash-algorithm and the number of partitions of the data in sender/receiver nodes. So for instance if we do a
 *    join on two tables where the left table is partitioned using Murmur but the right table is partitioned using
 *    hashCode, then this optimizer can detect this case and keep the shuffle.
 *
 * Also see: {@link ColocationKey} for its definition.
 */
public class GreedyShuffleRewriteVisitor
    implements StageNodeVisitor<Set<ColocationKey>, GreedyShuffleRewriteContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GreedyShuffleRewriteVisitor.class);

  private final TableCache _tableCache;
  private final Map<Integer, StageMetadata> _stageMetadataMap;
  private boolean _canSkipShuffleForJoin;

  public static void optimizeShuffles(QueryPlan queryPlan, TableCache tableCache) {
    StageNode rootStageNode = queryPlan.getQueryStageMap().get(0);
    Map<Integer, StageMetadata> stageMetadataMap = queryPlan.getStageMetadataMap();
    GreedyShuffleRewriteContext context = GreedyShuffleRewritePreComputeVisitor.preComputeContext(rootStageNode);
    // This assumes that if stageId(S1) > stageId(S2), then S1 is not an ancestor of S2.
    // TODO: If this assumption is wrong, we can compute the reverse topological ordering explicitly.
    for (int stageId = stageMetadataMap.size() - 1; stageId >= 0; stageId--) {
      StageNode stageNode = context.getRootStageNode(stageId);
      stageNode.visit(new GreedyShuffleRewriteVisitor(tableCache, stageMetadataMap), context);
    }
  }

  private GreedyShuffleRewriteVisitor(TableCache tableCache, Map<Integer, StageMetadata> stageMetadataMap) {
    _tableCache = tableCache;
    _stageMetadataMap = stageMetadataMap;
    _canSkipShuffleForJoin = false;
  }

  @Override
  public Set<ColocationKey> visitAggregate(AggregateNode node, GreedyShuffleRewriteContext context) {
    Set<ColocationKey> oldColocationKeys = node.getInputs().get(0).visit(this, context);

    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        oldToNewIndex.put(index, i);
      }
    }

    return computeNewColocationKeys(oldColocationKeys, oldToNewIndex);
  }

  @Override
  public Set<ColocationKey> visitFilter(FilterNode node, GreedyShuffleRewriteContext context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<ColocationKey> visitJoin(JoinNode node, GreedyShuffleRewriteContext context) {
    List<MailboxReceiveNode> innerLeafNodes = context.getLeafNodes(node.getStageId()).stream()
        .map(x -> (MailboxReceiveNode) x).collect(Collectors.toList());
    Preconditions.checkState(innerLeafNodes.size() == 2);

    // Multiple checks need to be made to ensure that shuffle can be skipped for a join.
    // Step-1: Join can be skipped only for equality joins.
    boolean canColocate = canJoinBeColocated(node);
    // Step-2: Only if the servers assigned to both left and right nodes are equal and the servers assigned to the join
    //         stage are a superset of those servers, can we skip shuffles.
    canColocate = canColocate && canServerAssignmentAllowShuffleSkip(node.getStageId(),
        innerLeafNodes.get(0).getSenderStageId(), innerLeafNodes.get(1).getSenderStageId());
    // Step-3: For both left/right MailboxReceiveNode/MailboxSendNode pairs, check whether the key partitioning can
    //         allow shuffle skip.
    canColocate = canColocate && partitionKeyConditionForJoin(innerLeafNodes.get(0),
        (MailboxSendNode) innerLeafNodes.get(0).getSender(), context);
    canColocate = canColocate && partitionKeyConditionForJoin(innerLeafNodes.get(1),
        (MailboxSendNode) innerLeafNodes.get(1).getSender(), context);
    // Step-4: Finally, ensure that the number of partitions and the hash algorithm is same for partition keys of both
    //         children.
    canColocate = canColocate && checkPartitionScheme(innerLeafNodes.get(0), innerLeafNodes.get(1), context);
    if (canColocate) {
      // If shuffle can be skipped, reassign servers.
      _stageMetadataMap.get(node.getStageId()).setServerInstances(
          _stageMetadataMap.get(innerLeafNodes.get(0).getSenderStageId()).getServerInstances());
      _canSkipShuffleForJoin = true;
    }

    Set<ColocationKey> leftPKs = node.getInputs().get(0).visit(this, context);
    Set<ColocationKey> rightPks = node.getInputs().get(1).visit(this, context);

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    Set<ColocationKey> colocationKeys = new HashSet<>(leftPKs);

    for (ColocationKey rightColocationKey : rightPks) {
      ColocationKey newColocationKey = new ColocationKey(rightColocationKey.getNumPartitions(),
          rightColocationKey.getHashAlgorithm());
      for (Integer index : rightColocationKey.getIndices()) {
        newColocationKey.addIndex(leftDataSchemaSize + index);
      }
      colocationKeys.add(newColocationKey);
    }

    return colocationKeys;
  }

  @Override
  public Set<ColocationKey> visitMailboxReceive(MailboxReceiveNode node, GreedyShuffleRewriteContext context) {
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();
    Set<ColocationKey> oldColocationKeys = context.getColocationKeys(node.getSenderStageId());
    // If the current stage is not a join-stage, then we already know sender's distribution
    if (!context.isJoinStage(node.getStageId())) {
      if (selector == null) {
        return new HashSet<>();
      } else if (colocationKeyCondition(oldColocationKeys, selector)
          && areServersSuperset(node.getStageId(), node.getSenderStageId())) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        _stageMetadataMap.get(node.getStageId()).setServerInstances(
            _stageMetadataMap.get(node.getSenderStageId()).getServerInstances());
        return oldColocationKeys;
      }
      // This means we can't skip shuffle and there's a partitioning enforced by receiver.
      int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
      List<ColocationKey> colocationKeys =
          ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
              .map(x -> new ColocationKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
      return new HashSet<>(colocationKeys);
    }
    // If the current stage is a join-stage then we already know whether shuffle can be skipped.
    if (_canSkipShuffleForJoin) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      // For the join-case, servers are already re-assigned in visitJoin. Moreover, we haven't yet changed sender's
      // distribution.
      ((MailboxSendNode) node.getSender()).setExchangeType(RelDistribution.Type.SINGLETON);
      return oldColocationKeys;
    } else if (selector == null) {
      return new HashSet<>();
    }
    // This means we can't skip shuffle and there's a partitioning enforced by receiver.
    int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
    List<ColocationKey> colocationKeys =
        ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
            .map(x -> new ColocationKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
    return new HashSet<>(colocationKeys);
  }

  @Override
  public Set<ColocationKey> visitMailboxSend(MailboxSendNode node, GreedyShuffleRewriteContext context) {
    Set<ColocationKey> oldColocationKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    boolean canSkipShuffleBasic = colocationKeyCondition(oldColocationKeys, selector);
    // If receiver is not a join-stage, then we can determine distribution type now.
    if (!context.isJoinStage(node.getReceiverStageId())) {
      Set<ColocationKey> colocationKeys;
      if (canSkipShuffleBasic && areServersSuperset(node.getReceiverStageId(), node.getStageId())) {
        // Servers are not re-assigned on sender-side. If needed, they are re-assigned on the receiver side.
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        colocationKeys = oldColocationKeys;
      } else {
        colocationKeys = new HashSet<>();
      }
      context.setColocationKeys(node.getStageId(), colocationKeys);
      return colocationKeys;
    }
    // If receiver is a join-stage, remember partition-keys of the child node of MailboxSendNode.
    Set<ColocationKey> mailboxSendColocationKeys = canSkipShuffleBasic ? oldColocationKeys : new HashSet<>();
    context.setColocationKeys(node.getStageId(), mailboxSendColocationKeys);
    return mailboxSendColocationKeys;
  }

  @Override
  public Set<ColocationKey> visitProject(ProjectNode node, GreedyShuffleRewriteContext context) {
    // Project reorders or removes keys
    Set<ColocationKey> oldColocationKeys = node.getInputs().get(0).visit(this, context);

    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        oldToNewIndex.put(index, i);
      }
    }

    return computeNewColocationKeys(oldColocationKeys, oldToNewIndex);
  }

  @Override
  public Set<ColocationKey> visitSort(SortNode node, GreedyShuffleRewriteContext context) {
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<ColocationKey> visitWindow(WindowNode node, GreedyShuffleRewriteContext context) {
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<ColocationKey> visitTableScan(TableScanNode node, GreedyShuffleRewriteContext context) {
    TableConfig tableConfig =
        _tableCache.getTableConfig(node.getTableName());
    if (tableConfig == null) {
      LOGGER.warn("Couldn't find tableConfig for {}", node.getTableName());
      return new HashSet<>();
    }
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.getSegmentPartitionConfig() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap =
          indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap();
      if (columnPartitionMap != null) {
        Set<String> partitionColumns = columnPartitionMap.keySet();
        Set<ColocationKey> newColocationKeys = new HashSet<>();
        for (int i = 0; i < node.getTableScanColumns().size(); i++) {
          String columnName = node.getTableScanColumns().get(i);
          if (partitionColumns.contains(node.getTableScanColumns().get(i))) {
            int numPartitions = columnPartitionMap.get(columnName).getNumPartitions();
            String hashAlgorithm = columnPartitionMap.get(columnName).getFunctionName();
            newColocationKeys.add(new ColocationKey(i, numPartitions, hashAlgorithm));
          }
        }
        return newColocationKeys;
      }
    }
    return new HashSet<>();
  }

  @Override
  public Set<ColocationKey> visitValue(ValueNode node, GreedyShuffleRewriteContext context) {
    return new HashSet<>();
  }

  // TODO: Only equality joins can be colocated. We don't have join clause info available right now.
  private boolean canJoinBeColocated(JoinNode joinNode) {
    return true;
  }

  /**
   * Checks if servers assigned to the receiver stage are a super-set of the sender stage.
   */
  private boolean areServersSuperset(int receiverStageId, int senderStageId) {
    return _stageMetadataMap.get(receiverStageId).getServerInstances().containsAll(
        _stageMetadataMap.get(senderStageId).getServerInstances());
  }

  /*
   * We allow shuffle skip only when both of the following conditions are met:
   * 1. Left and right stage have the same servers (say S).
   * 2. Servers assigned to the join-stage are a superset of S.
   */
  private boolean canServerAssignmentAllowShuffleSkip(int currentStageId, int leftStageId, int rightStageId) {
    Set<VirtualServer> leftServerInstances = new HashSet<>(_stageMetadataMap.get(leftStageId).getServerInstances());
    List<VirtualServer> rightServerInstances = _stageMetadataMap.get(rightStageId).getServerInstances();
    List<VirtualServer> currentServerInstances = _stageMetadataMap.get(currentStageId).getServerInstances();
    return leftServerInstances.containsAll(rightServerInstances)
        && leftServerInstances.size() == rightServerInstances.size()
        && currentServerInstances.containsAll(leftServerInstances);
  }

  /**
   * Given the existing partitioning keys in oldPartitionsKeys, it takes the mapping from the old index to the new index
   * and the computes the new partition keys. Any partition key that has an index that is not present in oldToNewIndex
   * is dropped. If all indices of a ColocationKey are present in oldToNewIndex, we keep that partition key in the new
   * keys and change the name using the map.
   */
  private static Set<ColocationKey> computeNewColocationKeys(Set<ColocationKey> oldColocationKeys,
      Map<Integer, Integer> oldToNewIndex) {
    Set<ColocationKey> colocationKeys = new HashSet<>();
    for (ColocationKey colocationKey : oldColocationKeys) {
      boolean shouldDrop = false;
      ColocationKey newColocationKey
          = new ColocationKey(colocationKey.getNumPartitions(), colocationKey.getHashAlgorithm());
      for (Integer index: colocationKey.getIndices()) {
        if (!oldToNewIndex.containsKey(index)) {
          shouldDrop = true;
          break;
        }
        newColocationKey.addIndex(oldToNewIndex.get(index));
      }
      if (!shouldDrop) {
        colocationKeys.add(newColocationKey);
      }
    }

    return colocationKeys;
  }

  private static boolean colocationKeyCondition(Set<ColocationKey> colocationKeys,
      KeySelector<Object[], Object[]> keySelector) {
    if (!colocationKeys.isEmpty() && keySelector != null) {
      List<Integer> targetSet = new ArrayList<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (ColocationKey colocationKey : colocationKeys) {
        if (targetSet.size() >= colocationKey.getIndices().size() && targetSet.subList(0,
            colocationKey.getIndices().size()).equals(colocationKey.getIndices())) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean partitionKeyConditionForJoin(MailboxReceiveNode mailboxReceiveNode,
      MailboxSendNode mailboxSendNode,
      GreedyShuffleRewriteContext context) {
    // First check ColocationKeyCondition for the sender <--> sender.getInputs().get(0) pair
    Set<ColocationKey> oldColocationKeys = context.getColocationKeys(mailboxSendNode.getStageId());
    KeySelector<Object[], Object[]> selector = mailboxSendNode.getPartitionKeySelector();
    if (!colocationKeyCondition(oldColocationKeys, selector)) {
      return false;
    }
    // Check ColocationKeyCondition for the sender <--> receiver pair
    // Since shuffle can be skipped, oldPartitionsKeys == senderColocationKeys
    selector = mailboxReceiveNode.getPartitionKeySelector();
    return colocationKeyCondition(oldColocationKeys, selector);
  }

  private static ColocationKey getEquivalentSenderKey(Set<ColocationKey> colocationKeys,
      KeySelector<Object[], Object[]> keySelector) {
    if (!colocationKeys.isEmpty() && keySelector != null) {
      List<Integer> targetSet = new ArrayList<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (ColocationKey colocationKey : colocationKeys) {
        if (targetSet.size() >= colocationKey.getIndices().size() && targetSet.subList(0,
            colocationKey.getIndices().size()).equals(colocationKey.getIndices())) {
          return colocationKey;
        }
      }
    }
    throw new IllegalStateException("Receiver's Equivalent Key in Sender Can't be Determined. This indicates a bug.");
  }

  private static boolean checkPartitionScheme(MailboxReceiveNode leftReceiveNode, MailboxReceiveNode rightReceiveNode,
      GreedyShuffleRewriteContext context) {
    int leftSender = leftReceiveNode.getSenderStageId();
    int rightSender = rightReceiveNode.getSenderStageId();
    ColocationKey leftPKey = getEquivalentSenderKey(context.getColocationKeys(leftSender),
        leftReceiveNode.getPartitionKeySelector());
    ColocationKey rightPKey = getEquivalentSenderKey(context.getColocationKeys(rightSender),
        rightReceiveNode.getPartitionKeySelector());
    if (leftPKey.getNumPartitions() != rightPKey.getNumPartitions()) {
      return false;
    }
    return leftPKey.getHashAlgorithm().equals(rightPKey.getHashAlgorithm());
  }
}
