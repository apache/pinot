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
import org.apache.pinot.core.transport.ServerInstance;
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
 * Also see: {@link PartitionKey} for its definition.
 */
public class GreedyShuffleRewriteVisitor
    implements StageNodeVisitor<Set<PartitionKey>, GreedyShuffleRewriteContext> {
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
  public Set<PartitionKey> visitAggregate(AggregateNode node, GreedyShuffleRewriteContext context) {
    Set<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        oldToNewIndex.put(index, i);
      }
    }

    return computeNewPartitionKeys(oldPartitionKeys, oldToNewIndex);
  }

  @Override
  public Set<PartitionKey> visitFilter(FilterNode node, GreedyShuffleRewriteContext context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<PartitionKey> visitJoin(JoinNode node, GreedyShuffleRewriteContext context) {
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

    Set<PartitionKey> leftPKs = node.getInputs().get(0).visit(this, context);
    Set<PartitionKey> rightPks = node.getInputs().get(1).visit(this, context);

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    Set<PartitionKey> partitionKeys = new HashSet<>(leftPKs);

    for (PartitionKey rightPartitionKey : rightPks) {
      PartitionKey newPartitionKey = new PartitionKey(rightPartitionKey.getNumPartitions(),
          rightPartitionKey.getHashAlgorithm());
      for (Integer index : rightPartitionKey.getIndices()) {
        newPartitionKey.addIndex(leftDataSchemaSize + index);
      }
      partitionKeys.add(newPartitionKey);
    }

    return partitionKeys;
  }

  @Override
  public Set<PartitionKey> visitMailboxReceive(MailboxReceiveNode node, GreedyShuffleRewriteContext context) {
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();
    Set<PartitionKey> oldPartitionKeys = context.getPartitionKeys(node.getSenderStageId());
    // If the current stage is not a join-stage, then we already know sender's distribution
    if (!context.isJoinStage(node.getStageId())) {
      if (selector == null) {
        return new HashSet<>();
      } else if (partitionKeyCondition(oldPartitionKeys, selector)
          && areServersSuperset(node.getStageId(), node.getSenderStageId())) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        _stageMetadataMap.get(node.getStageId()).setServerInstances(
            _stageMetadataMap.get(node.getSenderStageId()).getServerInstances());
        return oldPartitionKeys;
      }
      // This means we can't skip shuffle and there's a partitioning enforced by receiver.
      int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
      List<PartitionKey> partitionKeys =
          ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
              .map(x -> new PartitionKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
      return new HashSet<>(partitionKeys);
    }
    // If the current stage is a join-stage then we already know whether shuffle can be skipped.
    if (_canSkipShuffleForJoin) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      // For the join-case, servers are already re-assigned in visitJoin. Moreover, we haven't yet changed sender's
      // distribution.
      ((MailboxSendNode) node.getSender()).setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else if (selector == null) {
      return new HashSet<>();
    }
    // This means we can't skip shuffle and there's a partitioning enforced by receiver.
    int numPartitions = _stageMetadataMap.get(node.getStageId()).getServerInstances().size();
    List<PartitionKey> partitionKeys =
        ((FieldSelectionKeySelector) selector).getColumnIndices().stream()
            .map(x -> new PartitionKey(x, numPartitions, selector.hashAlgorithm())).collect(Collectors.toList());
    return new HashSet<>(partitionKeys);
  }

  @Override
  public Set<PartitionKey> visitMailboxSend(MailboxSendNode node, GreedyShuffleRewriteContext context) {
    Set<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    boolean canSkipShuffleBasic = partitionKeyCondition(oldPartitionKeys, selector);
    // If receiver is not a join-stage, then we can determine distribution type now.
    if (!context.isJoinStage(node.getReceiverStageId())) {
      Set<PartitionKey> partitionKeys;
      if (canSkipShuffleBasic && areServersSuperset(node.getReceiverStageId(), node.getStageId())) {
        // Servers are not re-assigned on sender-side. If needed, they are re-assigned on the receiver side.
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        partitionKeys = oldPartitionKeys;
      } else {
        partitionKeys = new HashSet<>();
      }
      context.setPartitionKeys(node.getStageId(), partitionKeys);
      return partitionKeys;
    }
    // If receiver is a join-stage, remember partition-keys of the child node of MailboxSendNode.
    Set<PartitionKey> mailboxSendPartitionKeys = canSkipShuffleBasic ? oldPartitionKeys : new HashSet<>();
    context.setPartitionKeys(node.getStageId(), mailboxSendPartitionKeys);
    return mailboxSendPartitionKeys;
  }

  @Override
  public Set<PartitionKey> visitProject(ProjectNode node, GreedyShuffleRewriteContext context) {
    // Project reorders or removes keys
    Set<PartitionKey> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        int index = ((RexExpression.InputRef) rex).getIndex();
        oldToNewIndex.put(index, i);
      }
    }

    return computeNewPartitionKeys(oldPartitionKeys, oldToNewIndex);
  }

  @Override
  public Set<PartitionKey> visitSort(SortNode node, GreedyShuffleRewriteContext context) {
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<PartitionKey> visitTableScan(TableScanNode node, GreedyShuffleRewriteContext context) {
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
        Set<PartitionKey> newPartitionKeys = new HashSet<>();
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
    return new HashSet<>();
  }

  @Override
  public Set<PartitionKey> visitValue(ValueNode node, GreedyShuffleRewriteContext context) {
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
    Set<ServerInstance> leftServerInstances = new HashSet<>(_stageMetadataMap.get(leftStageId).getServerInstances());
    List<ServerInstance> rightServerInstances = _stageMetadataMap.get(rightStageId).getServerInstances();
    List<ServerInstance> currentServerInstances = _stageMetadataMap.get(currentStageId).getServerInstances();
    return leftServerInstances.containsAll(rightServerInstances)
        && leftServerInstances.size() == rightServerInstances.size()
        && currentServerInstances.containsAll(leftServerInstances);
  }

  /**
   * Given the existing partitioning keys in oldPartitionsKeys, it takes the mapping from the old index to the new index
   * and the computes the new partition keys. Any partition key that has an index that is not present in oldToNewIndex
   * is dropped. If all indices of a PartitionKey are present in oldToNewIndex, we keep that partition key in the new
   * keys and change the name using the map.
   */
  private static Set<PartitionKey> computeNewPartitionKeys(Set<PartitionKey> oldPartitionKeys,
      Map<Integer, Integer> oldToNewIndex) {
    Set<PartitionKey> partitionKeys = new HashSet<>();
    for (PartitionKey partitionKey : oldPartitionKeys) {
      boolean shouldDrop = false;
      PartitionKey newPartitionKey = new PartitionKey(partitionKey.getNumPartitions(), partitionKey.getHashAlgorithm());
      for (Integer index: partitionKey.getIndices()) {
        if (!oldToNewIndex.containsKey(index)) {
          shouldDrop = true;
          break;
        }
        newPartitionKey.addIndex(oldToNewIndex.get(index));
      }
      if (!shouldDrop) {
        partitionKeys.add(newPartitionKey);
      }
    }

    return partitionKeys;
  }

  private static boolean partitionKeyCondition(Set<PartitionKey> partitionKeys,
      KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      List<Integer> targetSet = new ArrayList<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (PartitionKey partitionKey : partitionKeys) {
        if (targetSet.size() >= partitionKey.getIndices().size() && targetSet.subList(0,
            partitionKey.getIndices().size()).equals(partitionKey.getIndices())) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean partitionKeyConditionForJoin(MailboxReceiveNode mailboxReceiveNode,
      MailboxSendNode mailboxSendNode,
      GreedyShuffleRewriteContext context) {
    // First check PartitionKeyCondition for the sender <--> sender.getInputs().get(0) pair
    Set<PartitionKey> oldPartitionKeys = context.getPartitionKeys(mailboxSendNode.getStageId());
    KeySelector<Object[], Object[]> selector = mailboxSendNode.getPartitionKeySelector();
    if (!partitionKeyCondition(oldPartitionKeys, selector)) {
      return false;
    }
    // Check PartitionKeyCondition for the sender <--> receiver pair
    // Since shuffle can be skipped, oldPartitionsKeys == senderPartitionKeys
    selector = mailboxReceiveNode.getPartitionKeySelector();
    return partitionKeyCondition(oldPartitionKeys, selector);
  }

  private static PartitionKey getEquivalentSenderKey(Set<PartitionKey> partitionKeys,
      KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      List<Integer> targetSet = new ArrayList<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      for (PartitionKey partitionKey : partitionKeys) {
        if (targetSet.size() >= partitionKey.getIndices().size() && targetSet.subList(0,
            partitionKey.getIndices().size()).equals(partitionKey.getIndices())) {
          return partitionKey;
        }
      }
    }
    throw new IllegalStateException("Receiver's Equivalent Key in Sender Can't be Determined. This indicates a bug.");
  }

  private static boolean checkPartitionScheme(MailboxReceiveNode leftReceiveNode, MailboxReceiveNode rightReceiveNode,
      GreedyShuffleRewriteContext context) {
    int leftSender = leftReceiveNode.getSenderStageId();
    int rightSender = rightReceiveNode.getSenderStageId();
    PartitionKey leftPKey = getEquivalentSenderKey(context.getPartitionKeys(leftSender),
        leftReceiveNode.getPartitionKeySelector());
    PartitionKey rightPKey = getEquivalentSenderKey(context.getPartitionKeys(rightSender),
        rightReceiveNode.getPartitionKeySelector());
    if (leftPKey.getNumPartitions() != rightPKey.getNumPartitions()) {
      return false;
    }
    return leftPKey.getHashAlgorithm().equals(rightPKey.getHashAlgorithm());
  }
}
