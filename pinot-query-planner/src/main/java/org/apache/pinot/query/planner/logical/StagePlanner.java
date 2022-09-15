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
package org.apache.pinot.query.planner.logical;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.routing.WorkerManager;


/**
 * QueryPlanMaker walks top-down from {@link RelRoot} and construct a forest of trees with {@link StageNode}.
 *
 * This class is non-threadsafe. Do not reuse the stage planner for multiple query plans.
 */
public class StagePlanner {
  private final PlannerContext _plannerContext;
  private final WorkerManager _workerManager;

  private Map<Integer, StageNode> _queryStageMap;
  private Map<Integer, StageMetadata> _stageMetadataMap;
  private int _stageIdCounter;

  public StagePlanner(PlannerContext plannerContext, WorkerManager workerManager) {
    _plannerContext = plannerContext;
    _workerManager = workerManager;
  }

  /**
   * Construct the dispatchable plan from relational logical plan.
   *
   * @param relRoot relational plan root.
   * @return dispatchable plan.
   */
  public QueryPlan makePlan(RelNode relRoot) {
    // clear the state
    _queryStageMap = new HashMap<>();
    _stageMetadataMap = new HashMap<>();
    // Stage ID starts with 1, 0 will be reserved for ROOT stage.
    _stageIdCounter = 1;

    // walk the plan and create stages.
    StageNode globalStageRoot = walkRelPlan(relRoot, getNewStageId());

    // global root needs to send results back to the ROOT, a.k.a. the client response node. the last stage only has one
    // receiver so doesn't matter what the exchange type is. setting it to SINGLETON by default.
    StageNode globalReceiverNode =
        new MailboxReceiveNode(0, globalStageRoot.getDataSchema(), globalStageRoot.getStageId(),
            RelDistribution.Type.RANDOM_DISTRIBUTED, null);
    StageNode globalSenderNode = new MailboxSendNode(globalStageRoot.getStageId(), globalStageRoot.getDataSchema(),
        globalReceiverNode.getStageId(), RelDistribution.Type.RANDOM_DISTRIBUTED, null);
    globalSenderNode.addInput(globalStageRoot);
    _queryStageMap.put(globalSenderNode.getStageId(), globalSenderNode);
    StageMetadata stageMetadata = _stageMetadataMap.get(globalSenderNode.getStageId());
    stageMetadata.attach(globalSenderNode);

    _queryStageMap.put(globalReceiverNode.getStageId(), globalReceiverNode);
    StageMetadata globalReceivingStageMetadata = new StageMetadata();
    globalReceivingStageMetadata.attach(globalReceiverNode);
    _stageMetadataMap.put(globalReceiverNode.getStageId(), globalReceivingStageMetadata);

    // assign workers to each stage.
    for (Map.Entry<Integer, StageMetadata> e : _stageMetadataMap.entrySet()) {
      _workerManager.assignWorkerToStage(e.getKey(), e.getValue());
    }

    return new QueryPlan(_queryStageMap, _stageMetadataMap);
  }

  // non-threadsafe
  // TODO: add dataSchema (extracted from RelNode schema) to the StageNode.
  private StageNode walkRelPlan(RelNode node, int currentStageId) {
    if (isExchangeNode(node)) {
      // 1. exchangeNode always have only one input, get its input converted as a new stage root.
      StageNode nextStageRoot = walkRelPlan(node.getInput(0), getNewStageId());
      RelDistribution distribution = ((LogicalExchange) node).getDistribution();
      List<Integer> distributionKeys = distribution.getKeys();
      RelDistribution.Type exchangeType = distribution.getType();

      // 2. make an exchange sender and receiver node pair
      // only HASH_DISTRIBUTED requires a partition key selector; so all other types (SINGLETON and BROADCAST)
      // of exchange will not carry a partition key selector.
      KeySelector<Object[], Object[]> keySelector = exchangeType == RelDistribution.Type.HASH_DISTRIBUTED
          ? new FieldSelectionKeySelector(distributionKeys) : null;

      StageNode mailboxReceiver;
      StageNode mailboxSender;
      if (canSkipShuffle(nextStageRoot, keySelector)) {
        // Use SINGLETON exchange type indicates a LOCAL-to-LOCAL data transfer between execution threads.
        // TODO: actually implement the SINGLETON exchange without going through the over-the-wire GRPC mailbox
        // sender and receiver.
        mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getDataSchema(),
            nextStageRoot.getStageId(), RelDistribution.Type.SINGLETON, keySelector);
        mailboxSender = new MailboxSendNode(nextStageRoot.getStageId(), nextStageRoot.getDataSchema(),
            mailboxReceiver.getStageId(), RelDistribution.Type.SINGLETON, keySelector);
      } else {
        mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getDataSchema(),
            nextStageRoot.getStageId(), exchangeType, keySelector);
        mailboxSender = new MailboxSendNode(nextStageRoot.getStageId(), nextStageRoot.getDataSchema(),
            mailboxReceiver.getStageId(), exchangeType, keySelector);
      }
      mailboxSender.addInput(nextStageRoot);

      // 3. put the sender side as a completed stage.
      _queryStageMap.put(mailboxSender.getStageId(), mailboxSender);

      // 4. update stage metadata.
      updateStageMetadata(mailboxSender.getStageId(), mailboxSender, _stageMetadataMap);
      updateStageMetadata(mailboxReceiver.getStageId(), mailboxReceiver, _stageMetadataMap);

      // 5. return the receiver, this is considered as a "virtual table scan" node for its parent.
      return mailboxReceiver;
    } else {
      StageNode stageNode = RelToStageConverter.toStageNode(node, currentStageId);
      List<RelNode> inputs = node.getInputs();
      for (RelNode input : inputs) {
        stageNode.addInput(walkRelPlan(input, currentStageId));
      }
      updateStageMetadata(currentStageId, stageNode, _stageMetadataMap);
      return stageNode;
    }
  }

  private boolean canSkipShuffle(StageNode stageNode, KeySelector<Object[], Object[]> keySelector) {
    Set<Integer> originSet = stageNode.getPartitionKeys();
    if (!originSet.isEmpty() && keySelector != null) {
      Set<Integer> targetSet = new HashSet<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      return targetSet.containsAll(originSet);
    }
    return false;
  }

  private static void updateStageMetadata(int stageId, StageNode node, Map<Integer, StageMetadata> stageMetadataMap) {
    updatePartitionKeys(node);
    StageMetadata stageMetadata = stageMetadataMap.computeIfAbsent(stageId, (id) -> new StageMetadata());
    stageMetadata.attach(node);
  }

  private static void updatePartitionKeys(StageNode node) {
    if (node instanceof ProjectNode) {
      // any input reference directly carry over should still be a partition key.
      Set<Integer> previousPartitionKeys = node.getInputs().get(0).getPartitionKeys();
      Set<Integer> newPartitionKeys = new HashSet<>();
      ProjectNode projectNode = (ProjectNode) node;
      for (int i = 0; i < projectNode.getProjects().size(); i++) {
        RexExpression rexExpression = projectNode.getProjects().get(i);
        if (rexExpression instanceof RexExpression.InputRef
            && previousPartitionKeys.contains(((RexExpression.InputRef) rexExpression).getIndex())) {
          newPartitionKeys.add(i);
        }
      }
      projectNode.setPartitionKeys(newPartitionKeys);
    } else if (node instanceof FilterNode) {
      // filter node doesn't change partition keys.
      node.setPartitionKeys(node.getInputs().get(0).getPartitionKeys());
    } else if (node instanceof AggregateNode) {
      // any input reference directly carry over in group set of aggregation should still be a partition key.
      Set<Integer> previousPartitionKeys = node.getInputs().get(0).getPartitionKeys();
      Set<Integer> newPartitionKeys = new HashSet<>();
      AggregateNode aggregateNode = (AggregateNode) node;
      for (int i = 0; i < aggregateNode.getGroupSet().size(); i++) {
        RexExpression rexExpression = aggregateNode.getGroupSet().get(i);
        if (rexExpression instanceof RexExpression.InputRef
            && previousPartitionKeys.contains(((RexExpression.InputRef) rexExpression).getIndex())) {
          newPartitionKeys.add(i);
        }
      }
      aggregateNode.setPartitionKeys(newPartitionKeys);
    } else if (node instanceof JoinNode) {
      int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
      Set<Integer> leftPartitionKeys = node.getInputs().get(0).getPartitionKeys();
      Set<Integer> rightPartitionKeys = node.getInputs().get(1).getPartitionKeys();
      // TODO: currently JOIN criteria guarantee to only have one FieldSelectionKeySelector. Support more.
      FieldSelectionKeySelector leftJoinKeySelector =
          (FieldSelectionKeySelector) ((JoinNode) node).getCriteria().get(0).getLeftJoinKeySelector();
      FieldSelectionKeySelector rightJoinKeySelector =
          (FieldSelectionKeySelector) ((JoinNode) node).getCriteria().get(0).getRightJoinKeySelector();
      Set<Integer> newPartitionKeys = new HashSet<>();
      for (int i = 0; i < leftJoinKeySelector.getColumnIndices().size(); i++) {
        int leftIndex = leftJoinKeySelector.getColumnIndices().get(i);
        int rightIndex = rightJoinKeySelector.getColumnIndices().get(i);
        if (leftPartitionKeys.contains(leftIndex)) {
          newPartitionKeys.add(leftIndex);
        }
        if (rightPartitionKeys.contains(rightIndex)) {
          newPartitionKeys.add(leftDataSchemaSize + rightIndex);
        }
      }
      node.setPartitionKeys(newPartitionKeys);
    } else if (node instanceof TableScanNode) {
      // TODO: add table partition in table config as partition keys. we dont have that information yet.
    } else if (node instanceof MailboxReceiveNode) {
      // hash distribution key is partition key.
      FieldSelectionKeySelector keySelector = (FieldSelectionKeySelector)
          ((MailboxReceiveNode) node).getPartitionKeySelector();
      if (keySelector != null) {
        node.setPartitionKeys(new HashSet<>(keySelector.getColumnIndices()));
      }
    } else if (node instanceof MailboxSendNode) {
      FieldSelectionKeySelector keySelector = (FieldSelectionKeySelector)
          ((MailboxSendNode) node).getPartitionKeySelector();
      if (keySelector != null) {
        node.setPartitionKeys(new HashSet<>(keySelector.getColumnIndices()));
      }
    }
  }

  private boolean isExchangeNode(RelNode node) {
    return (node instanceof LogicalExchange);
  }

  private int getNewStageId() {
    return _stageIdCounter++;
  }
}
