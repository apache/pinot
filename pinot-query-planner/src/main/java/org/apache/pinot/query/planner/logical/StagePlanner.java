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
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.StageNode;
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
        new MailboxReceiveNode(0, globalStageRoot.getStageId(), RelDistribution.Type.SINGLETON);
    StageNode globalSenderNode = new MailboxSendNode(globalStageRoot.getStageId(), globalReceiverNode.getStageId(),
        RelDistribution.Type.SINGLETON);
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
  private StageNode walkRelPlan(RelNode node, int currentStageId) {
    if (isExchangeNode(node)) {
      // 1. exchangeNode always have only one input, get its input converted as a new stage root.
      StageNode nextStageRoot = walkRelPlan(node.getInput(0), getNewStageId());
      RelDistribution distribution = ((LogicalExchange) node).getDistribution();
      RelDistribution.Type exchangeType = distribution.getType();

      // 2. make an exchange sender and receiver node pair
      StageNode mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getStageId(), exchangeType);
      StageNode mailboxSender = new MailboxSendNode(nextStageRoot.getStageId(), mailboxReceiver.getStageId(),
          exchangeType, exchangeType == RelDistribution.Type.HASH_DISTRIBUTED
          ? new FieldSelectionKeySelector(distribution.getKeys().get(0)) : null);
      mailboxSender.addInput(nextStageRoot);

      // 3. put the sender side as a completed stage.
      _queryStageMap.put(mailboxSender.getStageId(), mailboxSender);

      // 4. return the receiver (this is considered as a "virtual table scan" node for its parent.
      return mailboxReceiver;
    } else {
      StageNode stageNode = RelToStageConverter.toStageNode(node, currentStageId);
      List<RelNode> inputs = node.getInputs();
      for (RelNode input : inputs) {
        stageNode.addInput(walkRelPlan(input, currentStageId));
      }
      StageMetadata stageMetadata = _stageMetadataMap.computeIfAbsent(currentStageId, (id) -> new StageMetadata());
      stageMetadata.attach(stageNode);
      return stageNode;
    }
  }

  private boolean isExchangeNode(RelNode node) {
    return (node instanceof LogicalExchange);
  }

  private int getNewStageId() {
    return _stageIdCounter++;
  }
}
