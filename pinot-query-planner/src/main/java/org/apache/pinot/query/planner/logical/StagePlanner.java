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
import org.apache.pinot.query.planner.partitioning.KeySelector;
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
  public QueryPlan makePlan(RelRoot relRoot) {
    RelNode relRootNode = relRoot.rel;
    // Stage ID starts with 1, 0 will be reserved for ROOT stage.
    _stageIdCounter = 1;

    // walk the plan and create stages.
    StageNode globalStageRoot = walkRelPlan(relRootNode, getNewStageId());
    ShuffleRewriteVisitor.optimizeShuffles(globalStageRoot);

    // global root needs to send results back to the ROOT, a.k.a. the client response node. the last stage only has one
    // receiver so doesn't matter what the exchange type is. setting it to SINGLETON by default.
    StageNode globalSenderNode = new MailboxSendNode(globalStageRoot.getStageId(), globalStageRoot.getDataSchema(),
        0, RelDistribution.Type.RANDOM_DISTRIBUTED, null);
    globalSenderNode.addInput(globalStageRoot);

    StageNode globalReceiverNode =
        new MailboxReceiveNode(0, globalStageRoot.getDataSchema(), globalStageRoot.getStageId(),
            RelDistribution.Type.RANDOM_DISTRIBUTED, null, globalSenderNode);

    QueryPlan queryPlan = StageMetadataVisitor.attachMetadata(relRoot.fields, globalReceiverNode);

    // assign workers to each stage.
    for (Map.Entry<Integer, StageMetadata> e : queryPlan.getStageMetadataMap().entrySet()) {
      _workerManager.assignWorkerToStage(e.getKey(), e.getValue());
    }

    return queryPlan;
  }

  // non-threadsafe
  // TODO: add dataSchema (extracted from RelNode schema) to the StageNode.
  private StageNode walkRelPlan(RelNode node, int currentStageId) {
    if (isExchangeNode(node)) {
      StageNode nextStageRoot = walkRelPlan(node.getInput(0), getNewStageId());
      RelDistribution distribution = ((LogicalExchange) node).getDistribution();
      return createSendReceivePair(nextStageRoot, distribution, currentStageId);
    } else {
      StageNode stageNode = RelToStageConverter.toStageNode(node, currentStageId);
      List<RelNode> inputs = node.getInputs();
      for (RelNode input : inputs) {
        stageNode.addInput(walkRelPlan(input, currentStageId));
      }
      return stageNode;
    }
  }


  private StageNode createSendReceivePair(StageNode nextStageRoot, RelDistribution distribution, int currentStageId) {
    List<Integer> distributionKeys = distribution.getKeys();
    RelDistribution.Type exchangeType = distribution.getType();

    // make an exchange sender and receiver node pair
    // only HASH_DISTRIBUTED requires a partition key selector; so all other types (SINGLETON and BROADCAST)
    // of exchange will not carry a partition key selector.
    KeySelector<Object[], Object[]> keySelector = exchangeType == RelDistribution.Type.HASH_DISTRIBUTED
        ? new FieldSelectionKeySelector(distributionKeys) : null;

    StageNode mailboxSender = new MailboxSendNode(nextStageRoot.getStageId(), nextStageRoot.getDataSchema(),
        currentStageId, exchangeType, keySelector);
    StageNode mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getDataSchema(),
        nextStageRoot.getStageId(), exchangeType, keySelector, mailboxSender);
    mailboxSender.addInput(nextStageRoot);

    return mailboxReceiver;
  }

  private boolean isExchangeNode(RelNode node) {
    return (node instanceof LogicalExchange);
  }

  private int getNewStageId() {
    return _stageIdCounter++;
  }
}
