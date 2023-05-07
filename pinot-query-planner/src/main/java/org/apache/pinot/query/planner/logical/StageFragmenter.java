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
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class StageFragmenter implements PlanNodeVisitor<PlanNode, StageFragmenter.Context> {
  public static final StageFragmenter INSTANCE = new StageFragmenter();

  private PlanNode process(PlanNode node, Context context) {
    node.setPlanFragmentId(context._currentStageId);
    List<PlanNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      context._previousStageId = node.getPlanFragmentId();
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public PlanNode visitAggregate(AggregateNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitFilter(FilterNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitJoin(JoinNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitMailboxReceive(MailboxReceiveNode node, Context context) {
    throw new UnsupportedOperationException("MailboxReceiveNode should not be visited by StageFragmenter");
  }

  @Override
  public PlanNode visitMailboxSend(MailboxSendNode node, Context context) {
    throw new UnsupportedOperationException("MailboxSendNode should not be visited by StageFragmenter");
  }

  @Override
  public PlanNode visitProject(ProjectNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSort(SortNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitTableScan(TableScanNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitValue(ValueNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitWindow(WindowNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSetOp(SetOpNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitExchange(ExchangeNode node, Context context) {
    int nodeStageId = context._previousStageId;

    context._currentStageId++;
    PlanNode nextStageRoot = node.getInputs().get(0).visit(this, context);

    List<Integer> distributionKeys = node.getDistributionKeys();
    RelDistribution.Type exchangeType = node.getDistributionType();

    // make an exchange sender and receiver node pair
    // only HASH_DISTRIBUTED requires a partition key selector; so all other types (SINGLETON and BROADCAST)
    // of exchange will not carry a partition key selector.
    KeySelector<Object[], Object[]> keySelector = exchangeType == RelDistribution.Type.HASH_DISTRIBUTED
        ? new FieldSelectionKeySelector(distributionKeys) : null;

    PlanNode mailboxSender = new MailboxSendNode(nextStageRoot.getPlanFragmentId(), nextStageRoot.getDataSchema(),
        nodeStageId, exchangeType, keySelector, node.getCollations(), node.isSortOnSender());
    PlanNode mailboxReceiver = new MailboxReceiveNode(nodeStageId, nextStageRoot.getDataSchema(),
        nextStageRoot.getPlanFragmentId(), exchangeType, keySelector,
        node.getCollations(), node.isSortOnSender(), node.isSortOnReceiver(), mailboxSender);
    mailboxSender.addInput(nextStageRoot);

    return mailboxReceiver;
  }

  public static class Context {

    // Stage ID starts with 1, 0 will be reserved for ROOT PlanFragment.
    Integer _currentStageId = 1;
    Integer _previousStageId = 1;
  }
}
