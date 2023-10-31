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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.PlanFragmentMetadata;
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


/**
 * PlanFragmenter is an implementation of {@link PlanNodeVisitor} to fragment a
 * {@link org.apache.pinot.query.planner.SubPlan} into multiple {@link PlanFragment}.
 *
 * The fragmenting process is as follows:
 * 1. Traverse the plan tree in a depth-first manner;
 * 2. For each node, if it is a PlanFragment splittable ExchangeNode, split it into {@link MailboxReceiveNode} and
 * {@link MailboxSendNode} pair;
 * 3. Assign current PlanFragment Id to {@link MailboxReceiveNode};
 * 4. Increment current PlanFragment Id by one and assign it to the {@link MailboxSendNode}.
 */
public class PlanFragmenter implements PlanNodeVisitor<PlanNode, PlanFragmenter.Context> {
  public static final PlanFragmenter INSTANCE = new PlanFragmenter();

  private PlanNode process(PlanNode node, Context context) {
    node.setPlanFragmentId(context._currentPlanFragmentId);
    List<PlanNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      context._previousPlanFragmentId = node.getPlanFragmentId();
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
    throw new UnsupportedOperationException("MailboxReceiveNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitMailboxSend(MailboxSendNode node, Context context) {
    throw new UnsupportedOperationException("MailboxSendNode should not be visited by PlanNodeFragmenter");
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
    if (!isPlanFragmentSplitter(node)) {
      return process(node, context);
    }
    int currentPlanFragmentId = context._previousPlanFragmentId;
    int nextPlanFragmentId = ++context._currentPlanFragmentId;
    // Set previous PlanFragment ID in the context to be the next PlanFragment ID to be used by the child node.
    context._previousPlanFragmentId = nextPlanFragmentId;
    PlanNode nextPlanFragmentRoot = node.getInputs().get(0).visit(this, context);

    PinotRelExchangeType exchangeType = node.getExchangeType();
    RelDistribution.Type distributionType = node.getDistributionType();
    // NOTE: Only HASH_DISTRIBUTED requires distribution keys
    // TODO: Revisit ExchangeNode creation logic to avoid using HASH_DISTRIBUTED with empty distribution keys
    List<Integer> distributionKeys =
        distributionType == RelDistribution.Type.HASH_DISTRIBUTED ? node.getDistributionKeys() : null;

    PlanNode mailboxSender =
        new MailboxSendNode(nextPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), currentPlanFragmentId,
            distributionType, exchangeType, distributionKeys, node.getCollations(), node.isSortOnSender());
    PlanNode mailboxReceiver =
        new MailboxReceiveNode(currentPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), nextPlanFragmentId,
            distributionType, exchangeType, distributionKeys, node.getCollations(), node.isSortOnSender(),
            node.isSortOnReceiver(), mailboxSender);
    mailboxSender.addInput(nextPlanFragmentRoot);

    context._planFragmentIdToRootNodeMap.put(nextPlanFragmentId,
        new PlanFragment(nextPlanFragmentId, mailboxSender, new PlanFragmentMetadata(), new ArrayList<>()));
    if (!context._planFragmentIdToChildrenMap.containsKey(currentPlanFragmentId)) {
      context._planFragmentIdToChildrenMap.put(currentPlanFragmentId, new ArrayList<>());
    }
    context._planFragmentIdToChildrenMap.get(currentPlanFragmentId).add(nextPlanFragmentId);

    return mailboxReceiver;
  }

  private boolean isPlanFragmentSplitter(PlanNode node) {
    return ((ExchangeNode) node).getExchangeType() != PinotRelExchangeType.SUB_PLAN;
  }

  public static class Context {

    // PlanFragment ID starts with 1, 0 will be reserved for ROOT PlanFragment.
    Integer _currentPlanFragmentId = 1;
    Integer _previousPlanFragmentId = 1;
    Map<Integer, PlanFragment> _planFragmentIdToRootNodeMap = new HashMap<>();

    Map<Integer, List<Integer>> _planFragmentIdToChildrenMap = new HashMap<>();
  }
}
