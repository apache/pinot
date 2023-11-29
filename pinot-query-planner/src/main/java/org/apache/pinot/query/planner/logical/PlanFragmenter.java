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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
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
 * PlanFragmenter is an implementation of {@link PlanNodeVisitor} to fragment a {@link SubPlan} into multiple
 * {@link PlanFragment}s.
 *
 * The fragmenting process is as follows:
 * 1. Traverse the plan tree in a depth-first manner;
 * 2. For each node, if it is a PlanFragment splittable ExchangeNode, split it into {@link MailboxReceiveNode} and
 * {@link MailboxSendNode} pair;
 * 3. Assign current PlanFragment ID to {@link MailboxReceiveNode};
 * 4. Increment current PlanFragment ID by one and assign it to the {@link MailboxSendNode}.
 */
public class PlanFragmenter implements PlanNodeVisitor<PlanNode, PlanFragmenter.Context> {
  private final Int2ObjectOpenHashMap<PlanFragment> _planFragmentMap = new Int2ObjectOpenHashMap<>();
  private final Int2ObjectOpenHashMap<IntList> _childPlanFragmentIdsMap = new Int2ObjectOpenHashMap<>();

  // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1, next PlanFragment ID starts with 2.
  private int _nextPlanFragmentId = 2;

  public Context createContext() {
    // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1.
    return new Context(1);
  }

  public Int2ObjectOpenHashMap<PlanFragment> getPlanFragmentMap() {
    return _planFragmentMap;
  }

  public Int2ObjectOpenHashMap<IntList> getChildPlanFragmentIdsMap() {
    return _childPlanFragmentIdsMap;
  }

  private PlanNode process(PlanNode node, Context context) {
    node.setPlanFragmentId(context._currentPlanFragmentId);
    node.getInputs().replaceAll(planNode -> planNode.visit(this, context));
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

    // Split the ExchangeNode to a MailboxReceiveNode and a MailboxSendNode, where MailboxReceiveNode is the leave node
    // of the current PlanFragment, and MailboxSendNode is the root node of the next PlanFragment.
    int receiverPlanFragmentId = context._currentPlanFragmentId;
    int senderPlanFragmentId = _nextPlanFragmentId++;
    _childPlanFragmentIdsMap.computeIfAbsent(receiverPlanFragmentId, k -> new IntArrayList())
        .add(senderPlanFragmentId);

    // Create a new context for the next PlanFragment with MailboxSendNode as the root node.
    PlanNode nextPlanFragmentRoot = node.getInputs().get(0).visit(this, new Context(senderPlanFragmentId));
    PinotRelExchangeType exchangeType = node.getExchangeType();
    RelDistribution.Type distributionType = node.getDistributionType();
    // NOTE: Only HASH_DISTRIBUTED requires distribution keys
    // TODO: Revisit ExchangeNode creation logic to avoid using HASH_DISTRIBUTED with empty distribution keys
    List<Integer> distributionKeys =
        distributionType == RelDistribution.Type.HASH_DISTRIBUTED ? node.getDistributionKeys() : null;
    MailboxSendNode mailboxSendNode =
        new MailboxSendNode(senderPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), receiverPlanFragmentId,
            distributionType, exchangeType, distributionKeys, node.getCollations(), node.isSortOnSender(),
            node.isPartitioned());
    mailboxSendNode.addInput(nextPlanFragmentRoot);
    _planFragmentMap.put(senderPlanFragmentId,
        new PlanFragment(senderPlanFragmentId, mailboxSendNode, new ArrayList<>()));

    // Return the MailboxReceiveNode as the leave node of the current PlanFragment.
    return new MailboxReceiveNode(receiverPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), senderPlanFragmentId,
        distributionType, exchangeType, distributionKeys, node.getCollations(), node.isSortOnSender(),
        node.isSortOnReceiver(), mailboxSendNode);
  }

  private boolean isPlanFragmentSplitter(PlanNode node) {
    return ((ExchangeNode) node).getExchangeType() != PinotRelExchangeType.SUB_PLAN;
  }

  public static class Context {
    private final int _currentPlanFragmentId;

    private Context(int currentPlanFragmentId) {
      _currentPlanFragmentId = currentPlanFragmentId;
    }
  }
}
