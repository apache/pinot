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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.SubPlanMetadata;
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
 * SubPlanFragmenter is an implementation of {@link PlanNodeVisitor} to fragment a
 * {@link org.apache.pinot.query.planner.QueryPlan} into multiple {@link org.apache.pinot.query.planner.SubPlan}.
 *
 * The fragmenting process is as follows:
 * 1. Traverse the plan tree in a depth-first manner;
 * 2. For each node, if it is a SubPlan splittable ExchangeNode, switch it to a {@link LiteralValueNode};
 * 3. Increment current SubPlan Id by one and keep traverse the tree.
 */
public class SubPlanFragmenter implements PlanNodeVisitor<PlanNode, SubPlanFragmenter.Context> {
  public static final SubPlanFragmenter INSTANCE = new SubPlanFragmenter();

  private PlanNode process(PlanNode node, Context context) {
    node.setPlanFragmentId(context._currentSubPlanId);
    List<PlanNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      context._previousSubPlanId = node.getPlanFragmentId();
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
    if (!isSubPlanSplitter(node)) {
      return process(node, context);
    }
    int currentStageId = context._previousSubPlanId;
    int nextSubPlanId = context._currentSubPlanId + 1;

    context._currentSubPlanId = nextSubPlanId;
    PlanNode nextStageRoot = node.getInputs().get(0).visit(this, context);
    context._subPlanIdToRootNodeMap.put(nextSubPlanId, nextStageRoot);
    if (!context._subPlanIdToChildrenMap.containsKey(currentStageId)) {
      context._subPlanIdToChildrenMap.put(currentStageId, new ArrayList<>());
    }
    context._subPlanIdToChildrenMap.get(currentStageId).add(nextSubPlanId);
    context._subPlanIdToMetadataMap.put(nextSubPlanId, new SubPlanMetadata(node.getTableNames(), ImmutableList.of()));
    PlanNode literalValueNode = new LiteralValueNode(nextStageRoot.getDataSchema());
    return literalValueNode;
  }

  private boolean isSubPlanSplitter(PlanNode node) {
    return ((ExchangeNode) node).getExchangeType() == PinotRelExchangeType.SUB_PLAN;
  }

  public static class Context {
    Map<Integer, PlanNode> _subPlanIdToRootNodeMap = new HashMap<>();

    Map<Integer, List<Integer>> _subPlanIdToChildrenMap = new HashMap<>();
    Map<Integer, SubPlanMetadata> _subPlanIdToMetadataMap = new HashMap<>();

    // SubPlan ID starts with 0.
    Integer _currentSubPlanId = 0;
    Integer _previousSubPlanId = 0;
  }
}
