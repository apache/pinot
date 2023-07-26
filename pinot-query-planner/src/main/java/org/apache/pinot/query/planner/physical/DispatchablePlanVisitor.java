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


public class DispatchablePlanVisitor implements PlanNodeVisitor<Void, DispatchablePlanContext> {
  public static final DispatchablePlanVisitor INSTANCE = new DispatchablePlanVisitor();

  private DispatchablePlanVisitor() {
  }

  private static DispatchablePlanMetadata getOrCreateDispatchablePlanMetadata(PlanNode node,
      DispatchablePlanContext context) {
    return context.getDispatchablePlanMetadataMap().computeIfAbsent(node.getPlanFragmentId(),
        (id) -> new DispatchablePlanMetadata());
  }

  @Override
  public Void visitAggregate(AggregateNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.setRequireSingleton(node.getGroupSet().size() == 0
        && node.getAggType().equals(AggregateNode.AggType.FINAL));
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    // TODO: Figure out a way to parallelize Empty OVER() and OVER(ORDER BY) so the computation can be done across
    //       multiple nodes.
    // Empty OVER() and OVER(ORDER BY) need to be processed on a singleton node. OVER() with PARTITION BY can be
    // distributed as no global ordering is required across partitions.
    dispatchablePlanMetadata.setRequireSingleton(node.getGroupSet().size() == 0);
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode setOpNode, DispatchablePlanContext context) {
    setOpNode.getInputs().forEach(input -> input.visit(this, context));
    getOrCreateDispatchablePlanMetadata(setOpNode, context);
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode exchangeNode, DispatchablePlanContext context) {
    throw new UnsupportedOperationException("ExchangeNode should not be visited by DispatchablePlanVisitor");
  }

  @Override
  public Void visitFilter(FilterNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, DispatchablePlanContext context) {
    node.getInputs().forEach(join -> join.visit(this, context));
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, DispatchablePlanContext context) {
    node.getSender().visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    context.getDispatchablePlanStageRootMap().put(node.getPlanFragmentId(), node);
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitSort(SortNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.setRequireSingleton(node.getCollationKeys().size() > 0 && node.getOffset() != -1);
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, DispatchablePlanContext context) {
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.addScannedTable(node.getTableName());
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, DispatchablePlanContext context) {
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }
}
