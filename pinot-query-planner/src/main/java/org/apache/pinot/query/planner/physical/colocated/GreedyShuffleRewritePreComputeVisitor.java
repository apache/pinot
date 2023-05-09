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

import org.apache.pinot.query.planner.plannode.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;


/**
 * A visitor that does the precomputation for the {@link GreedyShuffleRewriteContext} of a query plan.
 */
class GreedyShuffleRewritePreComputeVisitor
    extends DefaultPostOrderTraversalVisitor<Integer, GreedyShuffleRewriteContext> {

  static GreedyShuffleRewriteContext preComputeContext(PlanNode rootPlanNode) {
    GreedyShuffleRewriteContext context = new GreedyShuffleRewriteContext();
    rootPlanNode.visit(new GreedyShuffleRewritePreComputeVisitor(), context);
    return context;
  }

  @Override
  public Integer process(PlanNode planNode, GreedyShuffleRewriteContext context) {
    int currentStageId = planNode.getPlanFragmentId();
    context.setRootStageNode(currentStageId, planNode);
    return 0;
  }

  @Override
  public Integer visitJoin(JoinNode joinNode, GreedyShuffleRewriteContext context) {
    super.visitJoin(joinNode, context);
    context.markJoinStage(joinNode.getPlanFragmentId());
    return 0;
  }

  @Override
  public Integer visitMailboxReceive(MailboxReceiveNode planNode, GreedyShuffleRewriteContext context) {
    super.visitMailboxReceive(planNode, context);
    context.addLeafNode(planNode.getPlanFragmentId(), planNode);
    return 0;
  }

  @Override
  public Integer visitTableScan(TableScanNode planNode, GreedyShuffleRewriteContext context) {
    super.visitTableScan(planNode, context);
    context.addLeafNode(planNode.getPlanFragmentId(), planNode);
    return 0;
  }

  @Override
  public Integer visitSetOp(SetOpNode setOpNode, GreedyShuffleRewriteContext context) {
    super.visitSetOp(setOpNode, context);
    context.markSetOpStage(setOpNode.getPlanFragmentId());
    return 0;
  }
}
