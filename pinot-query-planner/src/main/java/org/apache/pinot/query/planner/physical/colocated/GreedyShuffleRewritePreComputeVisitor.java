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

import org.apache.pinot.query.planner.stage.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;


/**
 * A visitor that does the precomputation for the {@link GreedyShuffleRewriteContext} of a query plan.
 */
class GreedyShuffleRewritePreComputeVisitor
    extends DefaultPostOrderTraversalVisitor<Integer, GreedyShuffleRewriteContext> {

  static GreedyShuffleRewriteContext preComputeContext(StageNode rootStageNode) {
    GreedyShuffleRewriteContext context = new GreedyShuffleRewriteContext();
    rootStageNode.visit(new GreedyShuffleRewritePreComputeVisitor(), context);
    return context;
  }

  @Override
  public Integer process(StageNode stageNode, GreedyShuffleRewriteContext context) {
    int currentStageId = stageNode.getStageId();
    context.setRootStageNode(currentStageId, stageNode);
    return 0;
  }

  @Override
  public Integer visitJoin(JoinNode joinNode, GreedyShuffleRewriteContext context) {
    super.visitJoin(joinNode, context);
    context.markJoinStage(joinNode.getStageId());
    return 0;
  }

  @Override
  public Integer visitMailboxReceive(MailboxReceiveNode stageNode, GreedyShuffleRewriteContext context) {
    super.visitMailboxReceive(stageNode, context);
    context.addLeafNode(stageNode.getStageId(), stageNode);
    return 0;
  }

  @Override
  public Integer visitTableScan(TableScanNode stageNode, GreedyShuffleRewriteContext context) {
    super.visitTableScan(stageNode, context);
    context.addLeafNode(stageNode.getStageId(), stageNode);
    return 0;
  }

  @Override
  public Integer visitSetOp(SetOpNode setOpNode, GreedyShuffleRewriteContext context) {
    super.visitSetOp(setOpNode, context);
    context.markSetOpStage(setOpNode.getStageId());
    return 0;
  }
}
