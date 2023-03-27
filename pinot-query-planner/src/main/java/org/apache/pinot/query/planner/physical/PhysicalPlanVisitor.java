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

import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;


public class PhysicalPlanVisitor implements StageNodeVisitor<Void, PhysicalPlanContext> {
  public static final PhysicalPlanVisitor INSTANCE = new PhysicalPlanVisitor();

  private PhysicalPlanVisitor() {
  }

  /**
   * Entry point
   * @param globalReceiverNode
   * @param physicalPlanContext
   */
  public void constructPhysicalPlan(StageNode globalReceiverNode, PhysicalPlanContext physicalPlanContext) {
    globalReceiverNode.visit(PhysicalPlanVisitor.INSTANCE, physicalPlanContext);
    computeWorkerAssignment(globalReceiverNode, physicalPlanContext);
  }

  private StageMetadata getStageMetadata(StageNode node, PhysicalPlanContext context) {
    return context.getQueryPlan().getStageMetadataMap().computeIfAbsent(
        node.getStageId(), (id) -> new StageMetadata());
  }

  private void computeWorkerAssignment(StageNode node, PhysicalPlanContext context) {
    int stageId = node.getStageId();
    context.getWorkerManager().assignWorkerToStage(stageId, context.getQueryPlan().getStageMetadataMap().get(stageId),
        context.getRequestId(), context.getPlannerContext().getOptions());
  }

  @Override
  public Void visitAggregate(AggregateNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    StageMetadata stageMetadata = getStageMetadata(node, context);
    stageMetadata.setRequireSingleton(node.getGroupSet().size() == 0 && AggregateNode.isFinalStage(node));
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    StageMetadata stageMetadata = getStageMetadata(node, context);
    // TODO: Figure out a way to parallelize Empty OVER() and OVER(ORDER BY) so the computation can be done across
    //       multiple nodes.
    // Empty OVER() and OVER(ORDER BY) need to be processed on a singleton node. OVER() with PARTITION BY can be
    // distributed as no global ordering is required across partitions.
    stageMetadata.setRequireSingleton(node.getGroupSet().size() == 0);
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getStageMetadata(node, context);
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, PhysicalPlanContext context) {
    node.getInputs().forEach(join -> join.visit(this, context));
    getStageMetadata(node, context);
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, PhysicalPlanContext context) {
    node.getSender().visit(this, context);
    getStageMetadata(node, context);

    // special case for the global mailbox receive node
    if (node.getStageId() == 0) {
      context.getQueryPlan().getQueryStageMap().put(0, node);
    }

    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getStageMetadata(node, context);

    context.getQueryPlan().getQueryStageMap().put(node.getStageId(), node);
    computeWorkerAssignment(node, context);
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getStageMetadata(node, context);
    return null;
  }

  @Override
  public Void visitSort(SortNode node, PhysicalPlanContext context) {
    node.getInputs().get(0).visit(this, context);
    StageMetadata stageMetadata = getStageMetadata(node, context);
    stageMetadata.setRequireSingleton(node.getCollationKeys().size() > 0 && node.getOffset() != -1);
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, PhysicalPlanContext context) {
    StageMetadata stageMetadata = getStageMetadata(node, context);
    stageMetadata.addScannedTable(node.getTableName());
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, PhysicalPlanContext context) {
    getStageMetadata(node, context);
    return null;
  }
}
