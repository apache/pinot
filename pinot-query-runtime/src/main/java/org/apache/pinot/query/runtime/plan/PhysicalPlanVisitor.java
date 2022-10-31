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
package org.apache.pinot.query.runtime.plan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.transport.ServerInstance;
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
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;


/**
 * This visitor constructs a physical plan of operators from a {@link StageNode} tree. Note that
 * this works only for the intermediate stage nodes, leaf stage nodes are expected to compile into
 * v1 operators at this point in time.
 *
 * <p>This class should be used statically via {@link #build(StageNode, PlanRequestContext)}
 *
 * @see org.apache.pinot.query.runtime.QueryRunner#processQuery(DistributedStagePlan, ExecutorService, Map)
 */
public class PhysicalPlanVisitor implements StageNodeVisitor<Operator<TransferableBlock>, PlanRequestContext> {
  private static final PhysicalPlanVisitor INSTANCE = new PhysicalPlanVisitor();

  public static Operator<TransferableBlock> build(StageNode node, PlanRequestContext context) {
    return node.visit(INSTANCE, context);
  }

  @Override
  public Operator<TransferableBlock> visitMailboxReceive(MailboxReceiveNode node, PlanRequestContext context) {
    List<ServerInstance> sendingInstances = context.getMetadataMap().get(node.getSenderStageId()).getServerInstances();
    return new MailboxReceiveOperator(context.getMailboxService(), node.getDataSchema(), sendingInstances,
        node.getExchangeType(), node.getPartitionKeySelector(), context.getHostName(), context.getPort(),
        context.getRequestId(), node.getSenderStageId());
  }

  @Override
  public Operator<TransferableBlock> visitMailboxSend(MailboxSendNode node, PlanRequestContext context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, context);
    StageMetadata receivingStageMetadata = context.getMetadataMap().get(node.getReceiverStageId());
    return new MailboxSendOperator(context.getMailboxService(), node.getDataSchema(), nextOperator,
        receivingStageMetadata.getServerInstances(), node.getExchangeType(), node.getPartitionKeySelector(),
        context.getHostName(), context.getPort(), context.getRequestId(), node.getStageId());
  }

  @Override
  public Operator<TransferableBlock> visitAggregate(AggregateNode node, PlanRequestContext context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, context);
    return new AggregateOperator(nextOperator, node.getDataSchema(), node.getAggCalls(),
        node.getGroupSet(), node.getInputs().get(0).getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitFilter(FilterNode node, PlanRequestContext context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, context);
    return new FilterOperator(nextOperator, node.getDataSchema(), node.getCondition());
  }

  @Override
  public Operator<TransferableBlock> visitJoin(JoinNode node, PlanRequestContext context) {
    StageNode left = node.getInputs().get(0);
    StageNode right = node.getInputs().get(1);

    Operator<TransferableBlock> leftOperator = left.visit(this, context);
    Operator<TransferableBlock> rightOperator = right.visit(this, context);

    return new HashJoinOperator(leftOperator, left.getDataSchema(), rightOperator,
        right.getDataSchema(), node.getDataSchema(), node.getJoinKeys(),
        node.getJoinClauses(), node.getJoinRelType());
  }

  @Override
  public Operator<TransferableBlock> visitProject(ProjectNode node, PlanRequestContext context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, context);
    return new TransformOperator(nextOperator, node.getDataSchema(), node.getProjects(),
        node.getInputs().get(0).getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitSort(SortNode node, PlanRequestContext context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, context);
    return new SortOperator(nextOperator, node.getCollationKeys(), node.getCollationDirections(),
        node.getFetch(), node.getOffset(), node.getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitTableScan(TableScanNode node, PlanRequestContext context) {
    throw new UnsupportedOperationException("Stage node of type TableScanNode is not supported!");
  }

  @Override
  public Operator<TransferableBlock> visitValue(ValueNode node, PlanRequestContext context) {
    return new LiteralValueOperator(node.getDataSchema(), node.getLiteralRows());
  }
}
