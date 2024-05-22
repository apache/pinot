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
package org.apache.pinot.query.planner.serde;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
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


public class SerializationVisitor implements PlanNodeVisitor<Plan.PlanNode, List<Plan.PlanNode>> {
  public Plan.PlanNode process(PlanNode planNode, List<Plan.PlanNode> context) {
    AbstractPlanNode abstractPlanNode = (AbstractPlanNode) planNode;
    Plan.PlanNode.Builder builder = getBuilder(abstractPlanNode, context);
    builder.setObjectField(abstractPlanNode.toObjectField());

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);
    return protoPlanNode;
  }

  private static Plan.PlanNode.Builder getBuilder(AbstractPlanNode planNode, List<Plan.PlanNode> context) {
    Plan.PlanNode.Builder builder = Plan.PlanNode.newBuilder()
        .setStageId(planNode.getPlanFragmentId())
        .setNodeName(planNode.getClass().getSimpleName());
    DataSchema dataSchema = planNode.getDataSchema();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      builder.addColumnNames(dataSchema.getColumnName(i));
      builder.addColumnDataTypes(dataSchema.getColumnDataType(i).name());
    }

    builder.addAllInputs(context);
    return builder;
  }

  private static Plan.NodeHint.Builder getNodeHintBuilder(AbstractPlanNode.NodeHint nodeHint) {
    Plan.NodeHint.Builder builder = Plan.NodeHint.newBuilder();
    for (Map.Entry<String, Map<String, String>> entry : nodeHint._hintOptions.entrySet()) {
      Plan.StrStrMap.Builder strMapBuilder = Plan.StrStrMap.newBuilder();
      strMapBuilder.putAllOptions(entry.getValue());
      builder.putHintOptions(entry.getKey(), strMapBuilder.build());
    }
    return builder;
  }

  @Override
  public Plan.PlanNode visitAggregate(AggregateNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitFilter(FilterNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitJoin(JoinNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    node.getInputs().get(1).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitMailboxReceive(MailboxReceiveNode node, List<Plan.PlanNode> context) {
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitMailboxSend(MailboxSendNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitProject(ProjectNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitSort(SortNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitTableScan(TableScanNode node, List<Plan.PlanNode> context) {
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.TableScanNode.Builder tableScanNodeBuilder = Plan.TableScanNode.newBuilder()
        .setTableName(node.getTableName())
        .addAllTableScanColumns(node.getTableScanColumns())
        .setNodeHint(getNodeHintBuilder(node.getNodeHint()));
    builder.setTableScanNode(tableScanNodeBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  @Override
  public Plan.PlanNode visitValue(ValueNode node, List<Plan.PlanNode> context) {
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitWindow(WindowNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitSetOp(SetOpNode node, List<Plan.PlanNode> context) {
    node.getInputs().forEach(input -> input.visit(this, context));
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitExchange(ExchangeNode node, List<Plan.PlanNode> context) {
    node.getInputs().forEach(input -> input.visit(this, context));
    return process(node, context);
  }
}
