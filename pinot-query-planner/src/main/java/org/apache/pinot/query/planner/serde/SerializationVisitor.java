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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
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
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.MailboxReceiveNode.Builder receiveNodeBuilder =
        Plan.MailboxReceiveNode.newBuilder().setSenderStageId(node.getSenderStageId())
            .setDistributionType(convertDistributionType(node.getDistributionType()))
            .setExchangeType(convertExchangeType(node.getExchangeType())).addAllCollationKeys(
                node.getCollationKeys().stream().map((e) -> ((RexExpression.InputRef) e).getIndex())
                    .collect(Collectors.toList())).addAllCollationDirections(
                node.getCollationDirections().stream().map(SerializationVisitor::convertDirection)
                    .collect(Collectors.toList())).addAllCollationNullDirections(
                node.getCollationNullDirections().stream().map(SerializationVisitor::convertNullDirection)
                    .collect(Collectors.toList())).setSortOnSender(node.isSortOnSender())
            .setSortOnReceiver(node.isSortOnReceiver())
            .setSender(visitMailboxSend(node.getSender(), new ArrayList<>()));
    if (node.getDistributionKeys() != null) {
      receiveNodeBuilder.addAllDistributionKeys(node.getDistributionKeys());
    }
    builder.setReceiveNode(receiveNodeBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  @Override
  public Plan.PlanNode visitMailboxSend(MailboxSendNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.MailboxSendNode.Builder sendNodeBuilder = getSendNodeBuilder(node);
    builder.setSendNode(sendNodeBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  private static Plan.MailboxSendNode.Builder getSendNodeBuilder(MailboxSendNode node) {
    Plan.MailboxSendNode.Builder builder =
        Plan.MailboxSendNode.newBuilder().setReceiverStageId(node.getReceiverStageId())
            .setExchangeType(convertExchangeType(node.getExchangeType()))
            .setDistributionType(convertDistributionType(node.getDistributionType())).addAllCollationKeys(
                node.getCollationKeys().stream().map((e) -> ((RexExpression.InputRef) e).getIndex())
                    .collect(Collectors.toList())).addAllCollationDirections(
                node.getCollationDirections().stream().map(SerializationVisitor::convertDirection)
                    .collect(Collectors.toList())).setSortOnSender(node.isSortOnSender())
            .setPrePartitioned(node.isPrePartitioned());
    if (node.getDistributionKeys() != null) {
      builder.addAllDistributionKeys(node.getDistributionKeys());
    }

    return builder;
  }

  @Override
  public Plan.PlanNode visitProject(ProjectNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public Plan.PlanNode visitSort(SortNode node, List<Plan.PlanNode> context) {
    node.getInputs().get(0).visit(this, context);
    Plan.PlanNode.Builder builder = getBuilder(node, context);

    List<Plan.InputRef> inputRefList = node.getCollationKeys().stream()
        .map(expr -> Plan.InputRef.newBuilder().setIndex(((RexExpression.InputRef) expr).getIndex()).build())
        .collect(Collectors.toList());
    Plan.SortNode.Builder sortNodeBuilder = Plan.SortNode.newBuilder().addAllCollationKeys(inputRefList)
        .addAllCollationDirections(node.getCollationDirections().stream().map(SerializationVisitor::convertDirection)
            .collect(Collectors.toList())).addAllCollationNullDirections(
            node.getCollationNullDirections().stream().map(SerializationVisitor::convertNullDirection)
                .collect(Collectors.toList())).setFetch(node.getFetch()).setOffset(node.getOffset());

    builder.setSortNode(sortNodeBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  @Override
  public Plan.PlanNode visitTableScan(TableScanNode node, List<Plan.PlanNode> context) {
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.TableScanNode.Builder tableScanNodeBuilder = Plan.TableScanNode.newBuilder().setTableName(node.getTableName())
        .addAllTableScanColumns(node.getTableScanColumns()).setNodeHint(getNodeHintBuilder(node.getNodeHint()));
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
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.SetOpNode.Builder setOpBuilder =
        Plan.SetOpNode.newBuilder().setSetOpType(convertSetOpType(node.getSetOpType())).setAll(node.isAll());
    builder.setSetNode(setOpBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  @Override
  public Plan.PlanNode visitExchange(ExchangeNode node, List<Plan.PlanNode> context) {
    node.getInputs().forEach(input -> input.visit(this, context));
    Plan.PlanNode.Builder builder = getBuilder(node, context);
    Plan.ExchangeNode.Builder exchangeNodeBuilder =
        Plan.ExchangeNode.newBuilder().setExchangeType(convertExchangeType(node.getExchangeType()))
            .setDistributionType(convertDistributionType(node.getDistributionType()))
            .addAllKeys(node.getDistributionKeys()).setIsSortOnSender(node.isSortOnSender())
            .setIsSortOnReceiver(node.isSortOnReceiver()).setIsPrePartitioned(node.isPrePartitioned()).addAllCollations(
                node.getCollations().stream().map(c -> Plan.RelFieldCollation.newBuilder().setFieldIndex(c.getFieldIndex())
                    .setDirection(convertDirection(c.getDirection()))
                    .setNullDirection(convertNullDirection(c.nullDirection)).build()).collect(Collectors.toList()))
            .addAllTableNames(node.getTableNames());
    builder.setExchangeNode(exchangeNodeBuilder);

    context.clear();
    Plan.PlanNode protoPlanNode = builder.build();
    context.add(protoPlanNode);

    return protoPlanNode;
  }

  private static Plan.PlanNode.Builder getBuilder(AbstractPlanNode planNode, List<Plan.PlanNode> context) {
    Plan.PlanNode.Builder builder = Plan.PlanNode.newBuilder().setStageId(planNode.getPlanFragmentId())
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

  private static Plan.RelDistributionType convertDistributionType(RelDistribution.Type type) {
    switch (type) {
      case SINGLETON:
        return Plan.RelDistributionType.SINGLETON;
      case HASH_DISTRIBUTED:
        return Plan.RelDistributionType.HASH_DISTRIBUTED;
      case RANGE_DISTRIBUTED:
        return Plan.RelDistributionType.RANGE_DISTRIBUTED;
      case RANDOM_DISTRIBUTED:
        return Plan.RelDistributionType.RANDOM_DISTRIBUTED;
      case ROUND_ROBIN_DISTRIBUTED:
        return Plan.RelDistributionType.ROUND_ROBIN_DISTRIBUTED;
      case BROADCAST_DISTRIBUTED:
        return Plan.RelDistributionType.BROADCAST_DISTRIBUTED;
      case ANY:
        return Plan.RelDistributionType.ANY;
    }
    throw new RuntimeException(String.format("Unknown RelDistribution.Type %s", type));
  }

  private static Plan.PinotRelExchangeType convertExchangeType(PinotRelExchangeType exchangeType) {
    switch (exchangeType) {
      case SUB_PLAN:
        return Plan.PinotRelExchangeType.SUB_PLAN;
      case STREAMING:
        return Plan.PinotRelExchangeType.STREAMING;
      case PIPELINE_BREAKER:
        return Plan.PinotRelExchangeType.PIPELINE_BREAKER;
    }

    throw new RuntimeException(String.format("Unknown PinotRelExchangeType %s", exchangeType));
  }

  private static Plan.Direction convertDirection(RelFieldCollation.Direction direction) {
    switch (direction) {
      case ASCENDING:
        return Plan.Direction.ASCENDING;
      case STRICTLY_ASCENDING:
        return Plan.Direction.STRICTLY_ASCENDING;
      case DESCENDING:
        return Plan.Direction.DESCENDING;
      case STRICTLY_DESCENDING:
        return Plan.Direction.STRICTLY_DESCENDING;
      case CLUSTERED:
        return Plan.Direction.CLUSTERED;
    }
    throw new RuntimeException(String.format("Unknown Direction %s", direction));
  }

  private static Plan.NullDirection convertNullDirection(RelFieldCollation.NullDirection nullDirection) {
    switch (nullDirection) {
      case FIRST:
        return Plan.NullDirection.FIRST;
      case LAST:
        return Plan.NullDirection.LAST;
    }

    return Plan.NullDirection.UNSPECIFIED;
  }

  private static Plan.SetOpType convertSetOpType(SetOpNode.SetOpType type) {
    switch (type) {
      case INTERSECT:
        return Plan.SetOpType.INTERSECT;
      case UNION:
        return Plan.SetOpType.UNION;
      case MINUS:
        return Plan.SetOpType.MINUS;
    }
    throw new RuntimeException(String.format("Unknown SetOpType %s", type));
  }
}
