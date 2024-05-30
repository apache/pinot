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
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Expressions;
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
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class PlanNodeSerializer {

  private PlanNodeSerializer() {
  }

  public static Plan.StageNode process(AbstractPlanNode planNode) {
    Plan.StageNode.Builder builder = Plan.StageNode.newBuilder().setStageId(planNode.getPlanFragmentId());
    DataSchema dataSchema = planNode.getDataSchema();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      builder.addColumnNames(dataSchema.getColumnName(i));
      builder.addColumnDataTypes(RexExpressionToProtoExpression.convertColumnDataType(dataSchema.getColumnDataType(i)));
    }

    planNode.visit(SerializationVisitor.INSTANCE, builder);
    planNode.getInputs().forEach(input -> builder.addInputs(process((AbstractPlanNode) input)));

    return builder.build();
  }

  private static class SerializationVisitor implements PlanNodeVisitor<Void, Plan.StageNode.Builder> {
    public static final SerializationVisitor INSTANCE = new SerializationVisitor();

    private SerializationVisitor() {
    }

    @Override
    public Void visitAggregate(AggregateNode node, Plan.StageNode.Builder builder) {
      Plan.AggregateNode.Builder aggregateNodeBuilder =
          Plan.AggregateNode.newBuilder().setNodeHint(getNodeHintBuilder(node.getNodeHint()))
              .setAggCalls(convertExpressions(node.getAggCalls())).addAllFilterArgIndices(node.getFilterArgIndices())
              .setGroupSet(convertExpressions(node.getGroupSet())).setAggType(convertAggType(node.getAggType()));

      builder.setAggregateNode(aggregateNodeBuilder);
      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Plan.StageNode.Builder builder) {
      Expressions.RexExpression condition = RexExpressionToProtoExpression.process(node.getCondition());
      Plan.FilterNode.Builder filterNodeBuilder = Plan.FilterNode.newBuilder().setCondition(condition);
      builder.setFilterNode(filterNodeBuilder);

      return null;
    }

    @Override
    public Void visitJoin(JoinNode node, Plan.StageNode.Builder builder) {
      Plan.JoinKeys.Builder joinKeyBuilder = Plan.JoinKeys.newBuilder().addAllLeftKeys(node.getJoinKeys().getLeftKeys())
          .addAllRightKeys(node.getJoinKeys().getRightKeys());

      Plan.JoinNode.Builder joinNodeBuilder =
          Plan.JoinNode.newBuilder().setJoinRelType(convertJoinRelType(node.getJoinRelType()))
              .setJoinKeys(joinKeyBuilder).setJoinClause(convertExpressions(node.getJoinClauses()))
              .setJoinHints(getNodeHintBuilder(node.getJoinHints())).addAllLeftColumnNames(node.getLeftColumnNames())
              .addAllRightColumnNames(node.getRightColumnNames());

      builder.setJoinNode(joinNodeBuilder);

      return null;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, Plan.StageNode.Builder builder) {
      Plan.MailboxReceiveNode.Builder receiveNodeBuilder =
          Plan.MailboxReceiveNode.newBuilder().setSenderStageId(node.getSenderStageId())
              .setDistributionType(convertDistributionType(node.getDistributionType()))
              .setExchangeType(convertExchangeType(node.getExchangeType())).addAllCollationKeys(
                  node.getCollationKeys().stream().map((e) -> ((RexExpression.InputRef) e).getIndex())
                      .collect(Collectors.toList()))
              .setCollationDirections(convertDirectionsList(node.getCollationDirections()))
              .setCollationNullDirections(convertNullDirectionsList(node.getCollationNullDirections()))
              .setSortOnSender(node.isSortOnSender()).setSortOnReceiver(node.isSortOnReceiver())
              .setSender(process(node.getSender()));
      if (node.getDistributionKeys() != null) {
        receiveNodeBuilder.setDistributionKeys(
            Plan.DistributionKeyList.newBuilder().addAllItem(node.getDistributionKeys()).build());
      }
      builder.setReceiveNode(receiveNodeBuilder);

      return null;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Plan.StageNode.Builder builder) {
      Plan.MailboxSendNode.Builder sendNodeBuilder =
          Plan.MailboxSendNode.newBuilder().setReceiverStageId(node.getReceiverStageId())
              .setExchangeType(convertExchangeType(node.getExchangeType()))
              .setDistributionType(convertDistributionType(node.getDistributionType())).addAllCollationKeys(
                  node.getCollationKeys().stream().map((e) -> ((RexExpression.InputRef) e).getIndex())
                      .collect(Collectors.toList()))
              .setCollationDirections(convertDirectionsList(node.getCollationDirections()))
              .setSortOnSender(node.isSortOnSender()).setPrePartitioned(node.isPrePartitioned());
      if (node.getDistributionKeys() != null) {
        sendNodeBuilder.setDistributionKeys(
            Plan.DistributionKeyList.newBuilder().addAllItem(node.getDistributionKeys()).build());
      }

      builder.setSendNode(sendNodeBuilder);
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Plan.StageNode.Builder builder) {
      Plan.ProjectNode.Builder projectNodeBuilder =
          Plan.ProjectNode.newBuilder().setProjects(convertExpressions(node.getProjects()));
      builder.setProjectNode(projectNodeBuilder);

      return null;
    }

    @Override
    public Void visitSort(SortNode node, Plan.StageNode.Builder builder) {
      Plan.SortNode.Builder sortNodeBuilder =
          Plan.SortNode.newBuilder().setCollationKeys(convertExpressions(node.getCollationKeys()))
              .setCollationDirections(convertDirectionsList(node.getCollationDirections()))
              .setCollationNullDirections(convertNullDirectionsList(node.getCollationNullDirections()))
              .setFetch(node.getFetch()).setOffset(node.getOffset());

      builder.setSortNode(sortNodeBuilder);

      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Plan.StageNode.Builder builder) {
      Plan.TableScanNode.Builder tableScanNodeBuilder =
          Plan.TableScanNode.newBuilder().setTableName(node.getTableName())
              .addAllTableScanColumns(node.getTableScanColumns()).setNodeHint(getNodeHintBuilder(node.getNodeHint()));
      builder.setTableScanNode(tableScanNodeBuilder);

      return null;
    }

    @Override
    public Void visitValue(ValueNode node, Plan.StageNode.Builder builder) {
      Plan.ValueNode.Builder valueNodeBuilder = Plan.ValueNode.newBuilder();
      for (List<RexExpression> row : node.getLiteralRows()) {
        Plan.RexExpressionList.Builder exprBuilder = Plan.RexExpressionList.newBuilder();
        List<Expressions.RexExpression> expressions =
            row.stream().map(RexExpressionToProtoExpression::process).collect(Collectors.toList());
        exprBuilder.addAllItem(expressions);
        valueNodeBuilder.addRows(exprBuilder);
      }
      builder.setValueNode(valueNodeBuilder);

      return null;
    }

    @Override
    public Void visitWindow(WindowNode node, Plan.StageNode.Builder builder) {
      List<Plan.Direction> orderSetDirection =
          node.getOrderSetDirection().stream().map(SerializationVisitor::convertDirection).collect(Collectors.toList());
      List<Plan.NullDirection> orderSetNullDirection =
          node.getOrderSetNullDirection().stream().map(SerializationVisitor::convertNullDirection)
              .collect(Collectors.toList());

      Plan.WindowNode.Builder windowNodeBuilder =
          Plan.WindowNode.newBuilder().setGroupSet(convertExpressions(node.getGroupSet()))
              .setOrderSet(convertExpressions(node.getOrderSet())).addAllOrderSetDirection(orderSetDirection)
              .addAllOrderSetNullDirection(orderSetNullDirection).setAggCalls(convertExpressions(node.getAggCalls()))
              .setLowerBound(node.getLowerBound()).setUpperBound(node.getUpperBound())
              .setConstants(convertExpressions(node.getConstants()))
              .setWindowFrameType(convertWindowFrameType(node.getWindowFrameType()));

      builder.setWindowNode(windowNodeBuilder);

      return null;
    }

    @Override
    public Void visitSetOp(SetOpNode node, Plan.StageNode.Builder builder) {
      Plan.SetOpNode.Builder setOpBuilder =
          Plan.SetOpNode.newBuilder().setSetOpType(convertSetOpType(node.getSetOpType())).setAll(node.isAll());
      builder.setSetNode(setOpBuilder);

      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode node, Plan.StageNode.Builder builder) {
      Plan.ExchangeNode.Builder exchangeNodeBuilder =
          Plan.ExchangeNode.newBuilder().setExchangeType(convertExchangeType(node.getExchangeType()))
              .setDistributionType(convertDistributionType(node.getDistributionType()))
              .addAllKeys(node.getDistributionKeys()).setIsSortOnSender(node.isSortOnSender())
              .setIsSortOnReceiver(node.isSortOnReceiver()).setIsPrePartitioned(node.isPrePartitioned())
              .addAllCollations(node.getCollations().stream().map(
                  c -> Plan.RelFieldCollation.newBuilder().setFieldIndex(c.getFieldIndex())
                      .setDirection(convertDirection(c.getDirection()))
                      .setNullDirection(convertNullDirection(c.nullDirection)).build()).collect(Collectors.toList()))
              .addAllTableNames(node.getTableNames());
      builder.setExchangeNode(exchangeNodeBuilder);

      return null;
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

    private static Plan.RexExpressionList.Builder convertExpressions(List<RexExpression> expressions) {
      return Plan.RexExpressionList.newBuilder()
          .addAllItem(expressions.stream().map(RexExpressionToProtoExpression::process).collect(Collectors.toList()));
    }

    private static Plan.DirectionList.Builder convertDirectionsList(List<RelFieldCollation.Direction> directions) {
      return Plan.DirectionList.newBuilder()
          .addAllItem(directions.stream().map(SerializationVisitor::convertDirection).collect(Collectors.toList()));
    }

    private static Plan.NullDirectionList.Builder convertNullDirectionsList(
        List<RelFieldCollation.NullDirection> nullDirections) {
      return Plan.NullDirectionList.newBuilder().addAllItem(
          nullDirections.stream().map(SerializationVisitor::convertNullDirection).collect(Collectors.toList()));
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
        default:
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
        default:
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
        default:
      }
      throw new RuntimeException(String.format("Unknown Direction %s", direction));
    }

    private static Plan.NullDirection convertNullDirection(RelFieldCollation.NullDirection nullDirection) {
      switch (nullDirection) {
        case FIRST:
          return Plan.NullDirection.FIRST;
        case LAST:
          return Plan.NullDirection.LAST;
        default:
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
        default:
      }
      throw new RuntimeException(String.format("Unknown SetOpType %s", type));
    }

    private static Plan.WindowFrameType convertWindowFrameType(WindowNode.WindowFrameType windowFrameType) {
      switch (windowFrameType) {
        case ROWS:
          return Plan.WindowFrameType.ROWS;
        case RANGE:
          return Plan.WindowFrameType.RANGE;
        default:
      }

      throw new RuntimeException(String.format("Unknown WindowFrameType %s", windowFrameType));
    }

    private static Plan.AggType convertAggType(AggregateNode.AggType type) {
      switch (type) {
        case DIRECT:
          return Plan.AggType.DIRECT;
        case LEAF:
          return Plan.AggType.LEAF;
        case INTERMEDIATE:
          return Plan.AggType.INTERMEDIATE;
        case FINAL:
          return Plan.AggType.FINAL;
        default:
      }
      throw new RuntimeException(String.format("Unknown AggType %s", type));
    }

    private static Plan.JoinRelType convertJoinRelType(JoinRelType joinRelType) {
      switch (joinRelType) {
        case ANTI:
          return Plan.JoinRelType.ANTI;
        case FULL:
          return Plan.JoinRelType.FULL;
        case LEFT:
          return Plan.JoinRelType.LEFT;
        case SEMI:
          return Plan.JoinRelType.SEMI;
        case INNER:
          return Plan.JoinRelType.INNER;
        case RIGHT:
          return Plan.JoinRelType.RIGHT;
        default:
      }
      throw new RuntimeException(String.format("Unknown JoinRelType %s", joinRelType));
    }
  }
}
