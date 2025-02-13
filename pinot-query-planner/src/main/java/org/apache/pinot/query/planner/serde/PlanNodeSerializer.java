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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
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

  public static Plan.PlanNode process(PlanNode planNode) {
    Plan.PlanNode.Builder builder = Plan.PlanNode.newBuilder()
        .setStageId(planNode.getStageId())
        .setDataSchema(convertDataSchema(planNode.getDataSchema()))
        .setNodeHint(convertNodeHint(planNode.getNodeHint()));
    planNode.visit(SerializationVisitor.INSTANCE, builder);
    planNode.getInputs().forEach(input -> builder.addInputs(process(input)));
    return builder.build();
  }

  private static Plan.DataSchema convertDataSchema(DataSchema dataSchema) {
    Plan.DataSchema.Builder dataSchemaBuilder = Plan.DataSchema.newBuilder();
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      dataSchemaBuilder.addColumnNames(columnNames[i]);
      dataSchemaBuilder.addColumnDataTypes(RexExpressionToProtoExpression.convertColumnDataType(columnDataTypes[i]));
    }
    return dataSchemaBuilder.build();
  }

  private static Plan.NodeHint convertNodeHint(NodeHint nodeHint) {
    Plan.NodeHint.Builder nodeHintBuilder = Plan.NodeHint.newBuilder();
    for (Map.Entry<String, Map<String, String>> entry : nodeHint.getHintOptions().entrySet()) {
      Plan.StrStrMap.Builder strStrMapBuilder = Plan.StrStrMap.newBuilder();
      strStrMapBuilder.putAllOptions(entry.getValue());
      nodeHintBuilder.putHintOptions(entry.getKey(), strStrMapBuilder.build());
    }
    return nodeHintBuilder.build();
  }

  private static class SerializationVisitor implements PlanNodeVisitor<Void, Plan.PlanNode.Builder> {
    public static final SerializationVisitor INSTANCE = new SerializationVisitor();

    private SerializationVisitor() {
    }

    @Override
    public Void visitAggregate(AggregateNode node, Plan.PlanNode.Builder builder) {
      Plan.AggregateNode aggregateNode = Plan.AggregateNode.newBuilder()
          .addAllAggCalls(convertFunctionCalls(node.getAggCalls()))
          .addAllFilterArgs(node.getFilterArgs())
          .addAllGroupKeys(node.getGroupKeys())
          .setAggType(convertAggType(node.getAggType()))
          .build();
      builder.setAggregateNode(aggregateNode);
      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Plan.PlanNode.Builder builder) {
      Plan.FilterNode filterNode = Plan.FilterNode.newBuilder()
          .setCondition(RexExpressionToProtoExpression.convertExpression(node.getCondition()))
          .build();
      builder.setFilterNode(filterNode);
      return null;
    }

    @Override
    public Void visitJoin(JoinNode node, Plan.PlanNode.Builder builder) {
      Plan.JoinNode joinNode = Plan.JoinNode.newBuilder()
          .setJoinType(convertJoinType(node.getJoinType()))
          .addAllLeftKeys(node.getLeftKeys())
          .addAllRightKeys(node.getRightKeys())
          .addAllNonEquiConditions(convertExpressions(node.getNonEquiConditions()))
          .setJoinStrategy(convertJoinStrategy(node.getJoinStrategy()))
          .build();
      builder.setJoinNode(joinNode);
      return null;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, Plan.PlanNode.Builder builder) {
      Plan.MailboxReceiveNode mailboxReceiveNode = Plan.MailboxReceiveNode.newBuilder()
          .setSenderStageId(node.getSenderStageId())
          .setExchangeType(convertExchangeType(node.getExchangeType()))
          .setDistributionType(convertDistributionType(node.getDistributionType()))
          .addAllKeys(node.getKeys())
          .addAllCollations(convertCollations(node.getCollations()))
          .setSort(node.isSort())
          .setSortedOnSender(node.isSortedOnSender())
          .build();
      builder.setMailboxReceiveNode(mailboxReceiveNode);
      return null;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Plan.PlanNode.Builder builder) {
      List<Integer> receiverStageIds = new ArrayList<>();
      for (Integer receiverStageId : node.getReceiverStageIds()) {
        receiverStageIds.add(receiverStageId);
      }
      assert !receiverStageIds.isEmpty() : "Receiver stage IDs should not be empty";

      Plan.MailboxSendNode mailboxSendNode =
          Plan.MailboxSendNode.newBuilder()
          .setExchangeType(convertExchangeType(node.getExchangeType()))
          .setDistributionType(convertDistributionType(node.getDistributionType()))
          .addAllKeys(node.getKeys())
          .setPrePartitioned(node.isPrePartitioned())
          .addAllCollations(convertCollations(node.getCollations()))
          .setSort(node.isSort())
          .build();
      builder.setMailboxSendNode(mailboxSendNode);
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Plan.PlanNode.Builder builder) {
      Plan.ProjectNode projectNode =
          Plan.ProjectNode.newBuilder().addAllProjects(convertExpressions(node.getProjects())).build();
      builder.setProjectNode(projectNode);
      return null;
    }

    @Override
    public Void visitSetOp(SetOpNode node, Plan.PlanNode.Builder builder) {
      Plan.SetOpNode setOpNode =
          Plan.SetOpNode.newBuilder().setSetOpType(convertSetOpType(node.getSetOpType())).setAll(node.isAll()).build();
      builder.setSetOpNode(setOpNode);
      return null;
    }

    @Override
    public Void visitSort(SortNode node, Plan.PlanNode.Builder builder) {
      Plan.SortNode sortNode = Plan.SortNode.newBuilder()
          .addAllCollations(convertCollations(node.getCollations()))
          .setFetch(node.getFetch())
          .setOffset(node.getOffset())
          .build();
      builder.setSortNode(sortNode);
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Plan.PlanNode.Builder builder) {
      Plan.TableScanNode tableScanNode =
          Plan.TableScanNode.newBuilder().setTableName(node.getTableName()).addAllColumns(node.getColumns()).build();
      builder.setTableScanNode(tableScanNode);
      return null;
    }

    @Override
    public Void visitValue(ValueNode node, Plan.PlanNode.Builder builder) {
      Plan.ValueNode valueNode =
          Plan.ValueNode.newBuilder().addAllLiteralRows(convertLiteralRows(node.getLiteralRows())).build();
      builder.setValueNode(valueNode);
      return null;
    }

    @Override
    public Void visitWindow(WindowNode node, Plan.PlanNode.Builder builder) {
      Plan.WindowNode windowNode = Plan.WindowNode.newBuilder()
          .addAllAggCalls(convertFunctionCalls(node.getAggCalls()))
          .addAllKeys(node.getKeys())
          .addAllCollations(convertCollations(node.getCollations()))
          .setWindowFrameType(convertWindowFrameType(node.getWindowFrameType()))
          .setLowerBound(node.getLowerBound())
          .setUpperBound(node.getUpperBound())
          .addAllConstants(convertLiterals(node.getConstants()))
          .build();
      builder.setWindowNode(windowNode);
      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode exchangeNode, Plan.PlanNode.Builder context) {
      throw new IllegalStateException("ExchangeNode should not be visited by SerializationVisitor");
    }

    @Override
    public Void visitExplained(ExplainedNode node, Plan.PlanNode.Builder builder) {
      Plan.ExplainNode explainNode =
          Plan.ExplainNode.newBuilder().setTitle(node.getTitle()).putAllAttributes(node.getAttributes()).build();
      builder.setExplainNode(explainNode);
      return null;
    }

    private static List<Expressions.Expression> convertExpressions(List<RexExpression> expressions) {
      List<Expressions.Expression> protoExpressions = new ArrayList<>(expressions.size());
      for (RexExpression expression : expressions) {
        protoExpressions.add(RexExpressionToProtoExpression.convertExpression(expression));
      }
      return protoExpressions;
    }

    private static List<Expressions.FunctionCall> convertFunctionCalls(List<RexExpression.FunctionCall> functionCalls) {
      List<Expressions.FunctionCall> protoFunctionCalls = new ArrayList<>(functionCalls.size());
      for (RexExpression.FunctionCall functionCall : functionCalls) {
        protoFunctionCalls.add(RexExpressionToProtoExpression.convertFunctionCall(functionCall));
      }
      return protoFunctionCalls;
    }

    private static List<Expressions.Literal> convertLiterals(List<RexExpression.Literal> literals) {
      List<Expressions.Literal> protoLiterals = new ArrayList<>(literals.size());
      for (RexExpression.Literal literal : literals) {
        protoLiterals.add(RexExpressionToProtoExpression.convertLiteral(literal));
      }
      return protoLiterals;
    }

    private static Plan.AggType convertAggType(AggregateNode.AggType aggType) {
      switch (aggType) {
        case DIRECT:
          return Plan.AggType.DIRECT;
        case LEAF:
          return Plan.AggType.LEAF;
        case INTERMEDIATE:
          return Plan.AggType.INTERMEDIATE;
        case FINAL:
          return Plan.AggType.FINAL;
        default:
          throw new IllegalStateException("Unsupported AggType: " + aggType);
      }
    }

    private static Plan.JoinType convertJoinType(JoinRelType joinType) {
      switch (joinType) {
        case INNER:
          return Plan.JoinType.INNER;
        case LEFT:
          return Plan.JoinType.LEFT;
        case RIGHT:
          return Plan.JoinType.RIGHT;
        case FULL:
          return Plan.JoinType.FULL;
        case SEMI:
          return Plan.JoinType.SEMI;
        case ANTI:
          return Plan.JoinType.ANTI;
        default:
          throw new IllegalStateException("Unsupported JoinRelType: " + joinType);
      }
    }

    private static Plan.JoinStrategy convertJoinStrategy(JoinNode.JoinStrategy joinStrategy) {
      switch (joinStrategy) {
        case HASH:
          return Plan.JoinStrategy.HASH;
        case LOOKUP:
          return Plan.JoinStrategy.LOOKUP;
        default:
          throw new IllegalStateException("Unsupported JoinStrategy: " + joinStrategy);
      }
    }

    private static Plan.ExchangeType convertExchangeType(PinotRelExchangeType exchangeType) {
      switch (exchangeType) {
        case STREAMING:
          return Plan.ExchangeType.STREAMING;
        case SUB_PLAN:
          return Plan.ExchangeType.SUB_PLAN;
        case PIPELINE_BREAKER:
          return Plan.ExchangeType.PIPELINE_BREAKER;
        default:
          throw new IllegalStateException("Unsupported PinotRelExchangeType: " + exchangeType);
      }
    }

    private static Plan.DistributionType convertDistributionType(RelDistribution.Type distributionType) {
      switch (distributionType) {
        case SINGLETON:
          return Plan.DistributionType.SINGLETON;
        case HASH_DISTRIBUTED:
          return Plan.DistributionType.HASH_DISTRIBUTED;
        case RANGE_DISTRIBUTED:
          return Plan.DistributionType.RANGE_DISTRIBUTED;
        case RANDOM_DISTRIBUTED:
          return Plan.DistributionType.RANDOM_DISTRIBUTED;
        case ROUND_ROBIN_DISTRIBUTED:
          return Plan.DistributionType.ROUND_ROBIN_DISTRIBUTED;
        case BROADCAST_DISTRIBUTED:
          return Plan.DistributionType.BROADCAST_DISTRIBUTED;
        case ANY:
          return Plan.DistributionType.ANY;
        default:
          throw new IllegalStateException("Unsupported RelDistribution.Type: " + distributionType);
      }
    }

    private static Plan.Direction convertDirection(RelFieldCollation.Direction direction) {
      switch (direction) {
        case ASCENDING:
          return Plan.Direction.ASCENDING;
        case DESCENDING:
          return Plan.Direction.DESCENDING;
        // TODO: Currently we only support ASCENDING and DESCENDING.
        default:
          throw new IllegalStateException("Unsupported RelFieldCollation.Direction: " + direction);
      }
    }

    private static Plan.NullDirection convertNullDirection(RelFieldCollation.NullDirection nullDirection) {
      switch (nullDirection) {
        case FIRST:
          return Plan.NullDirection.FIRST;
        case LAST:
          return Plan.NullDirection.LAST;
        case UNSPECIFIED:
          return Plan.NullDirection.UNSPECIFIED;
        default:
          throw new IllegalStateException("Unsupported RelFieldCollation.NullDirection: " + nullDirection);
      }
    }

    private static List<Plan.Collation> convertCollations(List<RelFieldCollation> collations) {
      List<Plan.Collation> protoCollations = new ArrayList<>(collations.size());
      for (RelFieldCollation collation : collations) {
        protoCollations.add(Plan.Collation.newBuilder()
            .setIndex(collation.getFieldIndex())
            .setDirection(convertDirection(collation.direction))
            .setNullDirection(convertNullDirection(collation.nullDirection))
            .build());
      }
      return protoCollations;
    }

    private static Plan.SetOpType convertSetOpType(SetOpNode.SetOpType setOpType) {
      switch (setOpType) {
        case UNION:
          return Plan.SetOpType.UNION;
        case INTERSECT:
          return Plan.SetOpType.INTERSECT;
        case MINUS:
          return Plan.SetOpType.MINUS;
        default:
          throw new IllegalStateException("Unsupported SetOpType: " + setOpType);
      }
    }

    private static Plan.LiteralRow convertLiteralRow(List<RexExpression.Literal> literalRow) {
      List<Expressions.Literal> values = new ArrayList<>(literalRow.size());
      for (RexExpression.Literal literal : literalRow) {
        values.add(RexExpressionToProtoExpression.convertLiteral(literal));
      }
      return Plan.LiteralRow.newBuilder().addAllValues(values).build();
    }

    private static List<Plan.LiteralRow> convertLiteralRows(List<List<RexExpression.Literal>> literalRows) {
      List<Plan.LiteralRow> protoLiteralRows = new ArrayList<>(literalRows.size());
      for (List<RexExpression.Literal> literalRow : literalRows) {
        protoLiteralRows.add(convertLiteralRow(literalRow));
      }
      return protoLiteralRows;
    }

    private static Plan.WindowFrameType convertWindowFrameType(WindowNode.WindowFrameType windowFrameType) {
      switch (windowFrameType) {
        case ROWS:
          return Plan.WindowFrameType.ROWS;
        case RANGE:
          return Plan.WindowFrameType.RANGE;
        default:
          throw new IllegalStateException("Unsupported WindowFrameType: " + windowFrameType);
      }
    }
  }
}
