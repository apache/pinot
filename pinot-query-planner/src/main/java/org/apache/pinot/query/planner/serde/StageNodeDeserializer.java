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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
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
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class StageNodeDeserializer {
  private StageNodeDeserializer() {
  }

  public static AbstractPlanNode process(Plan.StageNode protoNode) {
    switch (protoNode.getNodeTypeCase()) {
      case TABLESCANNODE:
        return deserializeTableScanNode(protoNode);
      case RECEIVENODE:
        return deserializeMailboxReceiveNode(protoNode);
      case SENDNODE:
        return deserializeMailboxSendNode(protoNode);
      case SETNODE:
        return deserializeSetNode(protoNode);
      case EXCHANGENODE:
        return deserializeExchangeNode(protoNode);
      case SORTNODE:
        return deserializeSortNode(protoNode);
      case WINDOWNODE:
        return deserializeWindowNode(protoNode);
      case VALUENODE:
        return deserializeValueNode(protoNode);
      case PROJECTNODE:
        return deserializeProjectNode(protoNode);
      case FILTERNODE:
        return deserializeFilterNode(protoNode);
      case AGGREGATENODE:
        return deserializeAggregateNode(protoNode);
      case JOINNODE:
        return deserializeJoinNode(protoNode);
      default:
        throw new RuntimeException(String.format("Unknown Node Type %s", protoNode.getNodeTypeCase()));
    }
  }

  private static AbstractPlanNode deserializeTableScanNode(Plan.StageNode protoNode) {
    Plan.TableScanNode protoTableNode = protoNode.getTableScanNode();
    List<String> list = new ArrayList<>(protoTableNode.getTableScanColumnsList());
    return new TableScanNode(protoNode.getStageId(), extractDataSchema(protoNode),
        extractNodeHint(protoTableNode.getNodeHint()), protoTableNode.getTableName(), list);
  }

  private static AbstractPlanNode deserializeMailboxReceiveNode(Plan.StageNode protoNode) {
    Plan.MailboxReceiveNode protoReceiveNode = protoNode.getReceiveNode();
    return new MailboxReceiveNode(protoNode.getStageId(), extractDataSchema(protoNode),
        protoReceiveNode.getSenderStageId(), convertDistributionType(protoReceiveNode.getDistributionType()),
        convertExchangeType(protoReceiveNode.getExchangeType()),
        protoReceiveNode.hasDistributionKeys() ? protoReceiveNode.getDistributionKeys().getItemList() : null,
        protoReceiveNode.getCollationKeysList().stream().map(RexExpression.InputRef::new).collect(Collectors.toList()),
        protoReceiveNode.getCollationDirections().getItemList().stream().map(StageNodeDeserializer::convertDirection)
            .collect(Collectors.toList()), protoReceiveNode.getCollationNullDirections().getItemList().stream()
        .map(StageNodeDeserializer::convertNullDirection).collect(Collectors.toList()),
        protoReceiveNode.getSortOnSender(), protoReceiveNode.getSortOnReceiver(),
        (MailboxSendNode) deserializeMailboxSendNode(protoReceiveNode.getSender()));
  }

  private static AbstractPlanNode deserializeMailboxSendNode(Plan.StageNode protoNode) {
    Plan.MailboxSendNode protoSendNode = protoNode.getSendNode();
    MailboxSendNode sendNode =
        new MailboxSendNode(protoNode.getStageId(), extractDataSchema(protoNode), protoSendNode.getReceiverStageId(),
            convertDistributionType(protoSendNode.getDistributionType()),
            convertExchangeType(protoSendNode.getExchangeType()),
            protoSendNode.hasDistributionKeys() ? protoSendNode.getDistributionKeys().getItemList() : null,
            protoSendNode.getCollationKeysList().stream().map(RexExpression.InputRef::new).collect(Collectors.toList()),
            protoSendNode.getCollationDirections().getItemList().stream().map(StageNodeDeserializer::convertDirection)
                .collect(Collectors.toList()), protoSendNode.getSortOnSender(), protoSendNode.getPrePartitioned());

    protoNode.getInputsList().forEach((i) -> sendNode.addInput(process(i)));
    return sendNode;
  }

  private static AbstractPlanNode deserializeSetNode(Plan.StageNode protoNode) {
    Plan.SetOpNode protoSetOpNode = protoNode.getSetNode();
    SetOpNode setOpNode = new SetOpNode(convertSetOpType(protoSetOpNode.getSetOpType()), protoNode.getStageId(),
        extractDataSchema(protoNode), protoSetOpNode.getAll());
    protoNode.getInputsList().forEach((i) -> setOpNode.addInput(process(i)));
    return setOpNode;
  }

  private static AbstractPlanNode deserializeExchangeNode(Plan.StageNode protoNode) {
    Plan.ExchangeNode protoExchangeNode = protoNode.getExchangeNode();

    Set<String> tableNames = new HashSet<>(protoExchangeNode.getTableNamesList());
    List<RelFieldCollation> collations = protoExchangeNode.getCollationsList().stream().map(
        c -> new RelFieldCollation(c.getFieldIndex(), convertDirection(c.getDirection()),
            convertNullDirection(c.getNullDirection()))).collect(Collectors.toList());

    ExchangeNode exchangeNode = new ExchangeNode(protoNode.getStageId(), extractDataSchema(protoNode),
        convertExchangeType(protoExchangeNode.getExchangeType()), tableNames, protoExchangeNode.getKeysList(),
        convertDistributionType(protoExchangeNode.getDistributionType()), collations,
        protoExchangeNode.getIsSortOnSender(), protoExchangeNode.getIsSortOnReceiver(),
        protoExchangeNode.getIsPrePartitioned());
    protoNode.getInputsList().forEach((i) -> exchangeNode.addInput(process(i)));

    return exchangeNode;
  }

  private static AbstractPlanNode deserializeSortNode(Plan.StageNode protoNode) {
    Plan.SortNode protoSortNode = protoNode.getSortNode();

    List<RexExpression> expressions =
        protoSortNode.getCollationKeys().getItemList().stream().map(ProtoExpressionToRexExpression::process)
            .collect(Collectors.toList());
    List<RelFieldCollation.Direction> directions =
        protoSortNode.getCollationDirections().getItemList().stream().map(StageNodeDeserializer::convertDirection)
            .collect(Collectors.toList());
    List<RelFieldCollation.NullDirection> nullDirections =
        protoSortNode.getCollationNullDirections().getItemList().stream()
            .map(StageNodeDeserializer::convertNullDirection).collect(Collectors.toList());

    SortNode sortNode =
        new SortNode(protoNode.getStageId(), expressions, directions, nullDirections, protoSortNode.getFetch(),
            protoSortNode.getOffset(), extractDataSchema(protoNode));
    protoNode.getInputsList().forEach((i) -> sortNode.addInput(process(i)));
    return sortNode;
  }

  private static AbstractPlanNode deserializeWindowNode(Plan.StageNode protoNode) {
    Plan.WindowNode protoWindowNode = protoNode.getWindowNode();

    List<RelFieldCollation.Direction> orderSetDirection =
        protoWindowNode.getOrderSetDirectionList().stream().map(StageNodeDeserializer::convertDirection)
            .collect(Collectors.toList());
    List<RelFieldCollation.NullDirection> orderSetNullDirection =
        protoWindowNode.getOrderSetNullDirectionList().stream().map(StageNodeDeserializer::convertNullDirection)
            .collect(Collectors.toList());

    WindowNode windowNode = new WindowNode(protoNode.getStageId(), extractDataSchema(protoNode),
        convertExpressions(protoWindowNode.getGroupSet()), convertExpressions(protoWindowNode.getOrderSet()),
        orderSetDirection, orderSetNullDirection, convertExpressions(protoWindowNode.getAggCalls()),
        protoWindowNode.getLowerBound(), protoWindowNode.getUpperBound(),
        convertExpressions(protoWindowNode.getConstants()),
        convertWindowFrameType(protoWindowNode.getWindowFrameType()));
    protoNode.getInputsList().forEach((i) -> windowNode.addInput(process(i)));
    return windowNode;
  }

  private static AbstractPlanNode deserializeValueNode(Plan.StageNode protoNode) {
    Plan.ValueNode protoSortNode = protoNode.getValueNode();
    List<List<RexExpression>> rows = new ArrayList<>();

    for (Plan.RexExpressionList row : protoSortNode.getRowsList()) {
      rows.add(row.getItemList().stream().map(ProtoExpressionToRexExpression::process).collect(Collectors.toList()));
    }

    ValueNode valueNode = new ValueNode(protoNode.getStageId(), extractDataSchema(protoNode), rows);
    protoNode.getInputsList().forEach((i) -> valueNode.addInput(process(i)));
    return valueNode;
  }

  private static AbstractPlanNode deserializeProjectNode(Plan.StageNode protoNode) {
    Plan.ProjectNode protoProjectNode = protoNode.getProjectNode();

    ProjectNode projectNode = new ProjectNode(protoNode.getStageId(), extractDataSchema(protoNode),
        convertExpressions(protoProjectNode.getProjects()));
    protoNode.getInputsList().forEach((i) -> projectNode.addInput(process(i)));
    return projectNode;
  }

  private static AbstractPlanNode deserializeFilterNode(Plan.StageNode protoNode) {
    Plan.FilterNode protoFilterNode = protoNode.getFilterNode();

    RexExpression condition = ProtoExpressionToRexExpression.process(protoFilterNode.getCondition());

    FilterNode filterNode = new FilterNode(protoNode.getStageId(), extractDataSchema(protoNode), condition);
    protoNode.getInputsList().forEach((i) -> filterNode.addInput(process(i)));
    return filterNode;
  }

  private static AbstractPlanNode deserializeAggregateNode(Plan.StageNode protoNode) {
    Plan.AggregateNode protoAggregateNode = protoNode.getAggregateNode();

    AggregateNode aggregateNode = new AggregateNode(protoNode.getStageId(), extractDataSchema(protoNode),
        convertExpressions(protoAggregateNode.getAggCalls()), protoAggregateNode.getFilterArgIndicesList(),
        convertExpressions(protoAggregateNode.getGroupSet()), extractNodeHint(protoAggregateNode.getNodeHint()),
        convertAggType(protoAggregateNode.getAggType()));
    protoNode.getInputsList().forEach((i) -> aggregateNode.addInput(process(i)));
    return aggregateNode;
  }

  private static AbstractPlanNode deserializeJoinNode(Plan.StageNode protoNode) {
    Plan.JoinNode protoJoinNode = protoNode.getJoinNode();

    JoinNode.JoinKeys joinKeys = new JoinNode.JoinKeys(protoJoinNode.getJoinKeys().getLeftKeysList(),
        protoJoinNode.getJoinKeys().getRightKeysList());
    JoinNode joinNode =
        new JoinNode(protoNode.getStageId(), extractDataSchema(protoNode), protoJoinNode.getLeftColumnNamesList(),
            protoJoinNode.getRightColumnNamesList(), convertJoinRelType(protoJoinNode.getJoinRelType()), joinKeys,
            convertExpressions(protoJoinNode.getJoinClause()), extractNodeHint(protoJoinNode.getJoinHints()));
    protoNode.getInputsList().forEach((i) -> joinNode.addInput(process(i)));
    return joinNode;
  }

  private static AbstractPlanNode.NodeHint extractNodeHint(Plan.NodeHint protoNodeHint) {
    AbstractPlanNode.NodeHint nodeHint = new AbstractPlanNode.NodeHint();
    nodeHint._hintOptions = new HashMap<>();
    protoNodeHint.getHintOptionsMap().forEach((key, value) -> nodeHint._hintOptions.put(key, value.getOptionsMap()));

    return nodeHint;
  }

  private static DataSchema extractDataSchema(Plan.StageNode protoNode) {
    List<DataSchema.ColumnDataType> columnDataTypesList =
        protoNode.getColumnDataTypesList().stream().map(ProtoExpressionToRexExpression::convertColumnDataType)
            .collect(Collectors.toList());
    String[] columnNames = protoNode.getColumnNamesList().toArray(new String[]{});
    return new DataSchema(columnNames, columnDataTypesList.toArray(new DataSchema.ColumnDataType[]{}));
  }

  private static List<RexExpression> convertExpressions(Plan.RexExpressionList expressionList) {
    return expressionList.getItemList().stream().map(ProtoExpressionToRexExpression::process)
        .collect(Collectors.toList());
  }

  private static RelDistribution.Type convertDistributionType(Plan.RelDistributionType type) {
    switch (type) {
      case SINGLETON:
        return RelDistribution.Type.SINGLETON;
      case HASH_DISTRIBUTED:
        return RelDistribution.Type.HASH_DISTRIBUTED;
      case RANGE_DISTRIBUTED:
        return RelDistribution.Type.RANGE_DISTRIBUTED;
      case RANDOM_DISTRIBUTED:
        return RelDistribution.Type.RANDOM_DISTRIBUTED;
      case ROUND_ROBIN_DISTRIBUTED:
        return RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED;
      case BROADCAST_DISTRIBUTED:
        return RelDistribution.Type.BROADCAST_DISTRIBUTED;
      case ANY:
        return RelDistribution.Type.ANY;
      default:
    }
    throw new RuntimeException(String.format("Unknown RelDistribution.Type %s", type));
  }

  private static PinotRelExchangeType convertExchangeType(Plan.PinotRelExchangeType exchangeType) {
    switch (exchangeType) {
      case SUB_PLAN:
        return PinotRelExchangeType.SUB_PLAN;
      case STREAMING:
        return PinotRelExchangeType.STREAMING;
      case PIPELINE_BREAKER:
        return PinotRelExchangeType.PIPELINE_BREAKER;
      default:
    }

    throw new RuntimeException(String.format("Unknown PinotRelExchangeType %s", exchangeType));
  }

  private static RelFieldCollation.Direction convertDirection(Plan.Direction direction) {
    switch (direction) {
      case ASCENDING:
        return RelFieldCollation.Direction.ASCENDING;
      case STRICTLY_ASCENDING:
        return RelFieldCollation.Direction.STRICTLY_ASCENDING;
      case DESCENDING:
        return RelFieldCollation.Direction.DESCENDING;
      case STRICTLY_DESCENDING:
        return RelFieldCollation.Direction.STRICTLY_DESCENDING;
      case CLUSTERED:
        return RelFieldCollation.Direction.CLUSTERED;
      default:
    }
    throw new RuntimeException(String.format("Unknown Direction %s", direction));
  }

  private static RelFieldCollation.NullDirection convertNullDirection(Plan.NullDirection nullDirection) {
    switch (nullDirection) {
      case FIRST:
        return RelFieldCollation.NullDirection.FIRST;
      case LAST:
        return RelFieldCollation.NullDirection.LAST;
      default:
        return RelFieldCollation.NullDirection.UNSPECIFIED;
    }
  }

  private static SetOpNode.SetOpType convertSetOpType(Plan.SetOpType type) {
    switch (type) {
      case INTERSECT:
        return SetOpNode.SetOpType.INTERSECT;
      case UNION:
        return SetOpNode.SetOpType.UNION;
      case MINUS:
        return SetOpNode.SetOpType.MINUS;
      default:
    }
    throw new RuntimeException(String.format("Unknown SetOpType %s", type));
  }

  private static WindowNode.WindowFrameType convertWindowFrameType(Plan.WindowFrameType windowFrameType) {
    switch (windowFrameType) {
      case ROWS:
        return WindowNode.WindowFrameType.ROWS;
      case RANGE:
        return WindowNode.WindowFrameType.RANGE;
      default:
    }

    throw new RuntimeException(String.format("Unknown WindowFrameType %s", windowFrameType));
  }

  private static AggregateNode.AggType convertAggType(Plan.AggType type) {
    switch (type) {
      case DIRECT:
        return AggregateNode.AggType.DIRECT;
      case LEAF:
        return AggregateNode.AggType.LEAF;
      case INTERMEDIATE:
        return AggregateNode.AggType.INTERMEDIATE;
      case FINAL:
        return AggregateNode.AggType.FINAL;
      default:
    }
    throw new RuntimeException(String.format("Unknown AggType %s", type));
  }

  private static JoinRelType convertJoinRelType(Plan.JoinRelType joinRelType) {
    switch (joinRelType) {
      case ANTI:
        return JoinRelType.ANTI;
      case FULL:
        return JoinRelType.FULL;
      case LEFT:
        return JoinRelType.LEFT;
      case SEMI:
        return JoinRelType.SEMI;
      case INNER:
        return JoinRelType.INNER;
      case RIGHT:
        return JoinRelType.RIGHT;
      default:
    }
    throw new RuntimeException(String.format("Unknown JoinRelType %s", joinRelType));
  }
}
