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


public class DeserializationVisitor {
  public AbstractPlanNode process(Plan.StageNode protoNode) {
    switch (protoNode.getNodeTypeCase()) {
      case TABLESCANNODE:
        return visitTableScanNode(protoNode);
      case RECEIVENODE:
        return visitMailboxReceiveNode(protoNode);
      case SENDNODE:
        return visitMailboxSendNode(protoNode);
      case SETNODE:
        return visitSetNode(protoNode);
      case EXCHANGENODE:
        return visitExchangeNode(protoNode);
      case SORTNODE:
        return visitSortNode(protoNode);
      case WINDOWNODE:
        return visitWindowNode(protoNode);
      case VALUENODE:
        return visitValueNode(protoNode);
      case PROJECTNODE:
        return visitProjectNode(protoNode);
      case FILTERNODE:
        return visitFilterNode(protoNode);
      case AGGREGATENODE:
        return visitAggregateNode(protoNode);
      case JOINNODE:
        return visitJoinNode(protoNode);
      default:
        throw new RuntimeException(String.format("Unknown Node Type %s", protoNode.getNodeTypeCase()));
    }
  }

  private AbstractPlanNode visitTableScanNode(Plan.StageNode protoNode) {
    Plan.TableScanNode protoTableNode = protoNode.getTableScanNode();
    List<String> list = new ArrayList<>(protoTableNode.getTableScanColumnsList());
    return new TableScanNode(protoNode.getStageId(), extractDataSchema(protoNode),
        extractNodeHint(protoTableNode.getNodeHint()), protoTableNode.getTableName(), list);
  }

  private AbstractPlanNode visitMailboxReceiveNode(Plan.StageNode protoNode) {
    Plan.MailboxReceiveNode protoReceiveNode = protoNode.getReceiveNode();
    return new MailboxReceiveNode(protoNode.getStageId(), extractDataSchema(protoNode),
        protoReceiveNode.getSenderStageId(), convertDistributionType(protoReceiveNode.getDistributionType()),
        convertExchangeType(protoReceiveNode.getExchangeType()),
        protoReceiveNode.getDistributionKeysCount() > 0 ? protoReceiveNode.getDistributionKeysList() : null,
        protoReceiveNode.getCollationKeysList().stream().map(RexExpression.InputRef::new).collect(Collectors.toList()),
        protoReceiveNode.getCollationDirectionsList().stream().map(DeserializationVisitor::convertDirection)
            .collect(Collectors.toList()),
        protoReceiveNode.getCollationNullDirectionsList().stream().map(DeserializationVisitor::convertNullDirection)
            .collect(Collectors.toList()), protoReceiveNode.getSortOnSender(), protoReceiveNode.getSortOnReceiver(),
        (MailboxSendNode) visitMailboxSendNode(protoReceiveNode.getSender()));
  }

  private AbstractPlanNode visitMailboxSendNode(Plan.StageNode protoNode) {
    Plan.MailboxSendNode protoSendNode = protoNode.getSendNode();
    MailboxSendNode sendNode =
        new MailboxSendNode(protoNode.getStageId(), extractDataSchema(protoNode), protoSendNode.getReceiverStageId(),
            convertDistributionType(protoSendNode.getDistributionType()),
            convertExchangeType(protoSendNode.getExchangeType()),
            protoSendNode.getDistributionKeysCount() > 0 ? protoSendNode.getDistributionKeysList() : null,
            protoSendNode.getCollationKeysList().stream().map(RexExpression.InputRef::new).collect(Collectors.toList()),
            protoSendNode.getCollationDirectionsList().stream().map(DeserializationVisitor::convertDirection)
                .collect(Collectors.toList()), protoSendNode.getSortOnSender(), protoSendNode.getPrePartitioned());

    protoNode.getInputsList().forEach((i) -> sendNode.addInput(process(i)));
    return sendNode;
  }

  private AbstractPlanNode visitSetNode(Plan.StageNode protoNode) {
    Plan.SetOpNode protoSetOpNode = protoNode.getSetNode();
    SetOpNode setOpNode = new SetOpNode(convertSetOpType(protoSetOpNode.getSetOpType()), protoNode.getStageId(),
        extractDataSchema(protoNode), protoSetOpNode.getAll());
    protoNode.getInputsList().forEach((i) -> setOpNode.addInput(process(i)));
    return setOpNode;
  }

  private AbstractPlanNode visitExchangeNode(Plan.StageNode protoNode) {
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

  private AbstractPlanNode visitSortNode(Plan.StageNode protoNode) {
    Plan.SortNode protoSortNode = protoNode.getSortNode();

    List<RexExpression> expressions =
        protoSortNode.getCollationKeysList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    List<RelFieldCollation.Direction> directions =
        protoSortNode.getCollationDirectionsList().stream().map(DeserializationVisitor::convertDirection)
            .collect(Collectors.toList());
    List<RelFieldCollation.NullDirection> nullDirections =
        protoSortNode.getCollationNullDirectionsList().stream().map(DeserializationVisitor::convertNullDirection)
            .collect(Collectors.toList());

    SortNode sortNode =
        new SortNode(protoNode.getStageId(), expressions, directions, nullDirections, protoSortNode.getFetch(),
            protoSortNode.getOffset(), extractDataSchema(protoNode));
    protoNode.getInputsList().forEach((i) -> sortNode.addInput(process(i)));
    return sortNode;
  }

  private AbstractPlanNode visitWindowNode(Plan.StageNode protoNode) {
    Plan.WindowNode protoWindowNode = protoNode.getWindowNode();

    List<RexExpression> groupSet =
        protoWindowNode.getGroupSetList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    List<RexExpression> orderSet =
        protoWindowNode.getOrderSetList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    List<RelFieldCollation.Direction> orderSetDirection =
        protoWindowNode.getOrderSetDirectionList().stream().map(DeserializationVisitor::convertDirection)
            .collect(Collectors.toList());
    List<RelFieldCollation.NullDirection> orderSetNullDirection =
        protoWindowNode.getOrderSetNullDirectionList().stream().map(DeserializationVisitor::convertNullDirection)
            .collect(Collectors.toList());
    List<RexExpression> aggCalls =
        protoWindowNode.getAggCallsList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    List<RexExpression> constants =
        protoWindowNode.getConstantsList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());

    WindowNode windowNode =
        new WindowNode(protoNode.getStageId(), extractDataSchema(protoNode), groupSet, orderSet, orderSetDirection,
            orderSetNullDirection, aggCalls, protoWindowNode.getLowerBound(), protoWindowNode.getUpperBound(),
            constants, convertWindowFrameType(protoWindowNode.getWindowFrameType()));
    protoNode.getInputsList().forEach((i) -> windowNode.addInput(process(i)));
    return windowNode;
  }

  private AbstractPlanNode visitValueNode(Plan.StageNode protoNode) {
    Plan.ValueNode protoSortNode = protoNode.getValueNode();
    List<List<RexExpression>> rows = new ArrayList<>();

    for (Plan.RexExpressionList row : protoSortNode.getRowsList()) {
      rows.add(row.getExpressionsList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList()));
    }

    ValueNode valueNode = new ValueNode(protoNode.getStageId(), extractDataSchema(protoNode), rows);
    protoNode.getInputsList().forEach((i) -> valueNode.addInput(process(i)));
    return valueNode;
  }

  private AbstractPlanNode visitProjectNode(Plan.StageNode protoNode) {
    Plan.ProjectNode protoProjectNode = protoNode.getProjectNode();

    List<RexExpression> projects =
        protoProjectNode.getProjectsList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());

    ProjectNode projectNode = new ProjectNode(protoNode.getStageId(), extractDataSchema(protoNode), projects);
    protoNode.getInputsList().forEach((i) -> projectNode.addInput(process(i)));
    return projectNode;
  }

  private AbstractPlanNode visitFilterNode(Plan.StageNode protoNode) {
    Plan.FilterNode protoFilterNode = protoNode.getFilterNode();

    RexExpression condition = ProtoExpressionVisitor.process(protoFilterNode.getCondition());

    FilterNode filterNode = new FilterNode(protoNode.getStageId(), extractDataSchema(protoNode), condition);
    protoNode.getInputsList().forEach((i) -> filterNode.addInput(process(i)));
    return filterNode;
  }

  private AbstractPlanNode visitAggregateNode(Plan.StageNode protoNode) {
    Plan.AggregateNode protoAggregateNode = protoNode.getAggregateNode();

    List<RexExpression> aggCalls =
        protoAggregateNode.getAggCallsList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    List<RexExpression> groupSet =
        protoAggregateNode.getGroupSetList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    AggregateNode aggregateNode = new AggregateNode(protoNode.getStageId(), extractDataSchema(protoNode), aggCalls,
        protoAggregateNode.getFilterArgIndicesList(), groupSet, extractNodeHint(protoAggregateNode.getNodeHint()),
        convertAggType(protoAggregateNode.getAggType()));
    protoNode.getInputsList().forEach((i) -> aggregateNode.addInput(process(i)));
    return aggregateNode;
  }

  private AbstractPlanNode visitJoinNode(Plan.StageNode protoNode) {
    Plan.JoinNode protoJoinNode = protoNode.getJoinNode();

    JoinNode.JoinKeys joinKeys = new JoinNode.JoinKeys(protoJoinNode.getJoinKeys().getLeftKeysList(),
        protoJoinNode.getJoinKeys().getRightKeysList());
    List<RexExpression> joinClauses =
        protoJoinNode.getJoinClauseList().stream().map(ProtoExpressionVisitor::process).collect(Collectors.toList());
    JoinNode joinNode =
        new JoinNode(protoNode.getStageId(), extractDataSchema(protoNode), protoJoinNode.getLeftColumnNamesList(),
            protoJoinNode.getRightColumnNamesList(), convertJoinRelType(protoJoinNode.getJoinRelType()), joinKeys,
            joinClauses, extractNodeHint(protoJoinNode.getJoinHints()));
    protoNode.getInputsList().forEach((i) -> joinNode.addInput(process(i)));
    return joinNode;
  }

  private static AbstractPlanNode.NodeHint extractNodeHint(Plan.NodeHint protoNodeHint) {
    AbstractPlanNode.NodeHint nodeHint = new AbstractPlanNode.NodeHint();
    protoNodeHint.getHintOptionsMap().forEach((key, value) -> nodeHint._hintOptions.put(key, value.getOptionsMap()));

    return nodeHint;
  }

  private static DataSchema extractDataSchema(Plan.StageNode protoNode) {
    String[] columnDataTypesList = protoNode.getColumnDataTypesList().toArray(new String[]{});
    String[] columnNames = protoNode.getColumnNamesList().toArray(new String[]{});
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      columnDataTypes[i] = DataSchema.ColumnDataType.valueOf(columnDataTypesList[i]);
    }
    return new DataSchema(columnNames, columnDataTypes);
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

  private static AbstractPlanNode newNodeInstance(String nodeName, int planFragmentId) {
    switch (nodeName) {
      case "JoinNode":
        return new JoinNode(planFragmentId);
      case "ProjectNode":
        return new ProjectNode(planFragmentId);
      case "FilterNode":
        return new FilterNode(planFragmentId);
      case "AggregateNode":
        return new AggregateNode(planFragmentId);
      case "SortNode":
        return new SortNode(planFragmentId);
      case "ValueNode":
        return new ValueNode(planFragmentId);
      case "WindowNode":
        return new WindowNode(planFragmentId);
      case "SetOpNode":
        return new SetOpNode(planFragmentId);
      case "ExchangeNode":
        throw new IllegalArgumentException(
            "ExchangeNode should be already split into MailboxSendNode and MailboxReceiveNode");
      default:
        throw new IllegalArgumentException("Unknown node name: " + nodeName);
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
