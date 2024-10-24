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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class PlanNodeDeserializer {
  private PlanNodeDeserializer() {
  }

  public static PlanNode process(Plan.PlanNode protoNode) {
    switch (protoNode.getNodeCase()) {
      case AGGREGATENODE:
        return deserializeAggregateNode(protoNode);
      case FILTERNODE:
        return deserializeFilterNode(protoNode);
      case JOINNODE:
        return deserializeJoinNode(protoNode);
      case MAILBOXRECEIVENODE:
        return deserializeMailboxReceiveNode(protoNode);
      case MAILBOXSENDNODE:
        return deserializeMailboxSendNode(protoNode);
      case PROJECTNODE:
        return deserializeProjectNode(protoNode);
      case SETOPNODE:
        return deserializeSetNode(protoNode);
      case SORTNODE:
        return deserializeSortNode(protoNode);
      case TABLESCANNODE:
        return deserializeTableScanNode(protoNode);
      case VALUENODE:
        return deserializeValueNode(protoNode);
      case WINDOWNODE:
        return deserializeWindowNode(protoNode);
      case EXPLAINNODE:
        return deserializeExplainedNode(protoNode);
      default:
        throw new IllegalStateException("Unsupported PlanNode type: " + protoNode.getNodeCase());
    }
  }

  private static AggregateNode deserializeAggregateNode(Plan.PlanNode protoNode) {
    Plan.AggregateNode protoAggregateNode = protoNode.getAggregateNode();
    return new AggregateNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertFunctionCalls(protoAggregateNode.getAggCallsList()),
        protoAggregateNode.getFilterArgsList(), protoAggregateNode.getGroupKeysList(),
        convertAggType(protoAggregateNode.getAggType()));
  }

  private static FilterNode deserializeFilterNode(Plan.PlanNode protoNode) {
    Plan.FilterNode protoFilterNode = protoNode.getFilterNode();
    return new FilterNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), ProtoExpressionToRexExpression.convertExpression(protoFilterNode.getCondition()));
  }

  private static JoinNode deserializeJoinNode(Plan.PlanNode protoNode) {
    Plan.JoinNode protoJoinNode = protoNode.getJoinNode();
    return new JoinNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertJoinType(protoJoinNode.getJoinType()), protoJoinNode.getLeftKeysList(),
        protoJoinNode.getRightKeysList(), convertExpressions(protoJoinNode.getNonEquiConditionsList()),
        convertJoinStrategy(protoJoinNode.getJoinStrategy()));
  }

  private static MailboxReceiveNode deserializeMailboxReceiveNode(Plan.PlanNode protoNode) {
    List<PlanNode> planNodes = extractInputs(protoNode);
    Preconditions.checkState(planNodes.isEmpty(), "MailboxReceiveNode should not have inputs but has: %s", planNodes);
    Plan.MailboxReceiveNode protoMailboxReceiveNode = protoNode.getMailboxReceiveNode();
    return new MailboxReceiveNode(protoNode.getStageId(), extractDataSchema(protoNode),
        protoMailboxReceiveNode.getSenderStageId(), convertExchangeType(protoMailboxReceiveNode.getExchangeType()),
        convertDistributionType(protoMailboxReceiveNode.getDistributionType()), protoMailboxReceiveNode.getKeysList(),
        convertCollations(protoMailboxReceiveNode.getCollationsList()), protoMailboxReceiveNode.getSort(),
        protoMailboxReceiveNode.getSortedOnSender(), null);
  }

  private static MailboxSendNode deserializeMailboxSendNode(Plan.PlanNode protoNode) {
    Plan.MailboxSendNode protoMailboxSendNode = protoNode.getMailboxSendNode();
    return new MailboxSendNode(protoNode.getStageId(), extractDataSchema(protoNode), extractInputs(protoNode),
        protoMailboxSendNode.getReceiverStageId(), convertExchangeType(protoMailboxSendNode.getExchangeType()),
        convertDistributionType(protoMailboxSendNode.getDistributionType()), protoMailboxSendNode.getKeysList(),
        protoMailboxSendNode.getPrePartitioned(), convertCollations(protoMailboxSendNode.getCollationsList()),
        protoMailboxSendNode.getSort());
  }

  private static ProjectNode deserializeProjectNode(Plan.PlanNode protoNode) {
    Plan.ProjectNode protoProjectNode = protoNode.getProjectNode();
    return new ProjectNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertExpressions(protoProjectNode.getProjectsList()));
  }

  private static SetOpNode deserializeSetNode(Plan.PlanNode protoNode) {
    Plan.SetOpNode protoSetOpNode = protoNode.getSetOpNode();
    return new SetOpNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertSetOpType(protoSetOpNode.getSetOpType()), protoSetOpNode.getAll());
  }

  private static SortNode deserializeSortNode(Plan.PlanNode protoNode) {
    Plan.SortNode protoSortNode = protoNode.getSortNode();
    return new SortNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertCollations(protoSortNode.getCollationsList()), protoSortNode.getFetch(),
        protoSortNode.getOffset());
  }

  private static TableScanNode deserializeTableScanNode(Plan.PlanNode protoNode) {
    Plan.TableScanNode protoTableScanNode = protoNode.getTableScanNode();
    return new TableScanNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), protoTableScanNode.getTableName(), protoTableScanNode.getColumnsList());
  }

  private static ValueNode deserializeValueNode(Plan.PlanNode protoNode) {
    Plan.ValueNode protoValueNode = protoNode.getValueNode();
    return new ValueNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), convertLiteralRows(protoValueNode.getLiteralRowsList()));
  }

  private static WindowNode deserializeWindowNode(Plan.PlanNode protoNode) {
    Plan.WindowNode protoWindowNode = protoNode.getWindowNode();
    return new WindowNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), protoWindowNode.getKeysList(), convertCollations(protoWindowNode.getCollationsList()),
        convertFunctionCalls(protoWindowNode.getAggCallsList()),
        convertWindowFrameType(protoWindowNode.getWindowFrameType()), protoWindowNode.getLowerBound(),
        protoWindowNode.getUpperBound(), convertLiterals(protoWindowNode.getConstantsList()));
  }

  private static ExplainedNode deserializeExplainedNode(Plan.PlanNode protoNode) {
    Plan.ExplainNode protoExplainNode = protoNode.getExplainNode();
    return new ExplainedNode(protoNode.getStageId(), extractDataSchema(protoNode), extractNodeHint(protoNode),
        extractInputs(protoNode), protoExplainNode.getTitle(), protoExplainNode.getAttributesMap());
  }

  private static DataSchema extractDataSchema(Plan.PlanNode protoNode) {
    Plan.DataSchema protoDataSchema = protoNode.getDataSchema();
    String[] columnNames = protoDataSchema.getColumnNamesList().toArray(new String[0]);
    int numColumns = columnNames.length;
    List<Expressions.ColumnDataType> protoColumnDataTypes = protoDataSchema.getColumnDataTypesList();
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnDataTypes[i] = ProtoExpressionToRexExpression.convertColumnDataType(protoColumnDataTypes.get(i));
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private static PlanNode.NodeHint extractNodeHint(Plan.PlanNode protoNode) {
    Plan.NodeHint protoNodeHint = protoNode.getNodeHint();
    Map<String, Plan.StrStrMap> hintOptionsMap = protoNodeHint.getHintOptionsMap();
    Map<String, Map<String, String>> hintOptions;
    int numHints = hintOptionsMap.size();
    if (numHints == 0) {
      hintOptions = Map.of();
    } else if (numHints == 1) {
      Map.Entry<String, Plan.StrStrMap> entry = hintOptionsMap.entrySet().iterator().next();
      hintOptions = Map.of(entry.getKey(), entry.getValue().getOptionsMap());
    } else {
      hintOptions = Maps.newHashMapWithExpectedSize(numHints);
      for (Map.Entry<String, Plan.StrStrMap> entry : hintOptionsMap.entrySet()) {
        hintOptions.put(entry.getKey(), entry.getValue().getOptionsMap());
      }
    }
    return new PlanNode.NodeHint(hintOptions);
  }

  private static List<PlanNode> extractInputs(Plan.PlanNode protoNode) {
    List<Plan.PlanNode> protoInputs = protoNode.getInputsList();
    int numInputs = protoInputs.size();
    if (numInputs == 0) {
      return List.of();
    }
    if (numInputs == 1) {
      return List.of(process(protoInputs.get(0)));
    }
    if (numInputs == 2) {
      return List.of(process(protoInputs.get(0)), process(protoInputs.get(1)));
    }
    List<PlanNode> inputs = new ArrayList<>(numInputs);
    for (Plan.PlanNode protoInput : protoInputs) {
      inputs.add(process(protoInput));
    }
    return inputs;
  }

  private static List<RexExpression> convertExpressions(List<Expressions.Expression> expressions) {
    List<RexExpression> rexExpressions = new ArrayList<>(expressions.size());
    for (Expressions.Expression expression : expressions) {
      rexExpressions.add(ProtoExpressionToRexExpression.convertExpression(expression));
    }
    return rexExpressions;
  }

  private static List<RexExpression.FunctionCall> convertFunctionCalls(List<Expressions.FunctionCall> functionCalls) {
    List<RexExpression.FunctionCall> rexFunctionCalls = new ArrayList<>(functionCalls.size());
    for (Expressions.FunctionCall functionCall : functionCalls) {
      rexFunctionCalls.add(ProtoExpressionToRexExpression.convertFunctionCall(functionCall));
    }
    return rexFunctionCalls;
  }

  private static List<RexExpression.Literal> convertLiterals(List<Expressions.Literal> literals) {
    List<RexExpression.Literal> rexLiterals = new ArrayList<>(literals.size());
    for (Expressions.Literal literal : literals) {
      rexLiterals.add(ProtoExpressionToRexExpression.convertLiteral(literal));
    }
    return rexLiterals;
  }

  private static AggregateNode.AggType convertAggType(Plan.AggType aggType) {
    switch (aggType) {
      case DIRECT:
        return AggregateNode.AggType.DIRECT;
      case LEAF:
        return AggregateNode.AggType.LEAF;
      case INTERMEDIATE:
        return AggregateNode.AggType.INTERMEDIATE;
      case FINAL:
        return AggregateNode.AggType.FINAL;
      default:
        throw new IllegalStateException("Unsupported AggType: " + aggType);
    }
  }

  private static JoinRelType convertJoinType(Plan.JoinType joinType) {
    switch (joinType) {
      case INNER:
        return JoinRelType.INNER;
      case LEFT:
        return JoinRelType.LEFT;
      case RIGHT:
        return JoinRelType.RIGHT;
      case FULL:
        return JoinRelType.FULL;
      case SEMI:
        return JoinRelType.SEMI;
      case ANTI:
        return JoinRelType.ANTI;
      default:
        throw new IllegalStateException("Unsupported JoinType: " + joinType);
    }
  }

  private static JoinNode.JoinStrategy convertJoinStrategy(Plan.JoinStrategy joinStrategy) {
    switch (joinStrategy) {
      case HASH:
        return JoinNode.JoinStrategy.HASH;
      case LOOKUP:
        return JoinNode.JoinStrategy.LOOKUP;
      default:
        throw new IllegalStateException("Unsupported JoinStrategy: " + joinStrategy);
    }
  }

  private static PinotRelExchangeType convertExchangeType(Plan.ExchangeType exchangeType) {
    switch (exchangeType) {
      case STREAMING:
        return PinotRelExchangeType.STREAMING;
      case SUB_PLAN:
        return PinotRelExchangeType.SUB_PLAN;
      case PIPELINE_BREAKER:
        return PinotRelExchangeType.PIPELINE_BREAKER;
      default:
        throw new IllegalStateException("Unsupported ExchangeType: " + exchangeType);
    }
  }

  private static RelDistribution.Type convertDistributionType(Plan.DistributionType distributionType) {
    switch (distributionType) {
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
        throw new IllegalStateException("Unsupported DistributionType: " + distributionType);
    }
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
        throw new IllegalStateException("Unsupported Direction: " + direction);
    }
  }

  private static RelFieldCollation.NullDirection convertNullDirection(Plan.NullDirection nullDirection) {
    switch (nullDirection) {
      case FIRST:
        return RelFieldCollation.NullDirection.FIRST;
      case LAST:
        return RelFieldCollation.NullDirection.LAST;
      case UNSPECIFIED:
        return RelFieldCollation.NullDirection.UNSPECIFIED;
      default:
        throw new IllegalStateException("Unsupported NullDirection: " + nullDirection);
    }
  }

  private static List<RelFieldCollation> convertCollations(List<Plan.Collation> collations) {
    List<RelFieldCollation> relCollations = new ArrayList<>(collations.size());
    for (Plan.Collation collation : collations) {
      relCollations.add(new RelFieldCollation(collation.getIndex(), convertDirection(collation.getDirection()),
          convertNullDirection(collation.getNullDirection())));
    }
    return relCollations;
  }

  private static SetOpNode.SetOpType convertSetOpType(Plan.SetOpType setOpType) {
    switch (setOpType) {
      case UNION:
        return SetOpNode.SetOpType.UNION;
      case INTERSECT:
        return SetOpNode.SetOpType.INTERSECT;
      case MINUS:
        return SetOpNode.SetOpType.MINUS;
      default:
        throw new IllegalStateException("Unsupported SetOpType: " + setOpType);
    }
  }

  private static List<RexExpression.Literal> convertLiteralRow(Plan.LiteralRow literalRow) {
    List<Expressions.Literal> values = literalRow.getValuesList();
    List<RexExpression.Literal> literals = new ArrayList<>(values.size());
    for (Expressions.Literal value : values) {
      literals.add(ProtoExpressionToRexExpression.convertLiteral(value));
    }
    return literals;
  }

  private static List<List<RexExpression.Literal>> convertLiteralRows(List<Plan.LiteralRow> literalRows) {
    List<List<RexExpression.Literal>> rows = new ArrayList<>(literalRows.size());
    for (Plan.LiteralRow literalRow : literalRows) {
      rows.add(convertLiteralRow(literalRow));
    }
    return rows;
  }

  private static WindowNode.WindowFrameType convertWindowFrameType(Plan.WindowFrameType windowFrameType) {
    switch (windowFrameType) {
      case ROWS:
        return WindowNode.WindowFrameType.ROWS;
      case RANGE:
        return WindowNode.WindowFrameType.RANGE;
      default:
        throw new IllegalStateException("Unsupported WindowFrameType: " + windowFrameType);
    }
  }
}
