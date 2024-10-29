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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.plan.PinotExplainedRelNode;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Converts a {@link PlanNode} into a {@link RelNode}.
 *
 * This class is used to convert serialized plan nodes into RelNodes so they can be used when explain with
 * implementation is requested. Therefore some nodes may be transformed in a way that loses information that is
 * required to create an actual executable plan but not necessary in order to describe the plan.
 */
public final class PlanNodeToRelConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanNodeToRelConverter.class);

  private PlanNodeToRelConverter() {
  }

  public static RelNode convert(RelBuilder builder, PlanNode planNode) {
    ConverterVisitor visitor = new ConverterVisitor(builder);
    planNode.visit(visitor, null);

    return visitor.build();
  }

  private static class ConverterVisitor implements PlanNodeVisitor<Void, Void> {
    private final RelBuilder _builder;

    public ConverterVisitor(RelBuilder builder) {
      _builder = builder;
    }

    private void visitChildren(PlanNode node) {
      node.getInputs().forEach(input -> input.visit(this, null));
    }

    @Override
    public Void visitAggregate(AggregateNode node, Void context) {
      visitChildren(node);

      try {
        int[] groupKeyArr = node.getGroupKeys().stream().mapToInt(Integer::intValue).toArray();
        RelBuilder.GroupKey groupKey = _builder.groupKey(groupKeyArr);

        List<RelBuilder.AggCall> aggCalls =
            node.getAggCalls().stream().map(functionCall -> RexExpressionUtils.toAggCall(_builder, functionCall))
                .collect(Collectors.toList());

        _builder.aggregate(groupKey, aggCalls);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert aggregate node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownAggregate", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }

      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Void context) {
      visitChildren(node);

      try {
        RexNode rexNode = RexExpressionUtils.toRexNode(_builder, node.getCondition());
        _builder.filter(rexNode);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert filter node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownFilter", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }

      return null;
    }

    @Override
    public Void visitJoin(JoinNode node, Void context) {
      visitChildren(node);

      try {
        List<RexNode> conditions = new ArrayList<>(
            node.getLeftKeys().size() + node.getRightKeys().size() + node.getNonEquiConditions().size());
        for (Integer leftKey : node.getLeftKeys()) {
          conditions.add(_builder.field(2, 0, leftKey));
        }
        for (Integer rightKey : node.getRightKeys()) {
          conditions.add(_builder.field(2, 1, rightKey));
        }
        for (RexExpression nonEquiCondition : node.getNonEquiConditions()) {
          conditions.add(RexExpressionUtils.toRexNode(_builder, nonEquiCondition));
        }

        _builder.join(node.getJoinType(), conditions);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert join node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownJoin", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }

      return null;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, Void context) {
      visitChildren(node);

      ExplainAttributeBuilder attributes = new ExplainAttributeBuilder();
      if (node.isSortedOnSender()) {
        attributes.putBool("sortedOnSender", true);
      }

      List<RelNode> inputs = readAlreadyPushedChildren(node);

      PinotExplainedRelNode explained = new PinotExplainedRelNode(_builder.getCluster(), "MailboxReceive",
          attributes.build(), node.getDataSchema(), inputs);
      _builder.push(explained);
      return null;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Void context) {
      visitChildren(node);

      ExplainAttributeBuilder attributes = new ExplainAttributeBuilder();

      RelDistribution relDistribution;
      switch (node.getDistributionType()) {
        case HASH_DISTRIBUTED:
          relDistribution = RelDistributions.hash(node.getKeys());
          break;
        case RANGE_DISTRIBUTED:
          relDistribution = RelDistributions.range(node.getKeys());
          break;
        case SINGLETON:
          relDistribution = RelDistributions.SINGLETON;
          break;
        case ANY:
          relDistribution = RelDistributions.ANY;
          break;
        case BROADCAST_DISTRIBUTED:
          relDistribution = RelDistributions.BROADCAST_DISTRIBUTED;
          break;
        case ROUND_ROBIN_DISTRIBUTED:
          relDistribution = RelDistributions.ROUND_ROBIN_DISTRIBUTED;
          break;
        case RANDOM_DISTRIBUTED:
          relDistribution = RelDistributions.RANDOM_DISTRIBUTED;
          break;
        default:
          LOGGER.info("Unsupported distribution type: {}", node.getDistributionType());
          relDistribution = RelDistributions.ANY;
          break;
      }
      attributes.putString("distribution", relDistribution.toString());
      if (node.isPrePartitioned()) {
        attributes.putBool("prePartitioned", true);
      }

      List<RelNode> inputs = readAlreadyPushedChildren(node);

      String type = node.isSort() ? "PinotLogicalSortExchange" : "PinotLogicalExchange";
      PinotExplainedRelNode explained = new PinotExplainedRelNode(_builder.getCluster(), type, attributes.build(),
          node.getDataSchema(), inputs);
      _builder.push(explained);
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Void context) {
      visitChildren(node);

      try {
        List<RexNode> projects =
            node.getProjects().stream().map(project -> RexExpressionUtils.toRexNode(_builder, project))
                .collect(Collectors.toList());
        _builder.project(projects);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert project node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownProject", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }
      return null;
    }

    @Override
    public Void visitSort(SortNode node, Void context) {
      visitChildren(node);

      try {
        RelNode child = _builder.build();
        RexLiteral offset = _builder.literal(node.getOffset());
        RexLiteral fetch = _builder.literal(node.getFetch());
        RelCollation relCollation = RelCollations.of(node.getCollations());
        LogicalSort logicalSort = LogicalSort.create(child, relCollation, offset, fetch);
        _builder.push(logicalSort);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert sort node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownSort", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context) {
      visitChildren(node);

      try {
        _builder.scan(DatabaseUtils.splitTableName(node.getTableName()));
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert table scan node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownTableScan", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }
      return null;
    }

    @Override
    public Void visitValue(ValueNode node, Void context) {
      visitChildren(node);

      try {
        List<List<RexLiteral>> values = new ArrayList<>(node.getLiteralRows().size());
        for (List<RexExpression.Literal> literalRow : node.getLiteralRows()) {
          List<RexLiteral> rexRow = new ArrayList<>(literalRow.size());
          for (RexExpression.Literal literal : literalRow) {
            rexRow.add(RexExpressionUtils.toRexLiteral(_builder, literal));
          }
          values.add(rexRow);
        }

        RelDataType relDataType = node.getDataSchema().toRelDataType(_builder.getTypeFactory());
        _builder.values(values, relDataType);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert value node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownValue", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }
      return null;
    }

    @Override
    public Void visitWindow(WindowNode node, Void context) {
      try {
        Preconditions.checkArgument(node.getInputs().size() == 1, "Window node should have exactly one input");
        node.getInputs().get(0).visit(this, null);
        RelNode input = _builder.build();

        ImmutableBitSet keys = ImmutableBitSet.of(node.getKeys());
        boolean isRow = node.getWindowFrameType() == WindowNode.WindowFrameType.ROWS;
        RelCollation orderKeys = RelCollations.of(node.getCollations());

        List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
        for (RexExpression.FunctionCall funCall : node.getAggCalls()) {
          SqlAggFunction aggFunction = RexExpressionUtils.getAggFunction(funCall, _builder.getCluster());
          List<RexExpression> functionOperands = funCall.getFunctionOperands();
          List<RexNode> operands = new ArrayList<>(functionOperands.size());
          for (RexExpression functionOperand : functionOperands) {
            operands.add(RexExpressionUtils.toRexNode(_builder, functionOperand));
          }
          RelDataType relDataType = funCall.getDataType().toType(_builder.getTypeFactory());
          Window.RexWinAggCall winCall = new Window.RexWinAggCall(aggFunction, relDataType, operands, aggCalls.size(),
              // same as the one used in LogicalWindow.create
              funCall.isDistinct(), funCall.isIgnoreNulls());
          aggCalls.add(winCall);
        }

        Window.Group group =
            new Window.Group(keys, isRow, getWindowBound(node.getLowerBound()), getWindowBound(node.getUpperBound()),
                orderKeys, aggCalls);

        List<RexLiteral> constants =
            node.getConstants().stream().map(constant -> RexExpressionUtils.toRexLiteral(_builder, constant))
                .collect(Collectors.toList());
        RelDataType rowType = node.getDataSchema().toRelDataType(_builder.getTypeFactory());
        ;

        LogicalWindow window = LogicalWindow.create(RelTraitSet.createEmpty(), input, constants, rowType,
            Collections.singletonList(group));
        _builder.push(window);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert window node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownWindow", Collections.emptyMap(),
            node.getDataSchema(), readAlreadyPushedChildren(node)));
      }
      return null;
    }

    private RexWindowBound getWindowBound(int bound) {
      if (bound == Integer.MIN_VALUE) {
        return RexWindowBounds.UNBOUNDED_PRECEDING;
      } else if (bound == Integer.MAX_VALUE) {
        return RexWindowBounds.UNBOUNDED_FOLLOWING;
      } else if (bound == 0) {
        return RexWindowBounds.CURRENT_ROW;
      } else if (bound < 0) {
        return RexWindowBounds.preceding(_builder.literal(-bound));
      } else {
        return RexWindowBounds.following(_builder.literal(bound));
      }
    }

    @Override
    public Void visitSetOp(SetOpNode node, Void context) {
      List<RelNode> inputs = inputsAsList(node);

      try {
        SetOp setOp;
        switch (node.getSetOpType()) {
          case INTERSECT:
            setOp = new LogicalIntersect(_builder.getCluster(), RelTraitSet.createEmpty(), inputs, node.isAll());
            break;
          case MINUS:
            setOp = new LogicalMinus(_builder.getCluster(), RelTraitSet.createEmpty(), inputs, node.isAll());
            break;
          case UNION:
            setOp = new LogicalUnion(_builder.getCluster(), RelTraitSet.createEmpty(), inputs, node.isAll());
            break;
          default:
            throw new UnsupportedOperationException("Unsupported set op node: " + node.getSetOpType());
        }
        _builder.push(setOp);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert set op node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownSetOp", Collections.emptyMap(),
            node.getDataSchema(), inputs));
      }
      return null;
    }

    @Override
    public Void visitExplained(ExplainedNode node, Void context) {
      List<RelNode> inputs = inputsAsList(node);

      try {
        RelOptCluster cluster = _builder.getCluster();
        RelTraitSet empty = RelTraitSet.createEmpty();
        PinotExplainedRelNode explainedNode = new PinotExplainedRelNode(cluster, empty, node.getTitle(),
            node.getAttributes(), node.getDataSchema(), inputs);
        _builder.push(explainedNode);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert explained node: {}", node, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownExplained", Collections.emptyMap(),
            node.getDataSchema(), inputs));
      }
      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode exchangeNode, Void context) {
      visitChildren(exchangeNode);

      try {
        RelDistribution distribution;
        switch (exchangeNode.getDistributionType()) {
          case HASH_DISTRIBUTED: {
            List<Integer> keys = exchangeNode.getKeys();
            assert keys != null;
            distribution = RelDistributions.hash(keys);
            break;
          }
          case ANY:
            distribution = RelDistributions.ANY;
            break;
          case RANDOM_DISTRIBUTED:
            distribution = RelDistributions.RANDOM_DISTRIBUTED;
            break;
          case SINGLETON:
            distribution = RelDistributions.SINGLETON;
            break;
          case BROADCAST_DISTRIBUTED:
            distribution = RelDistributions.BROADCAST_DISTRIBUTED;
            break;
          case ROUND_ROBIN_DISTRIBUTED:
            distribution = RelDistributions.ROUND_ROBIN_DISTRIBUTED;
            break;
          case RANGE_DISTRIBUTED: {
            List<Integer> keys = exchangeNode.getKeys();
            assert keys != null;
            distribution = RelDistributions.range(keys);
            break;
          }
          default:
            throw new IllegalStateException("Unsupported distribution type: " + exchangeNode.getDistributionType());
        }
        _builder.exchange(distribution);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to convert exchange node: {}", exchangeNode, e);
        _builder.push(new PinotExplainedRelNode(_builder.getCluster(), "UnknownExchange", Collections.emptyMap(),
            exchangeNode.getDataSchema(), readAlreadyPushedChildren(exchangeNode)));
      }

      return null;
    }

    RelNode build() {
      return _builder.build();
    }

    private List<RelNode> inputsAsList(PlanNode node) {
      visitChildren(node);
      return readAlreadyPushedChildren(node);
    }

    private List<RelNode> readAlreadyPushedChildren(PlanNode node) {
      int inputSize = node.getInputs().size();
      List<RelNode> inputs = new ArrayList<>(inputSize);
      for (int i = 0; i < inputSize; i++) {
        inputs.add(null);
      }
      for (int size = inputSize - 1; size >= 0; size--) {
        inputs.set(size, _builder.build());
      }
      return inputs;
    }
  }
}
