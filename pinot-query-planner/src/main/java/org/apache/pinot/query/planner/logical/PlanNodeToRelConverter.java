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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.common.utils.DatabaseUtils;
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


/**
 * Converts a {@link PlanNode} into a {@link RelNode}.
 *
 * This class is used to convert serialized plan nodes into RelNodes so they can be used when explain with
 * implementation is requested. Therefore some nodes may be transformed in a way that loses information that is
 * required to create an actual executable plan but not necessary in order to describe the plan.
 */
public final class PlanNodeToRelConverter {

  private PlanNodeToRelConverter() {
  }

  public static RelNode convert(RelBuilder builder, PlanNode planNode) {
    // TODO: Merge nodes
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

      int[] groupKeyArr = node.getGroupKeys().stream().mapToInt(Integer::intValue).toArray();
      RelBuilder.GroupKey groupKey = _builder.groupKey(groupKeyArr);

      List<RelBuilder.AggCall> aggCalls = node.getAggCalls().stream()
          .map(functionCall -> RexExpressionUtils.toAggCall(_builder, functionCall))
          .collect(Collectors.toList());

      _builder.aggregate(groupKey, aggCalls);

      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Void context) {
      visitChildren(node);

      RexNode rexNode = RexExpressionUtils.toRexNode(_builder, node.getCondition());
      _builder.filter(rexNode);

      return null;
    }

    @Override
    public Void visitJoin(JoinNode node, Void context) {
      visitChildren(node);

      List<RexNode> conditions = new ArrayList<>(node.getLeftKeys().size() + node.getRightKeys().size()
          + node.getNonEquiConditions().size());
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

      return null;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, Void context) {
      // TODO: decide what to do in this case
      throw new UnsupportedOperationException("MailboxReceiveNode is not supported in RelNode");
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Void context) {
      // TODO: decide what to do in this case
      visitChildren(node);
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Void context) {
      visitChildren(node);
      List<RexNode> projects = node.getProjects().stream()
          .map(project -> RexExpressionUtils.toRexNode(_builder, project))
          .collect(Collectors.toList());
      _builder.project(projects);
      return null;
    }

    @Override
    public Void visitSort(SortNode node, Void context) {
      visitChildren(node);

      RelNode child = _builder.peek();
      RexLiteral offset = _builder.literal(node.getOffset());
      RexLiteral fetch = _builder.literal(node.getFetch());
      RelCollation relCollation = RelCollations.of(node.getCollations());
      LogicalSort logicalSort = LogicalSort.create(child, relCollation, offset, fetch);
      _builder.push(logicalSort);
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context) {
      visitChildren(node);
      _builder.scan(DatabaseUtils.splitTableName(node.getTableName()));
      return null;
    }

    @Override
    public Void visitValue(ValueNode node, Void context) {
      visitChildren(node);

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
      return null;
    }

    @Override
    public Void visitWindow(WindowNode node, Void context) {
      // TODO: Implement Window
      throw new UnsupportedOperationException("WindowNode is not supported in RelNode");
    }

    @Override
    public Void visitSetOp(SetOpNode setOpNode, Void context) {
      // TODO: Implement setOp
      throw new UnsupportedOperationException("SetOpNode is not supported in RelNode");
    }

    @Override
    public Void visitExplained(ExplainedNode node, Void context) {
      visitChildren(node);

      int size = node.getInputs().size();
      List<RelNode> children = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        children.add(_builder.peek());
      }

      RelOptCluster cluster = _builder.getCluster();
      RelTraitSet empty = RelTraitSet.createEmpty();
      PinotExplainedRelNode explainedNode = new PinotExplainedRelNode(
          cluster, empty, node.getType(), node.getAttributes(), node.getDataSchema(), children);
      _builder.push(explainedNode);
      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode exchangeNode, Void context) {
      visitChildren(exchangeNode);

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

      return null;
    }

    RelNode build() {
      return _builder.build();
    }
  }
}
