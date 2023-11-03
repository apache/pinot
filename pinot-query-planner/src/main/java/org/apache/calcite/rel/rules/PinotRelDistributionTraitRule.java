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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Special rule for Pinot, this rule populates {@link RelDistribution} across the entire relational tree.
 *
 * we implement this rule as a workaround b/c {@link org.apache.calcite.plan.RelTraitPropagationVisitor}, which is
 * deprecated. The idea is to associate every node with a RelDistribution derived from {@link RelNode#getInputs()}
 * or from the node itself (via hints, or special handling of the type of node in question).
 */
public class PinotRelDistributionTraitRule extends RelOptRule {
  public static final PinotRelDistributionTraitRule INSTANCE =
      new PinotRelDistributionTraitRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotRelDistributionTraitRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rels.length >= 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode current = call.rel(0);
    List<RelNode> inputs = current.getInputs();
    RelDistribution relDistribution;

    if (inputs == null || inputs.size() == 0) {
      relDistribution = computeCurrentDistribution(current);
    } else {
      // if there's input to the current node, attempt to derive the RelDistribution.
      relDistribution = deriveDistribution(current);
    }
    call.transformTo(attachTrait(current, relDistribution));
  }

  /**
   * currently, Pinot has {@link RelTraitSet} default set to empty and thus we directly pull the cluster trait set,
   * then plus the {@link RelDistribution} trait.
   */
  private static RelNode attachTrait(RelNode relNode, RelTrait trait) {
    RelTraitSet clusterTraitSet = relNode.getCluster().traitSet();
    if (relNode instanceof LogicalJoin) {
      // work around {@link LogicalJoin#copy(RelTraitSet, RexNode, RelNode, RelNode, JoinRelType, boolean)} not copying
      // properly
      LogicalJoin join = (LogicalJoin) relNode;
      return new LogicalJoin(join.getCluster(), clusterTraitSet.plus(trait), join.getLeft(),
          join.getRight(), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));
    } else if (relNode instanceof LogicalTableScan) {
      LogicalTableScan tableScan = (LogicalTableScan) relNode;
      return new LogicalTableScan(tableScan.getCluster(), clusterTraitSet.plus(trait), tableScan.getTable());
    } else {
      return relNode.copy(clusterTraitSet.plus(trait), relNode.getInputs());
    }
  }

  private static RelDistribution deriveDistribution(RelNode node) {
    List<RelNode> inputs = node.getInputs();
    RelNode input = PinotRuleUtils.unboxRel(inputs.get(0));
    if (node instanceof PinotLogicalExchange) {
      // TODO: derive from input first, only if the result is ANY we change it to current
      return computeCurrentDistribution(node);
    } else if (node instanceof LogicalProject) {
      assert inputs.size() == 1;
      @Nullable RelDistribution inputRelDistribution = input.getTraitSet().getDistribution();
      LogicalProject project = (LogicalProject) node;
      try {
        if (inputRelDistribution != null) {
          return inputRelDistribution.apply(project.getMapping());
        }
      } catch (Exception e) {
        // ... skip;
      }
    } else if (node instanceof LogicalFilter) {
      assert inputs.size() == 1;
      @Nullable RelDistribution inputRelDistribution = input.getTraitSet().getDistribution();
      if (inputRelDistribution != null) {
        return inputRelDistribution;
      }
    } else if (node instanceof LogicalAggregate) {
      assert inputs.size() == 1;
      @Nullable RelDistribution inputRelDistribution = inputs.get(0).getTraitSet().getDistribution();
      if (inputRelDistribution != null) {
        // create a mapping that only contains the group set
        LogicalAggregate agg = (LogicalAggregate) node;
        List<Integer> groupSetIndices = new ArrayList<>();
        agg.getGroupSet().forEach(groupSetIndices::add);
        return inputRelDistribution.apply(Mappings.target(groupSetIndices, input.getRowType().getFieldCount()));
      }
    } else if (node instanceof LogicalJoin) {
      // TODO: we only map a single RelTrait from the LEFT table, later we should support RIGHT table as well
      assert inputs.size() == 2;
      @Nullable RelDistribution inputRelDistribution = inputs.get(0).getTraitSet().getDistribution();
      if (inputRelDistribution != null) {
        // Since we only support LEFT RelTrait propagation, the inputRelDistribution can directly be applied
        // b/c the Join node always puts left relation RowTypes then right relation RowTypes sequentially.
        return inputRelDistribution;
      }
    }
    // TODO: add the rest of the nodes.
    return computeCurrentDistribution(node);
  }

  private static RelDistribution computeCurrentDistribution(RelNode node) {
    if (node instanceof Exchange) {
      return ((Exchange) node).getDistribution();
    } else if (node instanceof LogicalTableScan) {
      LogicalTableScan tableScan = (LogicalTableScan) node;
      // convert table scan hints into rel trait
      String partitionKey =
          PinotHintStrategyTable.getHintOption(tableScan.getHints(), PinotHintOptions.TABLE_HINT_OPTIONS,
              PinotHintOptions.TableHintOptions.PARTITION_KEY);
      if (partitionKey != null) {
        int partitionIndex = tableScan.getRowType().getField(partitionKey, true, true).getIndex();
        return RelDistributions.hash(ImmutableList.of(partitionIndex));
      } else {
        return RelDistributions.of(RelDistribution.Type.RANDOM_DISTRIBUTED, RelDistributions.EMPTY);
      }
    } else if (node instanceof LogicalAggregate) {
      LogicalAggregate agg = (LogicalAggregate) node;
      AggregateNode.AggType aggType = AggregateNode.AggType.valueOf(PinotHintStrategyTable.getHintOption(agg.getHints(),
          PinotHintOptions.INTERNAL_AGG_OPTIONS, PinotHintOptions.InternalAggregateOptions.AGG_TYPE));
      if (aggType == AggregateNode.AggType.FINAL || aggType == AggregateNode.AggType.DIRECT) {
        List<Integer> groupSetIndices = new ArrayList<>();
        agg.getGroupSet().forEach(groupSetIndices::add);
        return RelDistributions.hash(groupSetIndices);
      } else {
        return RelDistributions.of(RelDistribution.Type.RANDOM_DISTRIBUTED, RelDistributions.EMPTY);
      }
    }
    return RelDistributions.of(RelDistribution.Type.RANDOM_DISTRIBUTED, RelDistributions.EMPTY);
  }
}
