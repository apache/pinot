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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;


/// Same as [FilterAggregateTransposeRule], except for an aggregate that groups by `ARRAY_TO_MV(expr)` (see
/// [ArrayToMvGroupKeys#detect]).
///
/// For such an aggregate, a condition on an MV key stays above the aggregate. Below it, the condition becomes a
/// multi-value predicate in the leaf stage. That predicate keeps the whole row when any element matches, so the other
/// elements of the row also get groups. For example, `GROUP BY ARRAY_TO_MV(tags) HAVING ARRAY_TO_MV(tags) = 'vip'`
/// would also return the group of each other tag of a row that has `vip`.
///
/// The conditions on the other group keys go below the aggregate, into the `Project` that computes the keys, as with
/// [FilterAggregateTransposeRule]. The aggregate keeps the `Project` as its input, so that the MV aggregate rule still
/// detects the MV keys.
///
/// The rule is stateless and thread-safe.
public class PinotFilterAggregateTransposeRule extends FilterAggregateTransposeRule {

  public static PinotFilterAggregateTransposeRule instanceWithDescription(String description) {
    return new PinotFilterAggregateTransposeRule((Config) Config.DEFAULT.withDescription(description));
  }

  private PinotFilterAggregateTransposeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(1);
    List<Integer> mvKeys = ArrayToMvGroupKeys.detect(aggregate);
    if (mvKeys.isEmpty()) {
      super.onMatch(call);
      return;
    }
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      return;
    }
    Filter filter = call.rel(0);
    // ArrayToMvGroupKeys.detect only returns keys when the input is a Project
    Project project = (Project) PinotRuleUtils.unboxRel(aggregate.getInput());
    // The group keys are the first output columns of the aggregate, and mvKeys are positions among them
    int groupCount = aggregate.getGroupCount();
    ImmutableBitSet mvColumns = ImmutableBitSet.of(mvKeys);
    List<Integer> groupColumns = aggregate.getGroupSet().asList();
    int[] adjustments = new int[aggregate.getRowType().getFieldCount()];
    for (int i = 0; i < groupCount; i++) {
      adjustments[i] = groupColumns.get(i) - i;
    }
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

    List<RexNode> pushed = new ArrayList<>();
    List<RexNode> kept = new ArrayList<>();
    for (RexNode condition : RelOptUtil.conjunctions(filter.getCondition())) {
      ImmutableBitSet columns = RelOptUtil.InputFinder.bits(condition);
      boolean onSvKeysOnly = columns.nextSetBit(groupCount) < 0 && !columns.intersects(mvColumns);
      if (!onSvKeysOnly || !RexUtil.isDeterministic(condition)) {
        kept.add(condition);
        continue;
      }
      // Aggregate output column i is the Project column groupColumns.get(i): rewrite the condition on the Project
      // columns, then on the Project input.
      RexNode onProject = condition.accept(new RelOptUtil.RexInputConverter(rexBuilder,
          aggregate.getRowType().getFieldList(), project.getRowType().getFieldList(), adjustments));
      RexNode onProjectInput = RelOptUtil.pushPastProject(onProject, project);
      if (PinotProjectJoinTransposeRule.containsArrayToMv(onProjectInput)) {
        kept.add(condition);
      } else {
        pushed.add(onProjectInput);
      }
    }
    if (pushed.isEmpty()) {
      return;
    }
    RelBuilder relBuilder = call.builder();
    RelNode newProjectInput = relBuilder.push(project.getInput()).filter(pushed).build();
    Project newProject = project.copy(project.getTraitSet(), newProjectInput, project.getProjects(),
        project.getRowType());
    RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(), List.of(newProject));
    call.transformTo(relBuilder.push(newAggregate).filter(kept).build());
  }
}
