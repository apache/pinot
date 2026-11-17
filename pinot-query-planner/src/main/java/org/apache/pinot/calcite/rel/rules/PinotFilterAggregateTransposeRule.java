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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.calcite.rex.PinotSealedSearchOperator;


/// Same as [FilterAggregateTransposeRule], except for an aggregate that groups by `ARRAY_TO_MV(expr)` (see
/// [ArrayToMvGroupKeys#detect]).
///
/// For such an aggregate, a condition on an MV key stays above the aggregate. Below it, the condition becomes a
/// multi-value predicate in the leaf stage. That predicate keeps the whole row when any element matches, so the other
/// elements of the row also get groups. For example, `GROUP BY ARRAY_TO_MV(tags) HAVING ARRAY_TO_MV(tags) = 'vip'`
/// would also return the group of each other tag of a row that has `vip`.
///
/// A positive condition on one MV key is also copied below the aggregate when the `Project` reads directly from a
/// table scan. A positive condition is `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN` or a single range against literals.
/// The copy keeps only the rows that have a matching element, which are the only rows that contribute to a matching
/// group, so the leaf stage aggregates fewer rows. The exact condition stays above the aggregate. A negated condition
/// is not copied: as a multi-value predicate, `tags <> 'vip'` and `tags NOT IN (...)` match a row only when no element
/// matches. Without a table scan under the `Project`, the copy can end up in an intermediate stage, where
/// `ARRAY_TO_MV` is not supported. When a filter under the `Project` already has an `ARRAY_TO_MV` call, no condition
/// is copied, so that the rule does not copy the same condition again on its next match.
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

    boolean canCopyBelow = canCopyBelow(project);
    List<RexNode> pushed = new ArrayList<>();
    List<RexNode> kept = new ArrayList<>();
    for (RexNode condition : RelOptUtil.conjunctions(filter.getCondition())) {
      ImmutableBitSet columns = RelOptUtil.InputFinder.bits(condition);
      boolean onSvKeysOnly = columns.nextSetBit(groupCount) < 0 && !columns.intersects(mvColumns);
      boolean copyBelow = canCopyBelow && isPositiveOnMvKey(condition, mvColumns);
      if (!(onSvKeysOnly || copyBelow) || !RexUtil.isDeterministic(condition)) {
        kept.add(condition);
        continue;
      }
      // Aggregate output column i is the Project column groupColumns.get(i): rewrite the condition on the Project
      // columns, then on the Project input.
      RexNode onProject = condition.accept(new RelOptUtil.RexInputConverter(rexBuilder,
          aggregate.getRowType().getFieldList(), project.getRowType().getFieldList(), adjustments));
      RexNode onProjectInput = RelOptUtil.pushPastProject(onProject, project);
      if (copyBelow) {
        pushed.add(onProjectInput);
        kept.add(condition);
      } else if (PinotProjectJoinTransposeRule.containsArrayToMv(onProjectInput)) {
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

  /// Returns whether the input of `project`, through filters and projects, is a table scan, so that a filter put on
  /// the input runs in the leaf stage, and none of these filters has an `ARRAY_TO_MV` call.
  private static boolean canCopyBelow(Project project) {
    RelNode node = PinotRuleUtils.unboxRel(project.getInput());
    while (node instanceof Filter || node instanceof Project) {
      if (node instanceof Filter
          && PinotProjectJoinTransposeRule.containsArrayToMv(((Filter) node).getCondition())) {
        return false;
      }
      node = PinotRuleUtils.unboxRel(node.getInput(0));
    }
    return node instanceof TableScan;
  }

  /// Returns whether `condition` compares one MV key column with literals in a way that a row matches only when one
  /// of its elements matches: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, or a `SEARCH` over points or a single range
  /// that does not match NULL.
  private static boolean isPositiveOnMvKey(RexNode condition, ImmutableBitSet mvColumns) {
    if (!(condition instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) condition;
    List<RexNode> operands = call.getOperands();
    switch (call.getKind()) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return isMvKeyRef(operands.get(0), mvColumns) && isNonNullLiteral(operands.get(1))
            || isMvKeyRef(operands.get(1), mvColumns) && isNonNullLiteral(operands.get(0));
      case BETWEEN:
        return !((SqlBetweenOperator) call.getOperator()).isNegated() && isMvKeyRef(operands.get(0), mvColumns)
            && isNonNullLiteral(operands.get(1)) && isNonNullLiteral(operands.get(2));
      case SEARCH:
        return isMvKeyRef(operands.get(0), mvColumns) && isPositiveSarg((RexLiteral) operands.get(1));
      default:
        return call.getOperator() instanceof PinotSealedSearchOperator && isMvKeyRef(operands.get(0), mvColumns)
            && isPositiveSarg(((PinotSealedSearchOperator) call.getOperator()).getSargLiteral());
    }
  }

  private static boolean isMvKeyRef(RexNode node, ImmutableBitSet mvColumns) {
    return node instanceof RexInputRef && mvColumns.get(((RexInputRef) node).getIndex());
  }

  private static boolean isNonNullLiteral(RexNode node) {
    return node instanceof RexLiteral && !((RexLiteral) node).isNull();
  }

  /// Points become `IN` and a single range becomes range predicates. Other range sets can become `NOT_IN`, see
  /// `RexExpressionUtils`.
  private static boolean isPositiveSarg(RexLiteral sargLiteral) {
    Sarg<?> sarg = sargLiteral.getValueAs(Sarg.class);
    return sarg != null && sarg.nullAs != RexUnknownAs.TRUE
        && (sarg.isPoints() || sarg.rangeSet.asRanges().size() == 1);
  }
}
