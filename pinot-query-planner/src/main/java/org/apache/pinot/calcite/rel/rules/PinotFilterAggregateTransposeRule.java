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
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;


/// Same as [FilterAggregateTransposeRule], except that conjuncts referencing any group key containing `ARRAY_TO_MV`
/// stay above the aggregate while independent single-value group-key conjuncts can still be pushed down.
///
/// A condition on an `ARRAY_TO_MV` group key (whether bare `ARRAY_TO_MV(expr)` or nested like
/// `LOWER(ARRAY_TO_MV(expr))` or `CAST(ARRAY_TO_MV(expr) AS VARCHAR)`) below the aggregate becomes a multi-value
/// predicate in the leaf stage. That predicate keeps the whole row when any element matches, so the other elements of
/// the row also form groups. For example, `GROUP BY ARRAY_TO_MV(tags) HAVING ARRAY_TO_MV(tags) = 'vip'` would also
/// return the group of each other tag of a row that has `vip`. Keeping MV-key conjuncts above the aggregate ensures
/// they evaluate per group, while still allowing independent single-value conjuncts (such as `region = 'US'` in
/// `HAVING ARRAY_TO_MV(tags) = 'vip' AND region = 'US'`) to be pushed below the aggregate.
///
/// Sound positive leaf pre-filter copying for bare `ARRAY_TO_MV(col)` group keys is handled separately by
/// [PinotMvAggregatePreFilterRule].
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
    Filter filter = call.rel(0);
    Aggregate aggregate = call.rel(1);
    if (aggregate.getGroupCount() == 0) {
      return;
    }

    ImmutableBitSet mvGroupOutputCols = getMvGroupOutputCols(aggregate);
    if (mvGroupOutputCols.isEmpty()) {
      super.onMatch(call);
      return;
    }

    List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    List<RelDataTypeField> origFields = aggregate.getRowType().getFieldList();
    int[] adjustments = new int[origFields.size()];
    int j = 0;
    for (int i : aggregate.getGroupSet()) {
      adjustments[j] = i - j;
      j++;
    }
    RelOptUtil.RexInputConverter inputConverter = new RelOptUtil.RexInputConverter(rexBuilder, origFields,
        aggregate.getInput(0).getRowType().getFieldList(), adjustments);

    List<RexNode> pushedConditions = new ArrayList<>();
    List<RexNode> remainingConditions = new ArrayList<>();
    for (RexNode condition : conditions) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
      if (!rCols.intersects(mvGroupOutputCols) && canPushConjunct(aggregate, rCols)) {
        pushedConditions.add(condition.accept(inputConverter));
      } else {
        remainingConditions.add(condition);
      }
    }

    if (pushedConditions.isEmpty()) {
      return;
    }

    RelBuilder builder = call.builder();
    RelNode newInput = builder.push(aggregate.getInput()).filter(pushedConditions).build();
    if (newInput == aggregate.getInput(0)) {
      return;
    }
    RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(), List.of(newInput));
    RelNode result = builder.push(newAggregate).filter(remainingConditions).build();
    call.transformTo(result);
  }

  /// Returns the aggregate output column indexes whose group key contains `ARRAY_TO_MV` anywhere in its defining
  /// expression (bare or nested), walking through `Filter` and `Project` chains below `aggregate`.
  private static ImmutableBitSet getMvGroupOutputCols(Aggregate aggregate) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    List<Integer> groupColumns = aggregate.getGroupSet().asList();
    for (int outputCol = 0; outputCol < groupColumns.size(); outputCol++) {
      if (ArrayToMvGroupKeys.containsArrayToMvColumn(aggregate.getInput(), groupColumns.get(outputCol))) {
        builder.set(outputCol);
      }
    }
    return builder.build();
  }

  /// Returns whether a conjunct referencing aggregate output columns `rCols` can be pushed below `aggregate`.
  /// Mirrors Calcite's `FilterAggregateTransposeRule#canPush`, mapping output columns to input group columns when
  /// checking non-simple grouping sets.
  private static boolean canPushConjunct(Aggregate aggregate, ImmutableBitSet rCols) {
    ImmutableBitSet groupKeys = ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality());
    if (!groupKeys.contains(rCols)) {
      return false;
    }
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      List<Integer> groupColumns = aggregate.getGroupSet().asList();
      ImmutableBitSet.Builder inputColsBuilder = ImmutableBitSet.builder();
      for (int outputCol : rCols) {
        inputColsBuilder.set(groupColumns.get(outputCol));
      }
      ImmutableBitSet inputCols = inputColsBuilder.build();
      for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
        if (!groupingSet.contains(inputCols)) {
          return false;
        }
      }
    }
    return true;
  }
}
