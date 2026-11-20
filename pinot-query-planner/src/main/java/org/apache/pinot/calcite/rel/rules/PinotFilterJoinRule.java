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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.checkerframework.checker.nullness.qual.Nullable;


/// Similar to [FilterJoinRule] but do not push down filter into right side of lookup join.
public abstract class PinotFilterJoinRule<C extends FilterJoinRule.Config> extends FilterJoinRule<C> {

  private PinotFilterJoinRule(C config) {
    super(config);
  }

  // Following code are copy-pasted from Calcite, and modified to not push down filter into right side of lookup join.
  // SYNCED WITH Calcite 1.42.0 FilterJoinRule#perform -- re-diff this method body against upstream on every
  // calcite.version bump. The intended deviations are the canPushRight lookup-join restriction and the volatility half
  // of the isRelocatable guard, both marked PINOT MODIFICATION below.
  //
  // Known outstanding drift: upstream's RexUtil.containsCorrelation partitioning of aboveFilters and the variablesSet
  // argument on the final RelBuilder#filter (CALCITE-7319) are not ported. Pinot decorrelates in
  // QueryEnvironment#toRelation before these rules run, and the LogicalCorrelate shapes that do survive it
  // (UNNEST / CROSS JOIN UNNEST) have an Uncollect right input, so a Filter carrying $cor never sits directly above a
  // Join here. Revisit if Pinot ever retains a Correlate over a Join.
  //@formatter:off
  @Override
  protected void perform(RelOptRuleCall call, @Nullable Filter filter, Join join) {
    // From CALCITE-7373: a non-deterministic conjunct such as rand() < 0.1 has an empty input bitmap, so
    // classifyFilters would treat it as pushable and relocate it below the join, evaluating it per input row instead
    // of per join-output row. Like upstream, this bails on the whole condition rather than per conjunct, so a
    // deterministic conjunct sharing the WHERE clause also stays above the join.
    // PINOT MODIFICATION to also skip volatile conditions. Upstream uses RexUtil.isDeterministic, which only covers
    // @ScalarFunction(isDeterministic = false). Pinot's separate FunctionVolatility.VOLATILE axis (now(), ago(),
    // stageId(), ...) keeps isDeterministic() == true so PinotEvaluateLiteralRule can still fold it once at plan time,
    // so it needs the wider PinotRuleUtils.isRelocatable check here.
    // Skip non-deterministic or volatile filter condition
    if (filter != null && !PinotRuleUtils.isRelocatable(filter.getCondition())) {
      return;
    }
    // Skip non-deterministic or volatile join condition.
    // NOTE: for an INNER join this guard is largely moot for anything referencing a single side, because
    // RelOptUtil.pushDownJoinConditions already hoisted such a call into that input's Project during sql-to-rel,
    // before any rule ran; the condition seen here is then a bare RexInputRef. It is still load-bearing for outer
    // joins, where the ON clause is preserved. See JoinPlans.json for both shapes.
    if (!PinotRuleUtils.isRelocatable(join.getCondition())) {
      return;
    }

    List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());
    final List<RexNode> origJoinFilters = List.copyOf(joinFilters);

    // If there is only the joinRel,
    // make sure it does not match a cartesian product joinRel
    // (with "true" condition), otherwise this rule will be applied
    // again on the new cartesian product joinRel.
    if (filter == null && joinFilters.isEmpty()) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? getConjunctions(filter)
            : new ArrayList<>();
    final List<RexNode> origAboveFilters =
        List.copyOf(aboveFilters);

    // Simplify Outer Joins
    JoinRelType joinType = join.getJoinType();
    if (config.isSmart()
        && !origAboveFilters.isEmpty()
        && join.getJoinType() != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(join,
          com.google.common.collect.ImmutableList.copyOf(origAboveFilters), joinType);
    }

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // PINOT MODIFICATION to not push down filter into right side of lookup join.
    boolean canPushRight = !PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join);

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed =
        RelOptUtil.classifyFilters(join,
            aboveFilters,
            joinType.canPushIntoFromAbove(),
            joinType.canPushLeftFromAbove(),
            canPushRight && joinType.canPushRightFromAbove(),
            joinFilters,
            leftFilters,
            rightFilters);

    // Move join filters up if needed
    validateJoinFilters(aboveFilters, joinFilters, join, joinType);

    // If no filter got pushed after validate, reset filterPushed flag
    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinFilters.size() == origJoinFilters.size()
        && aboveFilters.size() == origAboveFilters.size()) {
      if (Sets.newHashSet(joinFilters)
          .equals(Sets.newHashSet(origJoinFilters))) {
        filterPushed = false;
      }
    }

    if (joinType != JoinRelType.FULL) {
      joinFilters = inferJoinEqualConditions(joinFilters, join);
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.

    // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
    //
    //     Join(condition=[AND(cond1, $2)], joinType=[anti])
    //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
    //     :  - prj(f0=[$0])
    //
    // The semantic would change if join condition $2 is pushed into left,
    // that is, the result set may be smaller. The right can not be pushed
    // into for the same reason.
    if (RelOptUtil.classifyFilters(
        join,
        joinFilters,
        false,
        joinType.canPushLeftFromWithin(),
        canPushRight && joinType.canPushRightFromWithin(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if ((!filterPushed
        && joinType == join.getJoinType())
        || (joinFilters.isEmpty()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty())) {
      return;
    }

    // create Filters on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelNode leftRel =
        relBuilder.push(join.getLeft()).filter(leftFilters).build();
    final RelNode rightRel =
        relBuilder.push(join.getRight()).filter(rightFilters).build();

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final List<RelDataType> fieldTypes = new ArrayList<>();
    fieldTypes.addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()));
    fieldTypes.addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType()));
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder,
            RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinType == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getTraitSet(),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty() && filter != null) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty() && filter != null) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    relBuilder.push(newJoinRel);

    // Create a project on top of the join if some of the columns have become
    // NOT NULL due to the join-type getting stricter.
    relBuilder.convert(join.getRowType(), false);

    // create a FilterRel on top of the join if needed
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, aboveFilters,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
    call.transformTo(relBuilder.build());
  }

  private static List<RexNode> getConjunctions(Filter filter) {
    List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    for (int i = 0; i < conjunctions.size(); i++) {
      RexNode node = conjunctions.get(i);
      if (node instanceof RexCall) {
        conjunctions.set(i,
            RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
      }
    }
    return conjunctions;
  }
  //@formatter:on

  public static class PinotJoinConditionPushRule extends PinotFilterJoinRule<JoinConditionPushRule.Config> {
    public static final PinotJoinConditionPushRule INSTANCE =
        new PinotJoinConditionPushRule(JoinConditionPushRuleConfig.DEFAULT);

    public static PinotJoinConditionPushRule instanceWithDescription(String description) {
      return new PinotJoinConditionPushRule(
            (JoinConditionPushRuleConfig) JoinConditionPushRuleConfig.DEFAULT.withDescription(description));
    }

    private PinotJoinConditionPushRule(JoinConditionPushRuleConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      perform(call, null, call.rel(0));
    }
  }

  public static class PinotFilterIntoJoinRule extends PinotFilterJoinRule<FilterIntoJoinRuleConfig> {
    public static final PinotFilterIntoJoinRule INSTANCE =
        new PinotFilterIntoJoinRule(FilterIntoJoinRuleConfig.DEFAULT);

    public static PinotFilterIntoJoinRule instanceWithDescription(String description) {
      return new PinotFilterIntoJoinRule(
          (FilterIntoJoinRuleConfig) FilterIntoJoinRuleConfig.DEFAULT.withDescription(description));
    }

    private PinotFilterIntoJoinRule(FilterIntoJoinRuleConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      perform(call, filter, join);
    }
  }
}
