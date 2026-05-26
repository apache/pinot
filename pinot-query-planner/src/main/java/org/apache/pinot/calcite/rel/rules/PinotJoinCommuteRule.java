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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.query.catalog.PinotTable;


/**
 * Logical-phase rule that swaps the inputs of a join when the left input is a dimension table and the right is not.
 *
 * <p>Pinot's exchange-insertion ({@link PinotJoinExchangeNodeInsertRule}) and lookup-join planning are sensitive to
 * which input appears on the right: the right side is broadcast for non-equi joins, becomes the build side at runtime
 * (see {@code BaseJoinOperator}), and is required to be a dimension table for the lookup-join strategy. When a user
 * writes {@code dim JOIN fact ON dim.k = fact.k}, the planner — without this rule — broadcasts and/or builds the fact
 * side, which is exactly the wrong choice. Swapping the inputs so the dim ends up on the right fixes both problems
 * with a single, locally reasoned transformation.
 *
 * <p>This is intentionally a narrow, no-statistics commute rule: it fires only when one side is configured as a
 * dimension table (a property already known at catalog-construction time). It does <b>not</b> attempt size-based
 * commutation between two non-dim tables; that requires a cardinality-aware cost model and is tracked as future
 * work toward a full cost-based optimizer.
 *
 * <p>Predicate (all must hold for commutation to fire):
 * <ol>
 *   <li>Join type is INNER, LEFT, RIGHT, or FULL. SEMI / ANTI / ASOF are skipped because they are semantically
 *       asymmetric (a separate rule, e.g. extension of {@link PinotJoinToDynamicBroadcastRule}, must handle the
 *       small-side-on-left case for semi-joins).</li>
 *   <li>The join has at least one equi-key. Non-equi joins use a different operator and runtime broadcast policy;
 *       commuting them is in scope for a follow-up PR.</li>
 *   <li>No user-supplied hint pins the distribution or join strategy. Specifically, the rule bails if the join
 *       carries any of {@code left_distribution_type}, {@code right_distribution_type}, or
 *       {@code is_colocated_by_join_keys} — these express explicit intent about side roles that the rule must not
 *       silently invert. The rule also bails on {@code join_strategy='hash'} and
 *       {@code join_strategy='dynamic_broadcast'}.
 *
 *       <p><b>Special case:</b> {@code join_strategy='lookup'} is the one strategy whose precondition is
 *       <i>directional</i> — the right input must be a dimension table (enforced at runtime by
 *       {@code LookupJoinOperator}). When the user writes {@code dim JOIN fact} with this hint, the hint is
 *       <i>unsatisfiable as written</i>; the only valid interpretation is to commute. The rule therefore lets
 *       commutation fire in this case; the preserved hint reaches downstream lookup-join planning, which then
 *       applies cleanly on the commuted plan. We auto-correct only when the user's intent is otherwise
 *       unrealizable.</li>
 *   <li>The left input is "dim-shaped" — its rel subtree resolves through a chain of {@link Project}/{@link Filter}
 *       nodes to a single {@link TableScan} of a dimension table. Subtrees containing {@code Aggregate},
 *       {@code Join}, {@code Window}, {@code Union}, etc. are conservatively rejected: their output is no longer
 *       guaranteed dim-sized.</li>
 *   <li>The right input is <b>not</b> dim-shaped. If both sides are dim, the rule has no preference signal and
 *       declines to fire — this also guarantees idempotency: once the dim is on the right, the predicate is false
 *       and the rule will not flip the join back.</li>
 * </ol>
 *
 * <p>The actual swap delegates to Calcite's
 * {@link JoinCommuteRule#swap(Join, boolean, org.apache.calcite.tools.RelBuilder)} with {@code swapOuterJoins=true},
 * which produces a Project-wrapped swapped Join with the join type and condition correctly inverted (LEFT ↔ RIGHT,
 * condition operands swapped). Join hints are preserved by {@code Join.copy()}.
 */
public class PinotJoinCommuteRule extends RelOptRule {
  public static final PinotJoinCommuteRule INSTANCE = new PinotJoinCommuteRule(PinotRuleUtils.PINOT_REL_FACTORY, null);

  public static PinotJoinCommuteRule instanceWithDescription(String description) {
    return new PinotJoinCommuteRule(PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  public PinotJoinCommuteRule(RelBuilderFactory factory, @Nullable String description) {
    super(operand(Join.class, any()), factory, description);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);

    // Asymmetric join types: semi/anti distinguish left from right by semantics, not just position; ASOF anchors
    // the temporal scan on the left side. Calcite's swap() returns null for these as well, but we short-circuit
    // here to avoid the dim-detection walk on joins we will never commute.
    JoinRelType joinType = join.getJoinType();
    if (joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI) {
      return false;
    }
    if (join instanceof LogicalAsofJoin) {
      return false;
    }

    // Require an equi-key. Without one, the runtime falls into NonEquiJoinOperator which has a separate broadcast
    // policy; commuting it is in scope for a follow-up PR.
    JoinInfo joinInfo = join.analyzeCondition();
    if (joinInfo.leftKeys.isEmpty()) {
      return false;
    }

    // Bail if the user has expressed any explicit intent about side roles. We do not want to silently invert a
    // deliberate hint like right_distribution_type=broadcast or join_strategy=lookup.
    if (hasUserPinnedSideHint(join)) {
      return false;
    }

    // The decision signal: dim on the wrong side, and only on the wrong side.
    return isDimShaped(join.getLeft()) && !isDimShaped(join.getRight());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode swapped = JoinCommuteRule.swap(join, /*swapOuterJoins=*/ true, call.builder());
    // swap() should not return null given matches() guards, but defend in case Calcite adds new pre-conditions.
    if (swapped != null) {
      call.transformTo(swapped);
    }
  }

  /**
   * Returns {@code true} if any hint on the join expresses explicit intent about which side should play which role,
   * <i>except</i> for {@code join_strategy='lookup'} with dim-on-left — that one combination is unsatisfiable as
   * written and commuting is the unique way to honor the user's intent (see class Javadoc, "Special case").
   */
  private static boolean hasUserPinnedSideHint(Join join) {
    String strategy = JoinHintOptions.getJoinStrategyHint(join);
    if (strategy != null && !JoinHintOptions.LOOKUP_JOIN_STRATEGY.equalsIgnoreCase(strategy)) {
      return true;
    }
    Map<String, String> hintOptions = JoinHintOptions.getJoinHintOptions(join);
    if (hintOptions == null) {
      return false;
    }
    return hintOptions.containsKey(JoinHintOptions.LEFT_DISTRIBUTION_TYPE)
        || hintOptions.containsKey(JoinHintOptions.RIGHT_DISTRIBUTION_TYPE)
        || hintOptions.containsKey(JoinHintOptions.IS_COLOCATED_BY_JOIN_KEYS);
  }

  /**
   * Returns {@code true} if {@code input} resolves through a thin chain of {@link Project} / {@link Filter} nodes
   * to a single {@link TableScan} whose underlying table is configured as a dimension table.
   *
   * <p>Single-input, single-output relational operators (Project, Filter) preserve the "this row corresponds to a
   * dim table row" property and therefore do not invalidate the dim-shape detection. Multi-input or
   * cardinality-changing operators (Join, Aggregate, Window, Union, etc.) do, and are conservatively rejected.
   */
  private static boolean isDimShaped(RelNode input) {
    RelNode current = PinotRuleUtils.unboxRel(input);
    while (true) {
      if (current instanceof TableScan) {
        return isDimTableScan((TableScan) current);
      }
      if (current instanceof Project || current instanceof Filter) {
        current = PinotRuleUtils.unboxRel(current.getInput(0));
        continue;
      }
      return false;
    }
  }

  private static boolean isDimTableScan(TableScan scan) {
    RelOptTable relOptTable = scan.getTable();
    if (relOptTable == null) {
      return false;
    }
    PinotTable pinotTable = unwrapPinotTable(relOptTable);
    return pinotTable != null && pinotTable.isDimTable();
  }

  @Nullable
  private static PinotTable unwrapPinotTable(RelOptTable relOptTable) {
    return relOptTable.unwrap(PinotTable.class);
  }
}
