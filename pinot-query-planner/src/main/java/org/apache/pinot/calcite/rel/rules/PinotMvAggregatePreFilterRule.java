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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.calcite.rex.PinotSealedSearchOperator;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;


/// Copies sound positive post-aggregate predicates on bare `ARRAY_TO_MV(tableScanCol)` group keys into a leaf
/// pre-filter directly above the [TableScan], while keeping the original [Filter] above the [Aggregate] intact.
///
/// For example, given:
/// ```sql
/// SELECT ARRAY_TO_MV(tags), COUNT(*) FROM t GROUP BY ARRAY_TO_MV(tags) HAVING ARRAY_TO_MV(tags) = 'vip'
/// ```
/// [PinotFilterAggregateTransposeRule] keeps `k = 'vip'` above the aggregate so non-`'vip'` elements of matching rows
/// are discarded after grouping. Without a leaf pre-filter, however, the leaf stage would aggregate every row in `t`.
/// This rule copies `ARRAY_TO_MV(tags) = 'vip'` directly above `TableScan(t)` (merged with any existing leaf `Filter`),
/// allowing the server to prune non-matching rows with inverted/range indexes before aggregation while the upper
/// filter still enforces exact per-group semantics.
///
/// Only strictly sound inclusive existential predicates on a single bare `ARRAY_TO_MV(tableScanCol)` group key are
/// copied:
/// - `=`, `<`, `<=`, `>`, `>=` against a non-null [RexLiteral] (including reversed `literal op key`).
/// - Asymmetric non-negated `BETWEEN` against two non-null [RexLiteral] bounds.
/// - `SEARCH` / [PinotSealedSearchOperator] only when `nullAs != TRUE`, the range set is neither empty nor unbounded
///   (`Range.all()`), and either `isPoints()` holds (O(1) fast path for large sealed `IN` lists) or
///   `!isComplementedPoints()` and its complement contains no single-point ranges (blocking `NOT IN`, `<>`, and
///   `IS NULL` / `IS NOT NULL` emission in `RexExpressionUtils`).
/// - `OR` / `AND` only when every operand is itself a sound positive predicate on the same single MV group key.
///
/// Negated predicates (`<>`, `NOT IN`, `NOT`), null checks (`IS NULL`, `IS NOT NULL`), wrapped expressions
/// (`LOWER(ARRAY_TO_MV(col))`, `CAST(...)`, `COALESCE(...)`), column-to-column comparisons, non-simple grouping sets,
/// and aggregates over non-scan inputs (such as joins) are never copied.
///
/// The copied filter is inserted below `Project` directly over `TableScan` with `ARRAY_TO_MV(col)` inlined so
/// `AggregateProjectPullUpConstantsRule` never treats the MV group key as a constant. Already-present leaf conjuncts
/// are skipped, guaranteeing O(1) idempotency with no planner loops.
///
/// The rule is stateless and thread-safe.
public class PinotMvAggregatePreFilterRule extends RelRule<RelRule.Config> {
  private static final Config DEFAULT_CONFIG = new RuleConfig(PinotRuleUtils.PINOT_REL_FACTORY,
      PlannerRuleNames.MV_AGGREGATE_PRE_FILTER,
      b0 -> b0.operand(Filter.class).oneInput(b1 -> b1.operand(Aggregate.class).anyInputs()));

  public static final PinotMvAggregatePreFilterRule INSTANCE = new PinotMvAggregatePreFilterRule(DEFAULT_CONFIG);

  private PinotMvAggregatePreFilterRule(Config config) {
    super(config);
  }

  private static final class RuleConfig implements RelRule.Config {
    private final RelBuilderFactory _relBuilderFactory;
    private final String _description;
    private final OperandTransform _operandSupplier;

    private RuleConfig(RelBuilderFactory relBuilderFactory, String description, OperandTransform operandSupplier) {
      _relBuilderFactory = relBuilderFactory;
      _description = description;
      _operandSupplier = operandSupplier;
    }

    @Override
    public PinotMvAggregatePreFilterRule toRule() {
      return new PinotMvAggregatePreFilterRule(this);
    }

    @Override
    public RelBuilderFactory relBuilderFactory() {
      return _relBuilderFactory;
    }

    @Override
    public RuleConfig withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
      return new RuleConfig(relBuilderFactory, _description, _operandSupplier);
    }

    @Override
    public String description() {
      return _description;
    }

    @Override
    public RuleConfig withDescription(String description) {
      return new RuleConfig(_relBuilderFactory, description, _operandSupplier);
    }

    @Override
    public OperandTransform operandSupplier() {
      return _operandSupplier;
    }

    @Override
    public RuleConfig withOperandSupplier(OperandTransform operandSupplier) {
      return new RuleConfig(_relBuilderFactory, _description, operandSupplier);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter topFilter = call.rel(0);
    Aggregate aggregate = call.rel(1);
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE || aggregate.getGroupCount() == 0) {
      return;
    }

    // Collect the chain between Aggregate and TableScan, strictly enforcing Project* -> Filter* -> TableScan.
    // A Filter above a Project (e.g. when FilterProjectTransposeRule skipped a bloated expression or was disabled)
    // must bail out: inserting another Filter below that Project would leave two unmergeable Filters split by a
    // Project, breaking ServerPlanRequestVisitor's single leaf Filter invariant.
    List<RelNode> chain = new ArrayList<>();
    RelNode current = PinotRuleUtils.unboxRel(aggregate.getInput());
    boolean seenFilter = false;
    while (current instanceof Project || current instanceof Filter) {
      if (current instanceof Filter) {
        if (((Filter) current).getCondition().isAlwaysFalse()) {
          return;
        }
        seenFilter = true;
      } else if (seenFilter) {
        return;
      }
      chain.add(current);
      current = PinotRuleUtils.unboxRel(current.getInput(0));
    }
    if (!(current instanceof TableScan)) {
      return;
    }
    TableScan tableScan = (TableScan) current;

    // Map each aggregate output column that traces to a bare top-level ARRAY_TO_MV(bareTableScanCol)
    // to its leaf expression ARRAY_TO_MV($tableScanCol).
    RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
    List<Integer> groupColumns = aggregate.getGroupSet().asList();
    Map<Integer, RexNode> mvKeyToLeafExpr = new HashMap<>();
    for (int outputCol = 0; outputCol < groupColumns.size(); outputCol++) {
      RexNode leafExpr = resolveBareArrayToMvLeafExpr(chain, tableScan, groupColumns.get(outputCol), rexBuilder);
      if (leafExpr != null) {
        mvKeyToLeafExpr.put(outputCol, leafExpr);
      }
    }
    if (mvKeyToLeafExpr.isEmpty()) {
      return;
    }

    // Collect existing conjuncts from any Filters directly above TableScan for idempotency.
    Set<RexNode> seenLeafConjuncts = new HashSet<>();
    for (int i = chain.size() - 1; i >= 0 && chain.get(i) instanceof Filter; i--) {
      seenLeafConjuncts.addAll(RelOptUtil.conjunctions(((Filter) chain.get(i)).getCondition()));
    }

    List<RexNode> newLeafConjuncts = new ArrayList<>();
    for (RexNode conjunct : RelOptUtil.conjunctions(topFilter.getCondition())) {
      ImmutableBitSet cols = RelOptUtil.InputFinder.bits(conjunct);
      if (cols.cardinality() != 1) {
        continue;
      }
      int keyCol = cols.nextSetBit(0);
      RexNode leafArrayToMv = mvKeyToLeafExpr.get(keyCol);
      if (leafArrayToMv == null || !isSoundPositivePredicate(conjunct, keyCol)) {
        continue;
      }
      RexNode leafConjunct = rewriteToLeaf(conjunct, keyCol, leafArrayToMv, rexBuilder);
      if (seenLeafConjuncts.add(leafConjunct)) {
        newLeafConjuncts.add(leafConjunct);
      }
    }

    if (newLeafConjuncts.isEmpty()) {
      return;
    }

    // Merge into the bottom-most leaf Filter if one already sits directly over TableScan, or create a new
    // LogicalFilter directly over TableScan. Using LogicalFilter.create + RexUtil.composeConjunction avoids
    // RelBuilder's RexSimplify rewriting conjuncts into different forms that would break idempotency.
    int rebuildFrom;
    RelNode child;
    if (!chain.isEmpty() && chain.get(chain.size() - 1) instanceof Filter) {
      Filter leafFilter = (Filter) chain.get(chain.size() - 1);
      List<RexNode> combinedConjuncts = new ArrayList<>(RelOptUtil.conjunctions(leafFilter.getCondition()));
      combinedConjuncts.addAll(newLeafConjuncts);
      RexNode combinedCondition = RexUtil.composeConjunction(rexBuilder, combinedConjuncts);
      if (combinedCondition.equals(leafFilter.getCondition())) {
        return;
      }
      child = leafFilter.copy(leafFilter.getTraitSet(), tableScan, combinedCondition);
      rebuildFrom = chain.size() - 2;
    } else {
      RexNode preFilterCondition = RexUtil.composeConjunction(rexBuilder, newLeafConjuncts);
      child = LogicalFilter.create(tableScan, preFilterCondition);
      rebuildFrom = chain.size() - 1;
    }

    for (int i = rebuildFrom; i >= 0; i--) {
      RelNode node = chain.get(i);
      child = node.copy(node.getTraitSet(), List.of(child));
    }

    RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(), List.of(child));
    RelNode newTopFilter = topFilter.copy(topFilter.getTraitSet(), newAggregate, topFilter.getCondition());
    call.transformTo(newTopFilter);
  }

  /// Resolves `aggInputCol` through the `Project`/`Filter` chain down to `tableScan`.
  /// Returns `ARRAY_TO_MV($tableScanCol)` (preserving the `ARRAY_TO_MV` call's type) if and only if the column traces
  /// through pass-through `RexInputRef`s and exactly one bare top-level `ARRAY_TO_MV(bareCol)` call to a `TableScan`
  /// column; otherwise returns `null`.
  @Nullable
  private static RexNode resolveBareArrayToMvLeafExpr(List<RelNode> chain, TableScan tableScan, int aggInputCol,
      RexBuilder rexBuilder) {
    int currCol = aggInputCol;
    RexCall arrayToMvCall = null;
    for (RelNode node : chain) {
      if (node instanceof Filter) {
        continue;
      }
      Project project = (Project) node;
      RexNode expr = project.getProjects().get(currCol);
      if (expr instanceof RexInputRef) {
        currCol = ((RexInputRef) expr).getIndex();
      } else if (arrayToMvCall == null && ArrayToMvGroupKeys.isArrayToMvCall(expr)) {
        RexCall call = (RexCall) expr;
        RexNode operand = call.getOperands().get(0);
        if (!(operand instanceof RexInputRef)) {
          return null;
        }
        arrayToMvCall = call;
        currCol = ((RexInputRef) operand).getIndex();
      } else {
        return null;
      }
    }
    if (arrayToMvCall == null) {
      return null;
    }
    RexInputRef tableScanRef = rexBuilder.makeInputRef(tableScan, currCol);
    return rexBuilder.makeCall(arrayToMvCall.getType(), PinotOperatorTable.ARRAY_TO_MV, List.of(tableScanRef));
  }

  /// Returns whether `expr` is a strictly sound positive existential predicate on the single aggregate output column
  /// `keyCol`.
  private static boolean isSoundPositivePredicate(RexNode expr, int keyCol) {
    if (!(expr instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expr;
    SqlOperator op = call.getOperator();
    if (op instanceof PinotSealedSearchOperator) {
      return call.getOperands().size() == 1
          && isInputRef(call.getOperands().get(0), keyCol)
          && isSoundPositiveSarg(((PinotSealedSearchOperator) op).getSargLiteral());
    }
    switch (call.getKind()) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL: {
        if (call.getOperands().size() != 2) {
          return false;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        return (isInputRef(left, keyCol) && isNonNullLiteral(right))
            || (isNonNullLiteral(left) && isInputRef(right, keyCol));
      }
      case BETWEEN: {
        if (call.getOperands().size() != 3 || !(op instanceof SqlBetweenOperator)) {
          return false;
        }
        SqlBetweenOperator betweenOp = (SqlBetweenOperator) op;
        if (betweenOp.isNegated() || betweenOp.flag != SqlBetweenOperator.Flag.ASYMMETRIC) {
          return false;
        }
        return isInputRef(call.getOperands().get(0), keyCol)
            && isNonNullLiteral(call.getOperands().get(1))
            && isNonNullLiteral(call.getOperands().get(2));
      }
      case SEARCH: {
        if (call.getOperands().size() != 2) {
          return false;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        return isInputRef(left, keyCol)
            && right instanceof RexLiteral
            && isSoundPositiveSarg((RexLiteral) right);
      }
      case OR:
      case AND: {
        List<RexNode> operands = call.getOperands();
        if (operands.isEmpty()) {
          return false;
        }
        for (RexNode operand : operands) {
          if (!isSoundPositivePredicate(operand, keyCol)) {
            return false;
          }
        }
        return true;
      }
      default:
        return false;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean isSoundPositiveSarg(RexLiteral sargLiteral) {
    Sarg sarg = sargLiteral.getValueAs(Sarg.class);
    if (sarg == null || sarg.nullAs == RexUnknownAs.TRUE) {
      return false;
    }
    RangeSet rangeSet = sarg.rangeSet;
    if (rangeSet.isEmpty() || rangeSet.encloses(Range.all())) {
      return false;
    }
    // O(1) fast path for point-set IN lists so large sealed Sargs never materialize rangeSet.complement().
    if (sarg.isPoints()) {
      return true;
    }
    if (sarg.isComplementedPoints()) {
      return false;
    }
    Set<Range> complementRanges = rangeSet.complement().asRanges();
    for (Range range : complementRanges) {
      if (isPointRange(range)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean isPointRange(Range range) {
    return range.hasLowerBound() && range.hasUpperBound()
        && range.lowerBoundType() == BoundType.CLOSED
        && range.upperBoundType() == BoundType.CLOSED
        && range.lowerEndpoint().compareTo(range.upperEndpoint()) == 0;
  }

  private static boolean isInputRef(RexNode node, int keyCol) {
    return node instanceof RexInputRef && ((RexInputRef) node).getIndex() == keyCol;
  }

  private static boolean isNonNullLiteral(RexNode node) {
    return node instanceof RexLiteral && !((RexLiteral) node).isNull();
  }

  /// Rewrites a validated sound positive predicate on `keyCol` into leaf coordinates by replacing `RexInputRef(keyCol)`
  /// with `leafArrayToMv`, canonicalizing reversed `literal op key` comparisons to `key flippedOp literal`.
  private static RexNode rewriteToLeaf(RexNode expr, int keyCol, RexNode leafArrayToMv, RexBuilder rexBuilder) {
    if (expr instanceof RexInputRef) {
      return leafArrayToMv;
    }
    if (!(expr instanceof RexCall)) {
      return expr;
    }
    RexCall call = (RexCall) expr;
    SqlKind kind = call.getKind();
    if (call.getOperands().size() == 2 && isNonNullLiteral(call.getOperands().get(0))
        && isInputRef(call.getOperands().get(1), keyCol)) {
      SqlOperator flippedOp = flipComparisonOperator(kind);
      if (flippedOp != null) {
        return rexBuilder.makeCall(call.getType(), flippedOp, List.of(leafArrayToMv, call.getOperands().get(0)));
      }
    }
    List<RexNode> oldOperands = call.getOperands();
    List<RexNode> newOperands = new ArrayList<>(oldOperands.size());
    for (RexNode operand : oldOperands) {
      newOperands.add(rewriteToLeaf(operand, keyCol, leafArrayToMv, rexBuilder));
    }
    return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
  }

  @Nullable
  private static SqlOperator flipComparisonOperator(SqlKind kind) {
    switch (kind) {
      case EQUALS:
        return SqlStdOperatorTable.EQUALS;
      case LESS_THAN:
        return SqlStdOperatorTable.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN:
        return SqlStdOperatorTable.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      default:
        return null;
    }
  }
}
