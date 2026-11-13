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
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;


/// Rewrites a filter predicate that replaces a null `ARRAY_TO_MV(array)` with a default value, so that the leaf stage
/// can run it.
///
/// The leaf stage runs `ARRAY_TO_MV(array) <op> value` as a multi-value predicate of the single-stage engine. It cannot
/// run `COALESCE(ARRAY_TO_MV(array), default)`, because the single-stage engine has no multi-value `COALESCE` or
/// `CASE`. This rule moves the null check out of the expression, with `A` for `ARRAY_TO_MV(array)`:
///
/// | Before | After |
/// |---|---|
/// | `pred(COALESCE(A, d))` | `(A IS NOT NULL AND pred(A)) OR (A IS NULL AND pred(d))` |
/// | `pred(CASE WHEN A IS NULL THEN d ELSE A END)` | `(A IS NOT NULL AND pred(A)) OR (A IS NULL AND pred(d))` |
/// | `CASE WHEN A IS NULL THEN v ELSE e END` | `(A IS NULL AND v) OR (A IS NOT NULL AND e)` |
/// | `CASE WHEN A IS NOT NULL THEN e ELSE v END` | `(A IS NULL AND v) OR (A IS NOT NULL AND e)` |
///
/// `pred` is a boolean call with `COALESCE(A, d)` (or the same `CASE`) as one operand, for example `=`, `<>`, `<` or
/// `LIKE`. Calcite can also move `pred` into the branches of the `CASE`, which gives the boolean `CASE` forms. When
/// `pred(d)` or `v` evaluates to a literal, the result is simpler: `A IS NULL OR pred(A)` for `true`, and
/// `A IS NOT NULL AND pred(A)` for `false`.
///
/// The rewrite gives the same result for every row in three-valued logic. `pred(A)` keeps the multi-value meaning of
/// the single-stage engine: for example, `A = 'a'` matches when an element is `'a'`, and `A <> 'a'` matches when no
/// element is `'a'`. The `A IS NOT NULL` check is necessary because, without null handling, a null array is stored as
/// the default value, and `pred(A)` can match it.
///
/// The rule does not change other uses of `ARRAY_TO_MV`, for example `UPPER(COALESCE(A, d)) = 'X'`. A predicate that
/// still has `ARRAY_TO_MV` in an intermediate stage fails the `ARRAY_TO_MV` validation, as before.
///
/// The rule is stateless and thread-safe.
public class PinotArrayToMvCoalesceFilterRule extends RelOptRule {
  public static final PinotArrayToMvCoalesceFilterRule INSTANCE =
      new PinotArrayToMvCoalesceFilterRule(PinotRuleUtils.PINOT_REL_FACTORY, null);

  public static PinotArrayToMvCoalesceFilterRule instanceWithDescription(String description) {
    return new PinotArrayToMvCoalesceFilterRule(PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  private PinotArrayToMvCoalesceFilterRule(RelBuilderFactory factory, @Nullable String description) {
    super(operand(LogicalFilter.class, any()), factory, description);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    return PinotProjectJoinTransposeRule.containsArrayToMv(filter.getCondition());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    RexNode condition = filter.getCondition();
    RexNode newCondition = rewrite(condition, filter.getCluster().getRexBuilder());
    if (newCondition != condition) {
      call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition));
    }
  }

  /// Returns `condition` with the predicates in the table of the class doc rewritten, or `condition` itself when no
  /// predicate matches.
  public static RexNode rewrite(RexNode condition, RexBuilder rexBuilder) {
    RexNode newCondition = condition.accept(new Shuttle(rexBuilder));
    return newCondition == condition ? condition : RexUtil.flatten(rexBuilder, newCondition);
  }

  private static class Shuttle extends RexShuttle {
    private final RexBuilder _rexBuilder;

    Shuttle(RexBuilder rexBuilder) {
      _rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visited = (RexCall) super.visitCall(call);
      if (visited.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        return visited;
      }
      RexNode rewritten = visited.getKind() == SqlKind.CASE ? rewriteCase(visited) : rewritePredicate(visited);
      return rewritten != null ? rewritten : visited;
    }

    /// Rewrites `pred(COALESCE(A, d...))` and `pred(CASE WHEN A IS NULL THEN d ELSE A END)`, or returns `null` when
    /// `call` does not have one of these forms.
    @Nullable
    private RexNode rewritePredicate(RexCall call) {
      if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR) || call.isA(SqlKind.NOT)) {
        return null;
      }
      List<RexNode> operands = call.getOperands();
      int index = -1;
      RexNode[] arrayToMvAndDefault = null;
      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        RexNode[] match = matchDefaultForNull(operand);
        if (match != null) {
          if (index >= 0) {
            return null;
          }
          index = i;
          arrayToMvAndDefault = match;
        } else if (PinotProjectJoinTransposeRule.containsArrayToMv(operand)) {
          return null;
        }
      }
      if (arrayToMvAndDefault == null) {
        return null;
      }
      RexNode arrayToMv = arrayToMvAndDefault[0];
      RexNode notNullBranch = replaceOperand(call, index, arrayToMv);
      RexNode nullBranch = RexUtil.expandSearch(_rexBuilder, null, replaceOperand(call, index, arrayToMvAndDefault[1]));
      return combine(arrayToMv, nullBranch, notNullBranch);
    }

    /// Returns `{A, d}` when `node` is `COALESCE(A, d...)`, `CASE WHEN A IS NULL THEN d ELSE A END` or
    /// `CASE WHEN A IS NOT NULL THEN A ELSE d END`, where `A` is an `ARRAY_TO_MV` call and `d` has no `ARRAY_TO_MV`
    /// call. Returns `null` otherwise.
    @Nullable
    private RexNode[] matchDefaultForNull(RexNode node) {
      if (!(node instanceof RexCall)) {
        return null;
      }
      List<RexNode> operands = ((RexCall) node).getOperands();
      RexNode arrayToMv;
      RexNode defaultValue;
      if (node.isA(SqlKind.COALESCE)) {
        if (operands.size() < 2) {
          return null;
        }
        arrayToMv = operands.get(0);
        List<RexNode> defaults = operands.subList(1, operands.size());
        defaultValue =
            defaults.size() == 1 ? defaults.get(0) : _rexBuilder.makeCall(((RexCall) node).getOperator(), defaults);
      } else if (node.isA(SqlKind.CASE) && operands.size() == 3 && operands.get(0) instanceof RexCall) {
        RexCall when = (RexCall) operands.get(0);
        boolean isNull = when.isA(SqlKind.IS_NULL);
        if (!isNull && !when.isA(SqlKind.IS_NOT_NULL)) {
          return null;
        }
        arrayToMv = when.getOperands().get(0);
        if (!arrayToMv.equals(isNull ? operands.get(2) : operands.get(1))) {
          return null;
        }
        defaultValue = isNull ? operands.get(1) : operands.get(2);
      } else {
        return null;
      }
      if (!ArrayToMvGroupKeys.isArrayToMvCall(arrayToMv) || PinotProjectJoinTransposeRule.containsArrayToMv(
          defaultValue)) {
        return null;
      }
      return new RexNode[]{arrayToMv, defaultValue};
    }

    /// Rewrites `CASE WHEN A IS NULL THEN v ELSE e END` and `CASE WHEN A IS NOT NULL THEN e ELSE v END`, or returns
    /// `null` when `call` does not have one of these forms.
    @Nullable
    private RexNode rewriteCase(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 3 || !(operands.get(0) instanceof RexCall)) {
        return null;
      }
      RexCall when = (RexCall) operands.get(0);
      boolean isNull = when.isA(SqlKind.IS_NULL);
      if (!(isNull || when.isA(SqlKind.IS_NOT_NULL))
          || !ArrayToMvGroupKeys.isArrayToMvCall(when.getOperands().get(0))) {
        return null;
      }
      RexNode nullBranch = isNull ? operands.get(1) : operands.get(2);
      RexNode notNullBranch = isNull ? operands.get(2) : operands.get(1);
      if (PinotProjectJoinTransposeRule.containsArrayToMv(nullBranch)) {
        return null;
      }
      return combine(when.getOperands().get(0), nullBranch, notNullBranch);
    }

    /// Returns `(A IS NULL AND nullBranch) OR (A IS NOT NULL AND notNullBranch)`, simplified when `nullBranch`
    /// evaluates to `true` or `false`.
    private RexNode combine(RexNode arrayToMv, RexNode nullBranch, RexNode notNullBranch) {
      RexNode isNull = _rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, arrayToMv);
      RexNode isNotNull = _rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, arrayToMv);
      RexNode evaluatedNullBranch = PinotEvaluateLiteralRule.Filter.evaluateLiterals(nullBranch, _rexBuilder);
      if (evaluatedNullBranch.isAlwaysTrue()) {
        return _rexBuilder.makeCall(SqlStdOperatorTable.OR, isNull, notNullBranch);
      }
      if (evaluatedNullBranch.isAlwaysFalse()) {
        return _rexBuilder.makeCall(SqlStdOperatorTable.AND, isNotNull, notNullBranch);
      }
      return _rexBuilder.makeCall(SqlStdOperatorTable.OR,
          _rexBuilder.makeCall(SqlStdOperatorTable.AND, isNull, evaluatedNullBranch),
          _rexBuilder.makeCall(SqlStdOperatorTable.AND, isNotNull, notNullBranch));
    }

    private static RexNode replaceOperand(RexCall call, int index, RexNode operand) {
      List<RexNode> operands = new ArrayList<>(call.getOperands());
      operands.set(index, operand);
      return call.clone(call.getType(), operands);
    }
  }
}
