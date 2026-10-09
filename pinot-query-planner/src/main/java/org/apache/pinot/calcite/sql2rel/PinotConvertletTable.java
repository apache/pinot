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
package org.apache.pinot.calcite.sql2rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.calcite.rex.PinotRexExecutor;
import org.apache.pinot.calcite.sql.fun.PinotInListOperator;


/// PinotConvertletTable is a wrapper of [StandardConvertletTable] with the customizations of not converting
/// certain SqlCalls, e.g. TIMESTAMPADD, TIMESTAMPDIFF. It also converts the large IN lists that `SearchSealer` marked
/// into one `SEARCH` each (see [InListConvertlet]).
public class PinotConvertletTable implements SqlRexConvertletTable {

  public static final PinotConvertletTable INSTANCE = new PinotConvertletTable();
  private static final SqlBetweenOperator PINOT_BETWEEN =
      new SqlBetweenOperator(SqlBetweenOperator.Flag.ASYMMETRIC, false) {
        @Override
        public boolean validRexOperands(int count, Litmus litmus) {
          return litmus.succeed();
        }
      };

  private PinotConvertletTable() {
  }

  @Nullable
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    if (call.getOperator() instanceof PinotInListOperator) {
      return InListConvertlet.INSTANCE;
    }
    switch (call.getKind()) {
      case TIMESTAMP_ADD:
        return TimestampAddConvertlet.INSTANCE;
      case TIMESTAMP_DIFF:
        return TimestampDiffConvertlet.INSTANCE;
      case BETWEEN:
        return BetweenConvertlet.INSTANCE;
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        // special convertlet to handle ROW comparisons
        if (isRowComparison(call)) {
          return RowComparisonConvertlet.INSTANCE;
        }
        return StandardConvertletTable.INSTANCE.get(call);
      default:
        return StandardConvertletTable.INSTANCE.get(call);
    }
  }

  /// Override [org.apache.calcite.sql2rel.StandardConvertletTable.TimestampAddConvertlet] to not convert the
  /// SqlCall to arithmetic time expression.
  private static class TimestampAddConvertlet implements SqlRexConvertlet {
    private static final TimestampAddConvertlet INSTANCE = new TimestampAddConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), SqlStdOperatorTable.TIMESTAMP_ADD,
          List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
              cx.convertExpression(call.operand(2))));
    }
  }

  /// Override [org.apache.calcite.sql2rel.StandardConvertletTable.TimestampDiffConvertlet] to not convert the
  /// SqlCall to arithmetic time expression.
  private static class TimestampDiffConvertlet implements SqlRexConvertlet {
    private static final TimestampDiffConvertlet INSTANCE = new TimestampDiffConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), SqlStdOperatorTable.TIMESTAMP_DIFF,
          List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
              cx.convertExpression(call.operand(2))));
    }
  }

  /// Override the standard convertlet for BETWEEN to avoid the rewrite to >= AND <= for MV columns since that breaks
  /// the filter predicate's semantics.
  private static class BetweenConvertlet implements SqlRexConvertlet {
    private static final BetweenConvertlet INSTANCE = new BetweenConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      if (call.operand(0) instanceof SqlCall && ((SqlCall) call.operand(0)).getOperator().getName()
          .equals("ARRAY_TO_MV")) {
        RexBuilder rexBuilder = cx.getRexBuilder();

        SqlBetweenOperator betweenOperator = (SqlBetweenOperator) call.getOperator();

        RexNode rexNode = rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), PINOT_BETWEEN,
            List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
                cx.convertExpression(call.operand(2))));

        // Since Pinot only has support for ASYMMETRIC BETWEEN, we need to rewrite SYMMETRIC BETWEEN, ASYMMETRIC NOT
        // BETWEEN, and SYMMETRIC NOT BETWEEN to the equivalent BETWEEN expressions.

        // (val BETWEEN SYMMETRIC x AND y) is equivalent to (val BETWEEN x AND y OR val BETWEEN y AND x)
        if (betweenOperator.flag == SqlBetweenOperator.Flag.SYMMETRIC) {
          RexNode flipped = rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), PINOT_BETWEEN,
              List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(2)),
                  cx.convertExpression(call.operand(1))));
          rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, rexNode, flipped);
        }

        if (betweenOperator.isNegated()) {
          rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, rexNode);
        }

        return rexNode;
      } else {
        return StandardConvertletTable.INSTANCE.convertBetween(cx, (SqlBetweenOperator) call.getOperator(), call);
      }
    }
  }

  /// Converts an `IN` or `NOT IN` call that `SearchSealer#markInLists` marked with [PinotInListOperator].
  ///
  /// The result is the expression that `SqlToRelConverter` and a later `RexSimplify` would build, without building
  /// and simplifying an `OR` of `N` terms (which is cubic in `N` outside a filter):
  /// 1. Like `SqlToRelConverter#convertInToOr`, each value becomes `operand = value`, converted by the standard
  ///    convertlets. So operand and literal types (including casts from type coercion) are the same as without
  ///    sealing. Each comparison is then simplified alone, which reduces casts of literals such as `CAST('1' AS
  ///    INTEGER)` (with the query's cached cast executables) and turns `x = NULL` into `NULL`.
  /// 2. The comparisons of one operand with non-null literals become one `SEARCH` ([RexBuilder#makeIn]). For `NOT IN`
  ///    the Sarg is negated, as `RexSimplify` does for `NOT(SEARCH)`. `SearchSealer#seal(RelNode)` seals the
  ///    `SEARCH` after `SqlToRelConverter`, once Calcite has folded the other predicates on the same operand (for
  ///    example `x IS NOT NULL`) into it, as it does without sealing.
  /// 3. Other comparisons (NULL values, values that are not literals) stay as they are: `IN` is the `OR` of all terms
  ///    and `NOT IN` is the `AND` of their negations. This is exact three-valued logic: for example,
  ///    `x IN (1, 2, NULL)` becomes `OR(SEARCH(x, [1, 2]), NULL)`.
  private static class InListConvertlet implements SqlRexConvertlet {
    static final InListConvertlet INSTANCE = new InListConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      RexBuilder rexBuilder = cx.getRexBuilder();
      boolean negated = ((PinotInListOperator) call.getOperator()).isNegated();
      SqlNode operand = call.operand(0);
      SqlNodeList values = call.operand(1);
      RexSimplify simplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, PinotRexExecutor.INSTANCE);

      // Each slot is a term, or the points of one operand at the position of its first point (like RexSimplify's Sarg
      // collector), so the terms keep the order that Calcite gives them.
      List<Object> slots = new ArrayList<>();
      Map<RexNode, Points> pointsByOperand = new HashMap<>();
      for (SqlNode value : values) {
        SqlCall equals = SqlStdOperatorTable.EQUALS.createCall(value.getParserPosition(), operand, value);
        RexNode term = simplify.simplifyUnknownAs(cx.convertExpression(equals), RexUnknownAs.UNKNOWN);
        RexNode pointOperand = getPointOperand(term);
        if (pointOperand == null) {
          slots.add(term);
          continue;
        }
        Points points = pointsByOperand.get(pointOperand);
        if (points == null) {
          points = new Points(pointOperand, new ArrayList<>());
          pointsByOperand.put(pointOperand, points);
          slots.add(points);
        }
        points.literals().add(getOther(term, pointOperand));
      }

      List<RexNode> terms = new ArrayList<>(slots.size());
      for (Object slot : slots) {
        if (slot instanceof Points points) {
          RexNode in = rexBuilder.makeIn(points.operand(), points.literals());
          terms.add(negated ? negate(rexBuilder, in) : in);
        } else {
          terms.add(negated ? negate(rexBuilder, (RexNode) slot) : (RexNode) slot);
        }
      }
      RexNode result;
      if (negated) {
        result = RexUtil.composeConjunction(rexBuilder, terms);
      } else {
        result = RexUtil.composeDisjunction(rexBuilder, terms);
      }
      return StandardConvertletTable.castToValidatedType(call, result, cx.getValidator(), rexBuilder);
    }

    /// Returns the operand of `term` if it compares a deterministic expression with a non-null literal (the terms
    /// that `RexSimplify`'s Sarg collector accepts for `=`), or null otherwise.
    @Nullable
    private static RexNode getPointOperand(RexNode term) {
      if (term.getKind() != SqlKind.EQUALS) {
        return null;
      }
      List<RexNode> operands = ((RexCall) term).getOperands();
      RexNode left = operands.get(0);
      RexNode right = operands.get(1);
      if (isNonNullLiteral(right) && RexUtil.isDeterministic(left)) {
        return left;
      }
      if (isNonNullLiteral(left) && RexUtil.isDeterministic(right)) {
        return right;
      }
      return null;
    }

    private static RexNode getOther(RexNode term, RexNode pointOperand) {
      List<RexNode> operands = ((RexCall) term).getOperands();
      return operands.get(0) == pointOperand ? operands.get(1) : operands.get(0);
    }

    private static boolean isNonNullLiteral(RexNode node) {
      return node instanceof RexLiteral && !((RexLiteral) node).isNull();
    }

    private static RexNode negate(RexBuilder rexBuilder, RexNode node) {
      if (node.getKind() == SqlKind.SEARCH) {
        RexCall search = (RexCall) node;
        RexLiteral sargLiteral = (RexLiteral) search.getOperands().get(1);
        Sarg<?> sarg = Objects.requireNonNull(sargLiteral.getValueAs(Sarg.class));
        return rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, search.getOperands().get(0),
            rexBuilder.makeSearchArgumentLiteral(sarg.negate(), sargLiteral.getType()));
      }
      if (RexUtil.isNullLiteral(node, false)) {
        // NOT(NULL) is NULL.
        return node;
      }
      if (node instanceof RexCall) {
        // For example, x = y becomes x <> y.
        RexNode negated = RexUtil.negate(rexBuilder, (RexCall) node);
        if (negated != null) {
          return negated;
        }
      }
      return RexUtil.not(node);
    }
  }

  /// The literals that one operand of an IN list is compared with.
  private record Points(RexNode operand, List<RexNode> literals) {
  }

  /// Check if a comparison call involves ROW expressions.
  private static boolean isRowComparison(SqlCall call) {
    if (call.getOperandList().size() != 2) {
      return false;
    }
    SqlNode left = call.operand(0);
    SqlNode right = call.operand(1);
    boolean leftIsRow = left instanceof SqlCall && ((SqlCall) left).getKind() == SqlKind.ROW;
    boolean rightIsRow = right instanceof SqlCall && ((SqlCall) right).getKind() == SqlKind.ROW;

    return leftIsRow || rightIsRow;
  }
}
