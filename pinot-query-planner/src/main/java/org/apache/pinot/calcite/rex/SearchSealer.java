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
package org.apache.pinot.calcite.rex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.calcite.sql.fun.PinotInListOperator;


/// Hides the large IN lists of one query from Calcite while the query is optimized.
///
/// Calcite keeps an IN list as `SEARCH($x, Sarg[...])`. Many rules and metadata handlers rebuild the Sarg's range set
/// each time they touch it (`O(N log N)`), and each Calcite release and each new planner rule adds such touches. This
/// class replaces every `SEARCH` whose Sarg has at least [#getThreshold()] ranges with a call to a
/// [PinotSealedSearchOperator], which Calcite cannot look into, and turns it back into the same `SEARCH` once
/// optimization is done. Everything after optimization (EXPLAIN, plan node conversion, broker pruning, the servers)
/// sees the same `SEARCH` calls as without sealing.
///
/// Planning a query uses one instance, in this order:
/// 1. [#markInLists] after validation. Large `IN` value lists skip `SqlToRelConverter`'s expansion into `OR`, and
///    `PinotConvertletTable` converts each marked call into one `SEARCH`. Closing the [MarkedInLists] restores the SQL
///    operators after the conversion.
/// 2. [#seal(RelNode)] after `SqlToRelConverter`. It first simplifies each filter and join condition that holds a
///    large Sarg, like `RelBuilder#filter` does, so that the other predicates on the same operand fold into the Sarg
///    as they do without sealing (for example `x IS NOT NULL`, a range, or a second list). Then it seals the large
///    `SEARCH` calls, including the ones that Calcite built from a user-written `x = 1 OR x = 2 OR ...`.
/// 3. [#unseal(RelNode)] after the last optimizer program.
///
/// Sealed Sargs lose Calcite's value-level reasoning across plan nodes during optimization. For example, a filter
/// that a rule pushes onto another filter with a sealed list on the same column is not merged with it. Filter
/// push-down, transitive predicates across joins, outer join simplification and all rules that treat the predicate
/// as a whole work as before.
///
/// Not thread safe: a query is planned by a single thread.
public final class SearchSealer {
  private final int _threshold;
  /// One operator per distinct Sarg literal (value and type), so that equal lists stay equal (for example in both
  /// branches of a common table expression).
  private final Map<RexLiteral, PinotSealedSearchOperator> _operators = new HashMap<>();

  /// @param threshold the smallest number of Sarg ranges (IN list values) to seal; 0 or less disables sealing
  public SearchSealer(int threshold) {
    _threshold = threshold;
  }

  /// Returns the smallest number of Sarg ranges (IN list values) that this instance seals.
  public int getThreshold() {
    return _threshold;
  }

  /// Returns whether this instance seals anything.
  public boolean isEnabled() {
    return _threshold > 0;
  }

  /// Returns whether a Sarg is large enough to seal.
  boolean shouldSeal(Sarg<?> sarg) {
    // A Sarg that is all or none has at most one range and is never large. Calcite has special cases for these
    // (for example RexCall#isAlwaysTrue), so they always stay visible.
    return _threshold > 0 && !sarg.isAll() && !sarg.isNone() && sarg.rangeSet.asRanges().size() >= _threshold;
  }

  /// Returns a sealed call for a `SEARCH` call whose Sarg is large enough, or the call itself otherwise.
  RexNode seal(RexBuilder rexBuilder, RexCall search) {
    RexLiteral sargLiteral = (RexLiteral) search.getOperands().get(1);
    Sarg<?> sarg = sargLiteral.getValueAs(Sarg.class);
    if (sarg == null || !shouldSeal(sarg)) {
      return search;
    }
    PinotSealedSearchOperator operator =
        _operators.computeIfAbsent(sargLiteral, literal -> new PinotSealedSearchOperator(_operators.size(), literal));
    return rexBuilder.makeCall(search.getType(), operator, List.of(search.getOperands().get(0)));
  }

  // --------------------------------------------------------------------------
  // SQL level
  // --------------------------------------------------------------------------

  /// Sets [PinotInListOperator] on every `IN` and `NOT IN` call in the validated tree whose value list has at least
  /// [#getThreshold()] values. Close the result after `SqlToRelConverter` is done, to restore the original operators:
  ///
  /// ```
  /// try (SearchSealer.MarkedInLists ignored = searchSealer.markInLists(validated)) {
  ///   relRoot = converter.convertQuery(validated, false, true);
  /// }
  /// ```
  ///
  /// Lists on a row (`(a, b) IN (...)`) and lists that contain a sub-query keep the standard conversion.
  public MarkedInLists markInLists(SqlNode validated) {
    List<SqlBasicCall> marked = new ArrayList<>();
    if (!isEnabled()) {
      return new MarkedInLists(marked);
    }
    try {
      validated.accept(new SqlBasicVisitor<Void>() {
        @Override
        public Void visit(SqlCall call) {
          if (call instanceof SqlBasicCall && isLargeInList(call)) {
            ((SqlBasicCall) call).setOperator(PinotInListOperator.of(call.getKind()));
            marked.add((SqlBasicCall) call);
            // The values have no sub-queries (checked above), so only the left operand needs a visit.
            call.operand(0).accept(this);
            return null;
          }
          return super.visit(call);
        }
      });
    } catch (Throwable t) {
      new MarkedInLists(marked).close();
      throw t;
    }
    return new MarkedInLists(marked);
  }

  /// The `IN` and `NOT IN` calls that [#markInLists] marked. [#close()] restores their original operators.
  public static final class MarkedInLists implements AutoCloseable {
    private final List<SqlBasicCall> _calls;

    private MarkedInLists(List<SqlBasicCall> calls) {
      _calls = calls;
    }

    @Override
    public void close() {
      for (SqlBasicCall call : _calls) {
        call.setOperator(((PinotInListOperator) call.getOperator()).getOriginal());
      }
    }
  }

  private boolean isLargeInList(SqlCall call) {
    SqlKind kind = call.getKind();
    if ((kind != SqlKind.IN && kind != SqlKind.NOT_IN) || !(call.getOperator() instanceof SqlInOperator)
        || call.operandCount() != 2) {
      return false;
    }
    if (!(call.operand(1) instanceof SqlNodeList) || call.operand(0).getKind() == SqlKind.ROW) {
      return false;
    }
    SqlNodeList values = call.operand(1);
    return values.size() >= _threshold && !containsQueryOrRow(values);
  }

  private static boolean containsQueryOrRow(SqlNodeList values) {
    SqlBasicVisitor<Boolean> finder = new SqlBasicVisitor<>() {
      @Override
      public Boolean visit(SqlCall call) {
        SqlKind kind = call.getKind();
        if (kind == SqlKind.ROW || kind == SqlKind.SCALAR_QUERY || kind.belongsTo(SqlKind.QUERY)) {
          return true;
        }
        for (SqlNode operand : call.getOperandList()) {
          if (operand != null && Boolean.TRUE.equals(operand.accept(this))) {
            return true;
          }
        }
        return false;
      }

      @Override
      public Boolean visit(SqlNodeList nodeList) {
        for (SqlNode node : nodeList) {
          if (node != null && Boolean.TRUE.equals(node.accept(this))) {
            return true;
          }
        }
        return false;
      }
    };
    return Boolean.TRUE.equals(values.accept(finder));
  }

  // --------------------------------------------------------------------------
  // Relational level
  // --------------------------------------------------------------------------

  /// Seals every large `SEARCH` in a plan that `SqlToRelConverter` produced.
  ///
  /// A filter or join condition that holds a large Sarg, or a large `AND` / `OR` of comparisons, is simplified first,
  /// the same way `RelBuilder#filter` does it. Field trimming usually did this already when it rebuilt the filter,
  /// but not when the filter keeps all its fields. This folds `x = 1 OR x = 2 OR ...` into a Sarg, and it folds the
  /// other predicates on the same operand into the Sarg (for example `x IS NOT NULL` into `NULL AS FALSE`), exactly
  /// as the optimizer would do without sealing. Once sealed, such a predicate cannot merge anymore, and Calcite could
  /// drop an `x IS NOT NULL` next to it as redundant.
  public RelNode seal(RelNode rel) {
    return isEnabled() ? rel.accept(new RelSealer()) : rel;
  }

  /// Restores the `SEARCH` calls that this instance sealed.
  public RelNode unseal(RelNode rel) {
    return _operators.isEmpty() ? rel : rel.accept(new RelUnsealer());
  }

  /// Returns the `SEARCH` call that a sealed call stands for.
  static RexNode unsealCall(RexBuilder rexBuilder, RexCall sealed) {
    PinotSealedSearchOperator operator = (PinotSealedSearchOperator) sealed.getOperator();
    return rexBuilder.makeCall(sealed.getType(), SqlStdOperatorTable.SEARCH,
        List.of(sealed.getOperands().get(0), operator.getSargLiteral()));
  }

  private final class RelSealer extends RelHomogeneousShuttle {
    @Override
    public RelNode visit(RelNode other) {
      RelNode rel = foldLargeLists(super.visit(other));
      return rel.accept(new RexSealer(rel.getCluster().getRexBuilder()));
    }

    private RelNode foldLargeLists(RelNode rel) {
      if (rel instanceof Filter) {
        Filter filter = (Filter) rel;
        if (hasLargeList(filter.getCondition())) {
          return filter.copy(filter.getTraitSet(), filter.getInput(),
              simplifyCondition(filter.getCluster().getRexBuilder(), filter.getCondition()));
        }
      } else if (rel instanceof Join) {
        Join join = (Join) rel;
        if (join.getJoinType() != JoinRelType.LEFT_MARK && hasLargeList(join.getCondition())) {
          return join.copy(join.getTraitSet(), simplifyCondition(join.getCluster().getRexBuilder(),
              join.getCondition()), join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone());
        }
      }
      return rel;
    }
  }

  /// Returns whether a condition has a `SEARCH` that is large enough to seal, or an AND or OR with so many comparisons
  /// to literals that Calcite could fold them into such a Sarg. An AND of `x <> v` comparisons folds into `n + 1`
  /// ranges.
  private boolean hasLargeList(RexNode condition) {
    int minComparisons = Math.max(_threshold - 1, 2);
    return Boolean.TRUE.equals(condition.accept(new RexVisitorImpl<Boolean>(true) {
      @Override
      public Boolean visitCall(RexCall call) {
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SEARCH) {
          Sarg<?> sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
          if (sarg != null && shouldSeal(sarg)) {
            return true;
          }
        }
        if ((kind == SqlKind.AND || kind == SqlKind.OR) && call.getOperands().size() >= minComparisons) {
          int comparisons = 0;
          for (RexNode operand : call.getOperands()) {
            if (isLiteralComparison(operand) && ++comparisons >= minComparisons) {
              return true;
            }
          }
        }
        for (RexNode operand : call.getOperands()) {
          if (Boolean.TRUE.equals(operand.accept(this))) {
            return true;
          }
        }
        return false;
      }
    }));
  }

  private static boolean isLiteralComparison(RexNode node) {
    if (!node.isA(SqlKind.COMPARISON) || !(node instanceof RexCall)) {
      return false;
    }
    List<RexNode> operands = ((RexCall) node).getOperands();
    return operands.size() == 2 && (operands.get(0) instanceof RexLiteral || operands.get(1) instanceof RexLiteral);
  }

  /// Simplifies a filter or join condition like `RelBuilder#filter` does. Unlike `RexSimplify#simplifyUnknownAsFalse`,
  /// this does not simplify each OR term against the other terms, which is quadratic in the number of terms.
  private static RexNode simplifyCondition(RexBuilder rexBuilder, RexNode condition) {
    RexNode simplified = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, PinotRexExecutor.INSTANCE)
        .simplifyFilterPredicates(List.of(condition));
    return simplified != null ? simplified : rexBuilder.makeLiteral(false);
  }

  private final class RexSealer extends RexShuttle {
    private final RexBuilder _rexBuilder;

    RexSealer(RexBuilder rexBuilder) {
      _rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexNode visited = super.visitCall(call);
      if (visited.getKind() == SqlKind.SEARCH) {
        return seal(_rexBuilder, (RexCall) visited);
      }
      return visited;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RexSubQuery visited = (RexSubQuery) super.visitSubQuery(subQuery);
      RelNode rel = visited.rel.accept(new RelSealer());
      return rel == visited.rel ? visited : visited.clone(rel);
    }
  }

  private static final class RelUnsealer extends RelHomogeneousShuttle {
    @Override
    public RelNode visit(RelNode other) {
      RelNode rel = super.visit(other);
      rel = rel.accept(new RexUnsealer(rel.getCluster().getRexBuilder()));
      // Un-sealing runs after the trait program: a copy must keep the traits of the original, for example its
      // distribution.
      return rel == other ? other : PinotRuleUtils.withTraits(rel, other.getTraitSet());
    }
  }


  private static final class RexUnsealer extends RexShuttle {
    private final RexBuilder _rexBuilder;

    RexUnsealer(RexBuilder rexBuilder) {
      _rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getKind() == SqlKind.NOT && isSealed(call.getOperands().get(0))) {
        // RexSimplify pushes NOT into a SEARCH by negating its Sarg, which it could not do while the Sarg was sealed.
        RexCall sealed = (RexCall) call.getOperands().get(0);
        PinotSealedSearchOperator operator = (PinotSealedSearchOperator) sealed.getOperator();
        RexLiteral negated =
            _rexBuilder.makeSearchArgumentLiteral(operator.getSarg().negate(), operator.getSargLiteral().getType());
        return _rexBuilder.makeCall(call.getType(), SqlStdOperatorTable.SEARCH,
            List.of(sealed.getOperands().get(0).accept(this), negated));
      }
      RexNode visited = super.visitCall(call);
      return isSealed(visited) ? unsealCall(_rexBuilder, (RexCall) visited) : visited;
    }

    private boolean isSealed(RexNode node) {
      return node instanceof RexCall && ((RexCall) node).getOperator() instanceof PinotSealedSearchOperator;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RexSubQuery visited = (RexSubQuery) super.visitSubQuery(subQuery);
      RelNode rel = visited.rel.accept(new RelUnsealer());
      return rel == visited.rel ? visited : visited.clone(rel);
    }
  }
}
