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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code DistributeConjunctsIntoOrFilterOptimizer} distributes selective conjuncts from an enclosing AND
 * into each branch of any OR child. This prevents the OR subtree from being evaluated independently of the
 * enclosing AND's selective predicates.
 * <p>
 * For example:
 * <pre>
 *   P AND (A OR B)  →  P AND ((P AND A) OR (P AND B))
 * </pre>
 * This rewrite is sound because in three-valued logic, {@code P ∧ (A ∨ B) ≡ P ∧ ((P ∧ A) ∨ (P ∧ B))}
 * when the filter only passes TRUE (SQL WHERE semantics).
 * <p>
 * <b>Why this is needed:</b> When an AND node has an OR child, the OR subtree can eagerly materialize
 * index-based children over the entire segment, ignoring the enclosing AND's selective predicates.
 * This causes expensive scan-based predicates (e.g. IN_SUBQUERY evaluated via ExpressionScanDocIdIterator)
 * to be evaluated for every document in the segment, not just those matching the selective predicate.
 * <p>
 * <b>Which predicates are distributed:</b> Only {@link FilterKind#EQUALS} and {@link FilterKind#IN}
 * predicates on single columns (the LHS is an IDENTIFIER). These are the predicates most commonly
 * backed by inverted/dictionary indexes in Pinot. Duplicating them into OR branches is cheap because
 * they use index-based access, and the restriction dramatically reduces the work of scan-based predicates
 * inside the OR branches.
 * <p>
 * NOTE: This optimizer should run after the merge optimizers ({@link MergeEqInFilterOptimizer},
 * {@link MergeRangeFilterOptimizer}) so that merge optimizations are applied to the original structure
 * before the distribution.
 *
 * @see <a href="https://github.com/apache/pinot/issues/19339">Issue #19339</a>
 */
public class DistributeConjunctsIntoOrFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return optimize(filterExpression);
  }

  private Expression optimize(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return expression;
    }
    String operator = function.getOperator();

    // First, recursively optimize children (post-order: children → parent)
    List<Expression> operands = function.getOperands();
    if (operands != null) {
      for (int i = 0; i < operands.size(); i++) {
        operands.set(i, optimize(operands.get(i)));
      }
    }

    // Only distribute from AND nodes
    if (FilterKind.AND.name().equals(operator)) {
      return distribute(expression);
    }
    return expression;
  }

  /**
   * Distributes selective conjuncts from the enclosing AND into each OR branch.
   * <p>
   * For an AND expression like {@code AND(P, Q, OR(A, B), R)}, this method:
   * <ol>
   *   <li>Collects distributable conjuncts (P, Q) — simple EQ/IN predicates on single columns</li>
   *   <li>For the OR child {@code OR(A, B)}, wraps each branch: {@code OR(AND(P, Q, A), AND(P, Q, B))}</li>
   *   <li>Rebuilds the outer AND as {@code AND(P, Q, R, OR(AND(P, Q, A), AND(P, Q, B)))}</li>
   * </ol>
   */
  private Expression distribute(Expression andExpression) {
    Function function = andExpression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    assert operands != null;

    // Collect OR children and distributable conjuncts
    List<Expression> distributableConjuncts = new ArrayList<>();
    List<Integer> orChildIndexes = new ArrayList<>();

    for (int i = 0; i < operands.size(); i++) {
      Expression operand = operands.get(i);
      Function operandFunction = operand.getFunctionCall();
      if (operandFunction != null && FilterKind.OR.name().equals(operandFunction.getOperator())) {
        orChildIndexes.add(i);
      } else if (isDistributable(operand)) {
        distributableConjuncts.add(operand);
      }
    }

    // Nothing to optimize if there are no OR children or no distributable conjuncts
    if (orChildIndexes.isEmpty() || distributableConjuncts.isEmpty()) {
      return andExpression;
    }

    // Rebuild the AND's operands, replacing OR children with the distributed versions
    List<Expression> newOperands = new ArrayList<>(operands.size());
    for (int i = 0; i < operands.size(); i++) {
      if (orChildIndexes.contains(i)) {
        // Distribute conjuncts into each branch of this OR
        newOperands.add(distributeIntoOr(operands.get(i), distributableConjuncts));
      } else {
        newOperands.add(operands.get(i));
      }
    }
    function.setOperands(newOperands);
    return andExpression;
  }

  /**
   * Distributes the given conjuncts into each branch of the OR expression.
   * <p>
   * {@code OR(A, B)} with conjuncts [P, Q] → {@code OR(AND(P, Q, A), AND(P, Q, B))}
   * <p>
   * If a branch or one of its AND operands is already identical to a conjunct, that conjunct is not
   * duplicated into the branch (this avoids creating no-op AND(P, P) nodes and keeps the filter small).
   */
  private Expression distributeIntoOr(Expression orExpression, List<Expression> conjuncts) {
    Function orFunction = orExpression.getFunctionCall();
    List<Expression> branches = orFunction.getOperands();
    assert branches != null;

    List<Expression> newBranches = new ArrayList<>(branches.size());
    for (Expression branch : branches) {
      // Build AND(conjuncts..., branch). Skip conjuncts already present in the branch.
      List<Expression> andOperands = new ArrayList<>(conjuncts.size() + 1);
      for (Expression conjunct : conjuncts) {
        if (!containsExpression(branch, conjunct)) {
          andOperands.add(conjunct);
        }
      }
      if (andOperands.isEmpty()) {
        // All conjuncts are already present in the branch; keep the branch as-is
        newBranches.add(branch);
      } else {
        andOperands.add(branch);
        // Avoid wrapping a single remaining expression in AND
        if (andOperands.size() == 1) {
          newBranches.add(andOperands.get(0));
        } else {
          newBranches.add(RequestUtils.getFunctionExpression(FilterKind.AND.name(), andOperands));
        }
      }
    }
    return RequestUtils.getFunctionExpression(FilterKind.OR.name(), newBranches);
  }

  /**
   * Returns true if the expression is a leaf predicate that can be safely distributed into OR branches.
   * <p>
   * A predicate is distributable if:
   * <ul>
   *   <li>It is a function call with operator EQUALS or IN</li>
   *   <li>The LHS (first operand) is an IDENTIFIER column reference</li>
   *   <li>For IN predicates, all value operands are LITERALs (not subqueries)</li>
   * </ul>
   * These predicates are typically backed by dictionary/inverted indexes in Pinot, making them
   * cheap to evaluate even when duplicated into multiple OR branches. The restriction they provide
   * dramatically reduces the work of scan-based predicates (like IN_SUBQUERY) inside the OR branches.
   */
  private static boolean isDistributable(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return false;
    }
    String operator = function.getOperator();
    List<Expression> operands = function.getOperands();
    if (operands == null || operands.isEmpty()) {
      return false;
    }

    // Only EQUALS and IN predicates
    if (!FilterKind.EQUALS.name().equals(operator) && !FilterKind.IN.name().equals(operator)) {
      return false;
    }

    // LHS must be a column identifier
    Expression lhs = operands.get(0);
    if (lhs.getType() != ExpressionType.IDENTIFIER) {
      return false;
    }

    // For IN predicates, all value operands must be literals (not subqueries)
    if (FilterKind.IN.name().equals(operator)) {
      for (int i = 1; i < operands.size(); i++) {
        if (operands.get(i).getType() != ExpressionType.LITERAL) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Returns true if the given expression structurally contains the target expression.
   * <p>
   * Uses the thrift-generated structural {@code equals()} (the same equality the existing optimizers
   * rely on, e.g. {@link BaseAndOrBooleanFilterOptimizer#TRUE}). For AND expressions, recursively
   * checks each operand.
   */
  private static boolean containsExpression(Expression expression, Expression target) {
    if (expression.equals(target)) {
      return true;
    }
    Function function = expression.getFunctionCall();
    if (function == null) {
      return false;
    }
    if (FilterKind.AND.name().equals(function.getOperator())) {
      List<Expression> operands = function.getOperands();
      if (operands != null) {
        for (Expression operand : operands) {
          if (containsExpression(operand, target)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}