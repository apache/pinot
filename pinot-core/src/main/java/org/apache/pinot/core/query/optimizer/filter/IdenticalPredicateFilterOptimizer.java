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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * This optimizer folds predicates that compare an expression to itself to a constant TRUE/FALSE literal, so the engine
 * does not evaluate them per-row. Such predicates are not typical to write by hand (e.g. {@code WHERE col1 = col1}),
 * but they are also produced internally and would otherwise be expensive.
 *
 * <p>{@code PredicateComparisonRewriter} rewrites a column-to-column comparison {@code a <op> b} into
 * {@code comparison(a, b) = true} (e.g. {@code equals(a, b) = true}). When the two operands are the same expression,
 * the inner comparison has a constant value, which this optimizer substitutes: {@code true} for {@code =}, {@code >=},
 * {@code <=} and {@code false} for {@code !=}, {@code >}, {@code <}.
 */
public class IdenticalPredicateFilterOptimizer extends BaseAndOrBooleanFilterOptimizer {

  // Comparison operators that are always true / always false when their two operands are identical.
  private static final Set<String> TRUE_FOR_IDENTICAL_OPERANDS = Set.of(
      TransformFunctionType.EQUALS.getName(),
      TransformFunctionType.GREATER_THAN_OR_EQUAL.getName(),
      TransformFunctionType.LESS_THAN_OR_EQUAL.getName());
  private static final Set<String> FALSE_FOR_IDENTICAL_OPERANDS = Set.of(
      TransformFunctionType.NOT_EQUALS.getName(),
      TransformFunctionType.GREATER_THAN.getName(),
      TransformFunctionType.LESS_THAN.getName());

  @Override
  boolean canBeOptimized(Expression filterExpression, @Nullable Schema schema) {
    // if there's no function call, there's no lhs or rhs
    return filterExpression.getFunctionCall() != null;
  }

  @Override
  Expression optimizeChild(Expression filterExpression, @Nullable Schema schema) {
    Function function = filterExpression.getFunctionCall();
    if (FilterKind.valueOf(function.getOperator()) == FilterKind.EQUALS) {
      Optional<Boolean> folded = foldIdenticalComparisonWithTrueLiteral(function.getOperands());
      if (folded.isPresent()) {
        return getExpressionFromBoolean(folded.get());
      }
    }
    return filterExpression;
  }

  /**
   * Folds a predicate of the form 'comparison(a, a) = true' — a comparison whose two operands are the <em>same</em>
   * expression — to the constant value it must always have. Returns an empty {@link Optional} when the predicate is
   * not such a comparison: the operands differ, the right-hand side is not the literal {@code true}, or the function
   * is a non-comparison like 'startsWith(a, a) = true' whose value cannot be determined here.
   */
  private Optional<Boolean> foldIdenticalComparisonWithTrueLiteral(List<Expression> operands) {
    // The predicate must be 'comparison(a, b) = true'.
    if (operands.size() != 2 || operands.get(0).getFunctionCall() == null || !isLiteralTrue(operands.get(1))) {
      return Optional.empty();
    }
    // The two compared operands must be the same expression, i.e. 'a <op> a'.
    List<Expression> comparisonOperands = operands.get(0).getFunctionCall().getOperands();
    if (comparisonOperands.size() != 2 || comparisonOperands.get(0) == null
        || !comparisonOperands.get(0).equals(comparisonOperands.get(1))) {
      return Optional.empty();
    }
    // 'a <op> a' is constant for comparison operators: true for =, >=, <=; false for !=, >, <.
    String operator = operands.get(0).getFunctionCall().getOperator();
    if (TRUE_FOR_IDENTICAL_OPERANDS.contains(operator)) {
      return Optional.of(Boolean.TRUE);
    }
    if (FALSE_FOR_IDENTICAL_OPERANDS.contains(operator)) {
      return Optional.of(Boolean.FALSE);
    }
    return Optional.empty();
  }

  private boolean isLiteralTrue(Expression expression) {
    Literal literal = expression.getLiteral();
    return literal != null && literal.isSetBoolValue() && literal.getBoolValue();
  }
}
