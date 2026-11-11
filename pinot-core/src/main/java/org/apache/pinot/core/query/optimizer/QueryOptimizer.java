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
package org.apache.pinot.core.query.optimizer;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.filter.ArrayContainsFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.FilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.FlattenAndOrFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.IdenticalPredicateFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.MergeEqInFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.MergeRangeFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.NumericalFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.TextMatchFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.TimePredicateFilterOptimizer;
import org.apache.pinot.core.query.optimizer.statement.AggregateFunctionRewriteOptimizer;
import org.apache.pinot.core.query.optimizer.statement.StatementOptimizer;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


public class QueryOptimizer {
  // DO NOT change the order of these optimizers.
  // - MergeEqInFilterOptimizer, MergeRangeFilterOptimizer, and TextMatchFilterOptimizer each rely on
  //   FlattenAndOrFilterOptimizer to flatten the AND/OR predicate so that the children are on the same level to
  //   be merged
  // - TimePredicateFilterOptimizer and MergeRangeFilterOptimizer relies on NumericalFilterOptimizer to convert the
  //   values to the proper format so that they can be properly parsed
  // - ArrayContainsFilterOptimizer runs before MergeEqInFilterOptimizer so that the EQUALS predicates it produces can
  //   be merged. It converts the literal to the column type itself because NumericalFilterOptimizer only handles
  //   single-value columns
  private static final List<FilterOptimizer> FILTER_OPTIMIZERS =
      List.of(new FlattenAndOrFilterOptimizer(), new ArrayContainsFilterOptimizer(),
          new IdenticalPredicateFilterOptimizer(), new MergeEqInFilterOptimizer(), new NumericalFilterOptimizer(),
          new TimePredicateFilterOptimizer(), new MergeRangeFilterOptimizer(), new TextMatchFilterOptimizer());

  private static final List<StatementOptimizer> STATEMENT_OPTIMIZERS =
      List.of(new AggregateFunctionRewriteOptimizer());

  /// Optimizes the given query.
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(optimizeFilter(filterExpression, schema));
    }

    // Run statement optimizer after filter has already been optimized.
    for (StatementOptimizer statementOptimizer : STATEMENT_OPTIMIZERS) {
      statementOptimizer.optimize(pinotQuery, schema);
    }
  }

  /// Optimizes the given filter expression with the filter optimizers that [#optimize(PinotQuery, Schema)] runs on the
  /// WHERE clause. The optimizers can modify the given expression, so pass a copy to keep the original.
  public Expression optimizeFilter(Expression filterExpression, @Nullable Schema schema) {
    for (FilterOptimizer filterOptimizer : FILTER_OPTIMIZERS) {
      filterExpression = filterOptimizer.optimize(filterExpression, schema);
    }
    return filterExpression;
  }

  /// Applies table expression overrides once, copying replacements so the cached map is not modified. The input
  /// expression can be modified while its operands are visited; pass a copy to preserve the original expression.
  public static Expression applyExpressionOverride(Expression expression,
      Map<Expression, Expression> expressionOverrideMap) {
    Expression override = expressionOverrideMap.get(expression);
    if (override != null) {
      return new Expression(override);
    }
    Function function = expression.getFunctionCall();
    if (function != null) {
      function.getOperands().replaceAll(operand -> applyExpressionOverride(operand, expressionOverrideMap));
    }
    return expression;
  }

  /// Applies table overrides to the value expressions of a filter, preserving its predicates and boolean structure.
  /// A whole-policy override can match this independent RLS tree but not the broker's flattened WHERE tree; applying
  /// it here could remove the RLS evaluation barrier. Predicate values and constants must also stay unchanged.
  public static Expression applyExpressionOverrideToFilterOperands(Expression filterExpression,
      Map<Expression, Expression> expressionOverrideMap) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression.getIdentifier() != null
          ? applyExpressionOverride(filterExpression, expressionOverrideMap) : filterExpression;
    }
    String operator = function.getOperator().toUpperCase(Locale.ROOT);
    List<Expression> operands = function.getOperands();
    if (operator.equals("ISTRUE")) {
      operands.set(0, applyExpressionOverrideToFilterOperands(operands.get(0), expressionOverrideMap));
    } else if (!EnumUtils.isValidEnum(FilterKind.class, operator)) {
      // A boolean-valued transform is an implicit equality predicate, with the transform as its value expression.
      return applyExpressionOverride(filterExpression, expressionOverrideMap);
    } else {
      switch (FilterKind.valueOf(operator)) {
        case AND, OR, NOT -> operands.replaceAll(
            operand -> applyExpressionOverrideToFilterOperands(operand, expressionOverrideMap));
        default -> operands.set(0, applyExpressionOverride(operands.get(0), expressionOverrideMap));
      }
    }
    return filterExpression;
  }
}
