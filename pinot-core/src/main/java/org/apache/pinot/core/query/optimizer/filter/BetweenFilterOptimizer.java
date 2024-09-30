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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * Rewrite a BETWEEN filter predicate to an AND of the equivalent GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL filter
 * predicates. This is done to make it easier to apply later range related optimizations (like in the
 * NumericalFilterOptimizer) that might not be possible to do directly on BETWEEN.
 * <p>
 * Ideally, this would be done in PredicateComparisonRewriter (that would also allow for things like
 * 'col1 BETWEEN col2 AND col3' which isn't currently supported), but we don't have access to the schema there, and
 * we cannot apply this rewrite for MV columns since that changes the semantics of the filter predicate.
 */
public class BetweenFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    if (schema == null || filterExpression.getType() != ExpressionType.FUNCTION) {
      return filterExpression;
    }

    Function function = filterExpression.getFunctionCall();
    String operator = function.getOperator();
    if (operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.OR.name()) || operator.equals(
        FilterKind.NOT.name())) {
      function.getOperands().replaceAll(expression -> optimize(expression, schema));
    } else if (operator.equals(FilterKind.BETWEEN.name())) {
      List<Expression> operands = function.getOperands();
      Expression op1 = operands.get(0);
      Expression op2 = operands.get(1);
      Expression op3 = operands.get(2);

      // Pinot currently only supports BETWEEN filter predicates where the first operand is a non-literal and the
      // second and third operands are literals. The PredicateComparisonRewriter will throw an error during query
      // compilation if the second or third argument is not a literal. If the first argument is also a literal, the
      // CompileTimeFunctionsInvoker should reduce that altogether. So, we can safely assume that the first operand
      // is not a literal, but we still handle this just in case.
      if (op1.isSetLiteral()) {
        return filterExpression;
      }

      // Don't apply this optimization to MV columns since that changes the semantics of the filter predicate
      if (op1.isSetIdentifier() && !schema.getFieldSpecFor(op1.getIdentifier().getName()).isSingleValueField()) {
        return filterExpression;
      }

      Expression greaterThanEqualExpression =
          RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN_OR_EQUAL.name(), op1, op2);
      Expression lessThanEqualExpression =
          RequestUtils.getFunctionExpression(FilterKind.LESS_THAN_OR_EQUAL.name(), op1, op3);

      return RequestUtils.getFunctionExpression(FilterKind.AND.name(), greaterThanEqualExpression,
          lessThanEqualExpression);
    }

    return filterExpression;
  }
}
