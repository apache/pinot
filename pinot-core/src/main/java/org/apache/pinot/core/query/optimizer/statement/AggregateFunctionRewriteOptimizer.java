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
package org.apache.pinot.core.query.optimizer.statement;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

/**
 * Rewrites aggregate functions to type-specific versions in order to support polymorphic functions.
 */
public class AggregateFunctionRewriteOptimizer implements StatementOptimizer {

  @Override
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    for (int i = 0; i < pinotQuery.getSelectListSize(); i++) {
      Expression expression = maybeRewriteAggregateFunction(pinotQuery.getSelectList().get(i), schema);
      pinotQuery.getSelectList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      Expression expression = maybeRewriteAggregateFunction(pinotQuery.getGroupByList().get(i), schema);
      pinotQuery.getGroupByList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      Expression expression = maybeRewriteAggregateFunction(pinotQuery.getOrderByList().get(i), schema);
      pinotQuery.getOrderByList().set(i, expression);
    }
    Expression filterExpression = maybeRewriteAggregateFunction(pinotQuery.getFilterExpression(), schema);
    pinotQuery.setFilterExpression(filterExpression);
    Expression havingExpression = maybeRewriteAggregateFunction(pinotQuery.getHavingExpression(), schema);
    pinotQuery.setHavingExpression(havingExpression);
  }

  private Expression maybeRewriteAggregateFunction(Expression expression, Schema schema) {
    if (expression == null || !expression.isSetFunctionCall() || !AggregationFunctionType.isAggregationFunction(
        expression.getFunctionCall().getOperator())) {
      return expression;
    }

    // Rewrite MIN(stringCol) and MAX(stringCol) to MINSTRING / MAXSTRING
    String functionName = expression.getFunctionCall().getOperator();
    if ((functionName.equalsIgnoreCase(AggregationFunctionType.MIN.name())
        || functionName.equalsIgnoreCase(AggregationFunctionType.MAX.name()))
        && expression.getFunctionCall().getOperandsSize() == 1) {
      Expression operand = expression.getFunctionCall().getOperands().get(0);
      if (operand.isSetIdentifier()) {
        String columnName = operand.getIdentifier().getName();
        if (schema != null) {
          FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
          if (fieldSpec != null && fieldSpec.getDataType() == FieldSpec.DataType.STRING) {
            String newFunctionName = functionName.equalsIgnoreCase(AggregationFunctionType.MIN.name())
                ? AggregationFunctionType.MINSTRING.name() : AggregationFunctionType.MAXSTRING.name();
            expression.getFunctionCall().setOperator(newFunctionName);
          }
        }
      }
    }

    return expression;
  }
}
