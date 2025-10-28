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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.calcite.function.SseExpressionTypeInference;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rewrites aggregate functions to type-specific versions in order to support polymorphic functions.
 */
public class AggregateFunctionRewriteOptimizer implements StatementOptimizer {

  public static final Logger LOGGER = LoggerFactory.getLogger(AggregateFunctionRewriteOptimizer.class);

  @Override
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    if (schema == null) {
      return;
    }

    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList != null) {
      for (Expression expression : selectList) {
        maybeRewriteAggregateFunction(expression, schema);
      }
    }

    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        maybeRewriteAggregateFunction(expression, schema);
      }
    }

    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        maybeRewriteAggregateFunction(expression, schema);
      }
    }

    maybeRewriteAggregateFunction(pinotQuery.getFilterExpression(), schema);
    maybeRewriteAggregateFunction(pinotQuery.getHavingExpression(), schema);
  }

  private void maybeRewriteAggregateFunction(@Nullable Expression expression, Schema schema) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }

    Function function = expression.getFunctionCall();
    String functionName = function.getOperator();
    if (!AggregationFunctionType.isAggregationFunction(functionName)) {
      return;
    }

    // Rewrite MIN(stringVal) and MAX(stringVal) to MINSTRING / MAXSTRING
    if ((functionName.equals(AggregationFunctionType.MIN.getName())
        || functionName.equals(AggregationFunctionType.MAX.getName()))
        && function.getOperandsSize() == 1) {
      Expression operand = function.getOperands().get(0);

      ColumnDataType dataType;
      try {
        dataType = SseExpressionTypeInference.inferReturnRelType(operand, schema);
      } catch (Exception e) {
        // Ignore exceptions during type inference and do not rewrite the function
        LOGGER.warn("Exception while inferring return type for expression: {}", operand, e);
        return;
      }
      if (dataType.getStoredType() == ColumnDataType.STRING) {
        String newFunctionName =
            functionName.equals(AggregationFunctionType.MIN.getName())
                ? AggregationFunctionType.MINSTRING.name().toLowerCase()
                : AggregationFunctionType.MAXSTRING.name().toLowerCase();
        function.setOperator(newFunctionName);
      }
    }
  }
}
