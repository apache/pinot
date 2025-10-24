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
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

/// Rewrites certain aggregation functions based on operand types to support polymorphic aggregations.
///
/// Currently supported rewrites:
/// - MIN(stringType) -> MINSTRING
/// - MAX(stringType) -> MAXSTRING
/// - MIN(longType) -> MINLONG
/// - MAX(longType) -> MAXLONG
/// - SUM(longType) -> SUMLONG
/// - SUM(intType) -> SUMINT
public class AggregateFunctionRewriteOptimizer implements StatementOptimizer {

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

    Expression operand = function.getOperands().get(0);
    FieldSpec.DataType operandType;
    // TODO: Handle more complex expressions (e.g. MIN(trim(stringCol)) )
    if (operand.isSetIdentifier()) {
      String columnName = operand.getIdentifier().getName();
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      if (fieldSpec == null) {
        return;
      }
      operandType = fieldSpec.getDataType().getStoredType();
    } else {
      return;
    }

    // Rewrite MIN(stringCol) and MAX(stringCol) to MINSTRING / MAXSTRING
    // Rewrite MIN(longCol) and MAX(longCol) to MINLONG / MAXLONG
    if ((functionName.equals(AggregationFunctionType.MIN.getName())
        || functionName.equals(AggregationFunctionType.MAX.getName()))
        && function.getOperandsSize() == 1) {
      if (operandType == FieldSpec.DataType.STRING) {
        String newFunctionName =
            functionName.equals(AggregationFunctionType.MIN.getName())
                ? AggregationFunctionType.MINSTRING.name().toLowerCase()
                : AggregationFunctionType.MAXSTRING.name().toLowerCase();
        function.setOperator(newFunctionName);
      }
      if (operandType == FieldSpec.DataType.LONG) {
        String newFunctionName =
            functionName.equals(AggregationFunctionType.MIN.getName())
                ? AggregationFunctionType.MINLONG.name().toLowerCase()
                : AggregationFunctionType.MAXLONG.name().toLowerCase();
        function.setOperator(newFunctionName);
      }
    }

    // Rewrite SUM(intCol) and SUM(longCol) to SUMINT / SUMLONG
    if (functionName.equals(AggregationFunctionType.SUM.getName())) {
      if (operandType == FieldSpec.DataType.INT) {
        function.setOperator(AggregationFunctionType.SUMINT.name().toLowerCase());
      }
      if (operandType == FieldSpec.DataType.LONG) {
        function.setOperator(AggregationFunctionType.SUMLONG.name().toLowerCase());
      }
    }
  }
}
