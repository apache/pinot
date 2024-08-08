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
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * Given two column names 'strColumn1' and 'strColumn1', CalciteSqlParser.queryRewrite will turn WHERE and HAVING
 * expressions of form "strColumn1 <operator> strColumn2" into "MINUS(strColumn1,strColumn2) <operator> 0" regardless
 * of the column datatype. The resulting query will fail to evaluate since the MINUS operator does not work with the
 * STRING column type. This class rewrites expressions of form "MINUS(strColumn1,strColumn2) <operator> 0" to
 * "STRCMP(strColumn1, strColumn2) <operator> 0" to fix the issue.
 *
 * Currently, rewrite phase (see CalciteSqlParser.queryRewrite) does not have access to schema; hence, we need to again
 * rewrite MINUS(strColumn1, strColumn2) into STRCMP(strColumn1, strColumn2). At some point, we should merge query
 * rewrite phase with optimizer phase to avoid such issues altogether.
 */
public class StringPredicateFilterOptimizer implements StatementOptimizer {
  private static final String MINUS_OPERATOR_NAME = "minus";
  private static final String STRCMP_OPERATOR_NAME = "strcmp";

  @Override
  public void optimize(PinotQuery query, @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    if (schema == null) {
      return;
    }

    Expression filter = query.getFilterExpression();
    if (filter != null) {
      optimizeExpression(filter, schema);
    }

    Expression expression = query.getHavingExpression();
    if (expression != null) {
      optimizeExpression(expression, schema);
    }
  }

  /** Traverse an expression tree to replace MINUS function with STRCMP if function operands are STRING. */
  private static void optimizeExpression(Expression expression, Schema schema) {
    ExpressionType type = expression.getType();
    if (type != ExpressionType.FUNCTION) {
      // We have nothing to rewrite if expression is not a function.
      return;
    }

    Function function = expression.getFunctionCall();
    String operator = function.getOperator();
    List<Expression> operands = function.getOperands();
    if (operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.OR.name()) || operator.equals(
        FilterKind.NOT.name())) {
      for (Expression operand : operands) {
        optimizeExpression(operand, schema);
      }
    } else {
      replaceMinusWithCompareForStrings(operands.get(0), schema);
    }
  }

  /** Replace the operator of a MINUS function with COMPARE if both operands are STRING. */
  private static void replaceMinusWithCompareForStrings(Expression expression, Schema schema) {
    if (expression.getType() != ExpressionType.FUNCTION) {
      // We have nothing to rewrite if expression is not a function.
      return;
    }

    Function function = expression.getFunctionCall();
    String operator = function.getOperator();
    List<Expression> operands = function.getOperands();
    if (operator.equals(MINUS_OPERATOR_NAME) && operands.size() == 2 && isString(operands.get(0), schema) && isString(
        operands.get(1), schema)) {
      function.setOperator(STRCMP_OPERATOR_NAME);
    }
  }

  /** @return true if expression is STRING column or a function that outputs STRING. */
  private static boolean isString(Expression expression, Schema schema) {
    ExpressionType expressionType = expression.getType();

    if (expressionType == ExpressionType.IDENTIFIER) {
      // Check if this is a STRING column.
      String column = expression.getIdentifier().getName();
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      return fieldSpec != null && fieldSpec.getDataType() == FieldSpec.DataType.STRING;
    }

    if (expressionType == ExpressionType.FUNCTION) {
      // Check if the function returns STRING as output.
      Function function = expression.getFunctionCall();
      String canonicalName = FunctionRegistry.canonicalize(function.getOperator());
      FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, function.getOperands().size());
      return functionInfo != null && functionInfo.getMethod().getReturnType() == String.class;
    }

    return false;
  }
}
