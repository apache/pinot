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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// Adds an inferred type argument to string and timestamp MODE expressions
/// so the result type is fixed before execution.
/// This also supplies the broker with the correct result type when no rows match or groups are trimmed before
/// finalization. Type inference reads schema and function metadata only: server-dependent transforms such as LOOKUP
/// must not be initialized on the broker. Expressions whose type is unknown retain the legacy MODE implementation.
public class ModeAggregationFunctionRewriteOptimizer implements StatementOptimizer {
  @Override
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    if (schema == null) {
      return;
    }
    rewriteExpressions(pinotQuery.getSelectList(), schema);
    rewriteExpressions(pinotQuery.getGroupByList(), schema);
    rewriteExpressions(pinotQuery.getOrderByList(), schema);
    rewriteExpression(pinotQuery.getFilterExpression(), schema);
    rewriteExpression(pinotQuery.getHavingExpression(), schema);
  }

  private static void rewriteExpressions(@Nullable List<Expression> expressions, Schema schema) {
    if (expressions != null) {
      for (Expression expression : expressions) {
        rewriteExpression(expression, schema);
      }
    }
  }

  private static void rewriteExpression(@Nullable Expression expression, Schema schema) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }
    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    rewriteExpressions(operands, schema);
    if (!AggregationFunctionType.MODE.getName().equalsIgnoreCase(function.getOperator()) || operands.isEmpty()
        || operands.size() >= 3) {
      return;
    }

    ColumnDataType operandType = getOperandType(operands.get(0), schema);
    if (operandType == ColumnDataType.STRING || operandType == ColumnDataType.TIMESTAMP) {
      List<Expression> typedOperands = new ArrayList<>(operands);
      if (typedOperands.size() == 1) {
        typedOperands.add(RequestUtils.getLiteralExpression("MIN"));
      }
      typedOperands.add(RequestUtils.getLiteralExpression(operandType.name()));
      function.setOperands(typedOperands);
    }
  }

  @Nullable
  private static ColumnDataType getOperandType(Expression operand, Schema schema) {
    if (operand.isSetIdentifier()) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(operand.getIdentifier().getName());
      return fieldSpec != null
          ? ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField())
          : null;
    }
    if (operand.isSetLiteral()) {
      return RequestUtils.getLiteralTypeAndValue(operand.getLiteral()).getLeft();
    }
    if (!operand.isSetFunctionCall()) {
      return null;
    }
    Function function = operand.getFunctionCall();
    List<Expression> arguments = function.getOperands();
    String name = FunctionRegistry.canonicalize(function.getOperator());
    switch (name) {
      case "cast":
        return literalType(arguments, 1);
      case "jsonextractscalar":
      case "jsonextractscalarfast":
      case "jsonextractscalarfirstmatch":
      case "jsonextractscalarfory":
        return literalType(arguments, 2);
      case "case":
        // CASE stores alternating condition/result pairs followed by an optional ELSE result.
        ColumnDataType resultType = ColumnDataType.UNKNOWN;
        for (int i = 1; i < arguments.size(); i += 2) {
          resultType = commonType(resultType, getOperandType(arguments.get(i), schema));
        }
        if (arguments.size() % 2 == 1) {
          resultType = commonType(resultType, getOperandType(arguments.get(arguments.size() - 1), schema));
        }
        return resultType;
      case "datetimeconvert":
        if (arguments.size() < 3 || !arguments.get(2).isSetLiteral()
            || !arguments.get(2).getLiteral().isSetStringValue()) {
          return null;
        }
        DateTimeFieldSpec.TimeFormat format =
            new DateTimeFormatSpec(arguments.get(2).getLiteral().getStringValue()).getTimeFormat();
        return format == DateTimeFieldSpec.TimeFormat.EPOCH || format == DateTimeFieldSpec.TimeFormat.TIMESTAMP
            ? ColumnDataType.LONG
            : ColumnDataType.STRING;
      default:
        ColumnDataType[] argumentTypes = new ColumnDataType[arguments.size()];
        for (int i = 0; i < arguments.size(); i++) {
          argumentTypes[i] = getOperandType(arguments.get(i), schema);
          if (argumentTypes[i] == null) {
            return null;
          }
        }
        FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(name, argumentTypes);
        return functionInfo != null ? FunctionUtils.getColumnDataType(functionInfo.getMethod().getReturnType()) : null;
    }
  }

  @Nullable
  private static ColumnDataType commonType(@Nullable ColumnDataType left, @Nullable ColumnDataType right) {
    if (left == null || right == null) {
      return null;
    }
    if (left == ColumnDataType.UNKNOWN) {
      return right;
    }
    return right == ColumnDataType.UNKNOWN || left == right ? left : null;
  }

  @Nullable
  private static ColumnDataType literalType(List<Expression> arguments, int position) {
    if (arguments.size() <= position || !arguments.get(position).isSetLiteral()
        || !arguments.get(position).getLiteral().isSetStringValue()) {
      return null;
    }
    String type = arguments.get(position).getLiteral().getStringValue().toUpperCase(Locale.ROOT);
    return switch (type) {
      case "VARCHAR", "CHAR", "JSON" -> ColumnDataType.STRING;
      case "BIGINT" -> ColumnDataType.LONG;
      case "INTEGER" -> ColumnDataType.INT;
      case "REAL" -> ColumnDataType.FLOAT;
      case "DECIMAL" -> ColumnDataType.BIG_DECIMAL;
      default -> {
        try {
          yield ColumnDataType.valueOf(type);
        } catch (IllegalArgumentException e) {
          yield null;
        }
      }
    };
  }
}
