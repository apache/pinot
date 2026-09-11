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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;


/// Binds inferred arguments declared by aggregation functions and optionally rewrites type-specific variants.
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

    boolean autoRewrite = pinotQuery.getQueryOptions() != null && Boolean.parseBoolean(pinotQuery.getQueryOptions().get(
        CommonConstants.Broker.Request.QueryOptionKey.AUTO_REWRITE_AGGREGATION_TYPE));

    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList != null) {
      for (Expression expression : selectList) {
        maybeRewriteAggregateFunction(expression, schema, autoRewrite);
      }
    }

    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        maybeRewriteAggregateFunction(expression, schema, autoRewrite);
      }
    }

    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        maybeRewriteAggregateFunction(expression, schema, autoRewrite);
      }
    }

    maybeRewriteAggregateFunction(pinotQuery.getFilterExpression(), schema, autoRewrite);
    maybeRewriteAggregateFunction(pinotQuery.getHavingExpression(), schema, autoRewrite);
  }

  private void maybeRewriteAggregateFunction(@Nullable Expression expression, Schema schema, boolean autoRewrite) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }

    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    for (Expression operand : operands) {
      // Infer arguments within scalar expressions while preserving the existing top-level variant rewrites.
      maybeRewriteAggregateFunction(operand, schema, false);
    }
    String functionName = function.getOperator();
    if (!AggregationFunctionType.isAggregationFunction(functionName)) {
      return;
    }

    List<String> inferredArguments = AggregationFunctionType.getAggregationFunctionType(functionName)
        .inferArguments(operands.size(), i -> {
          ColumnDataType type = getOperandType(operands.get(i), schema);
          if (type == null || type.isArray() || type == ColumnDataType.OBJECT) {
            return DataType.UNKNOWN;
          }
          return type == ColumnDataType.MAP ? DataType.MAP : type.toDataType();
        });
    if (!inferredArguments.isEmpty()) {
      List<Expression> typedOperands = new ArrayList<>(operands);
      for (String argument : inferredArguments) {
        typedOperands.add(RequestUtils.getLiteralExpression(argument));
      }
      function.setOperands(typedOperands);
    }
    if (!autoRewrite || operands.isEmpty()) {
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
