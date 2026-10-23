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
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants;


/// Rewrites certain aggregation functions based on operand types to support polymorphic aggregations.
///
/// Always applied:
/// - FIRST_WITH_TIME(dataColumn, timeColumn) -> FIRST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - LAST_WITH_TIME(dataColumn, timeColumn) -> LAST_WITH_TIME(dataColumn, timeColumn, 'dataType')
///
/// Applied only when enabled through the `autoRewriteAggregationType` query option:
/// - MIN(stringType) -> MINSTRING
/// - MAX(stringType) -> MAXSTRING
/// - MIN(longType) -> MINLONG
/// - MAX(longType) -> MAXLONG
/// - SUM(longType) -> SUMLONG
/// - SUM(intType) -> SUMINT
///
/// Aggregations nested in other expressions, such as an alias, a FILTER clause or a transform, are rewritten as well.
public class AggregateFunctionRewriteOptimizer implements StatementOptimizer {

  @Override
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    if (schema == null) {
      return;
    }

    boolean rewriteToTypedVariants = pinotQuery.getQueryOptions() != null && Boolean.parseBoolean(
        pinotQuery.getQueryOptions().get(CommonConstants.Broker.Request.QueryOptionKey.AUTO_REWRITE_AGGREGATION_TYPE));
    rewrite(pinotQuery.getSelectList(), schema, rewriteToTypedVariants);
    rewrite(pinotQuery.getGroupByList(), schema, rewriteToTypedVariants);
    rewrite(pinotQuery.getOrderByList(), schema, rewriteToTypedVariants);
    rewrite(pinotQuery.getFilterExpression(), schema, rewriteToTypedVariants);
    rewrite(pinotQuery.getHavingExpression(), schema, rewriteToTypedVariants);
  }

  private static void rewrite(@Nullable List<Expression> expressions, Schema schema, boolean rewriteToTypedVariants) {
    if (expressions != null) {
      for (Expression expression : expressions) {
        rewrite(expression, schema, rewriteToTypedVariants);
      }
    }
  }

  private static void rewrite(@Nullable Expression expression, Schema schema, boolean rewriteToTypedVariants) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }

    Function function = expression.getFunctionCall();
    String functionName = function.getOperator();
    if (!AggregationFunctionType.isAggregationFunction(functionName)) {
      rewrite(function.getOperands(), schema, rewriteToTypedVariants);
      return;
    }

    // TODO: Handle more complex expressions (e.g. MIN(trim(stringCol)) )
    if (function.getOperandsSize() == 0 || !function.getOperands().get(0).isSetIdentifier()) {
      return;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(function.getOperands().get(0).getIdentifier().getName());
    if (fieldSpec == null) {
      return;
    }

    String normalizedName = AggregationFunctionType.getNormalizedAggregationFunctionName(functionName);
    if ((normalizedName.equals(AggregationFunctionType.FIRSTWITHTIME.name())
        || normalizedName.equals(AggregationFunctionType.LASTWITHTIME.name())) && function.getOperandsSize() == 2) {
      addDataTypeArgument(function, normalizedName, fieldSpec);
      return;
    }
    if (rewriteToTypedVariants) {
      // The typed variants follow the stored type (e.g. TIMESTAMP as LONG), while the data type argument above keeps
      // the logical type.
      rewriteToTypedVariant(function, functionName, fieldSpec.getDataType().getStoredType());
    }
  }

  /// Appends the data column's logical type. The supported types match `PinotAggregateTypeArgumentRewriteRule` in the
  /// multi-stage engine.
  private static void addDataTypeArgument(Function function, String functionName, FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      switch (fieldSpec.getDataType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BIG_DECIMAL:
        case STRING:
        case BOOLEAN:
        case TIMESTAMP:
        case BYTES:
        case UUID:
          function.addToOperands(RequestUtils.getLiteralExpression(fieldSpec.getDataType().name()));
          return;
        case JSON:
          // Same as the multi-stage engine, which types a JSON column as VARCHAR.
          function.addToOperands(RequestUtils.getLiteralExpression(FieldSpec.DataType.STRING.name()));
          return;
        default:
          break;
      }
    }
    throw new BadQueryRequestException(String.format("Unsupported data type for %s(dataColumn, timeColumn): %s%s",
        functionName, fieldSpec.isSingleValueField() ? "" : "multi-value ", fieldSpec.getDataType()));
  }

  private static void rewriteToTypedVariant(Function function, String functionName, FieldSpec.DataType operandType) {
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
