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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.sql.FilterKind;


/// The `ArrayContainsFilterOptimizer` rewrites a boolean `arrayContains*` predicate on a multi-value column into the
/// equivalent `EQUALS` predicate, e.g. `arrayContainsString(mvCol, 'foo')` (parsed as
/// `arrayContainsString(mvCol, 'foo') = true`) becomes `mvCol = 'foo'`.
///
/// On a multi-value column, `EQUALS` matches a document when any of its values equals the literal, which is exactly
/// what `arrayContains*` evaluates. Unlike the scalar function (evaluated by scanning every document), the `EQUALS`
/// predicate can be served by the dictionary / inverted index, and can be used by the segment pruners that run after
/// the optimizer (on the broker for single-stage queries, on the server for both single-stage and multi-stage leaf
/// queries).
///
/// The rewrite is only applied when:
/// - The first argument is an identifier of a multi-value column in the schema whose stored type matches the function
///   parameter type (e.g. `arrayContainsInt` on an `INT` column)
/// - The second argument is a non-null literal that can be converted to the function parameter type. The literal is
///   converted with the same [PinotDataType] conversion the scalar function applies to its literal arguments (e.g.
///   `5.5` is truncated to `5`, `' 5'` is trimmed), and the converted value is used in the `EQUALS` predicate, so both
///   forms match the same values. If the conversion fails, the scalar function is kept.
/// - The predicate is `= true`. Negations (`NOT`, `= false`, `!= true`) are left untouched because `!=` on a
///   multi-value column means "some value does not match", which differs from "no value matches"
///
/// NOTE: The rewrite runs before broker-side expression overrides are applied, so an expression override keyed on an
///       `arrayContains*` predicate that gets rewritten no longer matches.
public class ArrayContainsFilterOptimizer implements FilterOptimizer {
  private static final Map<String, PinotDataType> FUNCTION_PARAMETER_TYPES =
      Map.of("arraycontainsint", PinotDataType.INT, "arraycontainsstring", PinotDataType.STRING);

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return schema != null ? rewrite(filterExpression, schema) : filterExpression;
  }

  private static Expression rewrite(Expression filterExpression, Schema schema) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression;
    }
    String operator = function.getOperator();
    if (operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.OR.name())) {
      function.getOperands().replaceAll(operand -> rewrite(operand, schema));
      return filterExpression;
    }
    if (operator.equals(FilterKind.EQUALS.name())) {
      Expression rewritten = rewriteArrayContains(function.getOperands(), schema);
      return rewritten != null ? rewritten : filterExpression;
    }
    // Do not descend into NOT (see class Javadoc)
    return filterExpression;
  }

  /// Returns the rewritten `EQUALS` predicate for `arrayContains*(column, literal) = true`, or `null` if the operands
  /// do not match that shape.
  @Nullable
  private static Expression rewriteArrayContains(List<Expression> equalsOperands, Schema schema) {
    Literal rhs = equalsOperands.get(1).getLiteral();
    if (rhs == null || !rhs.isSetBoolValue() || !rhs.getBoolValue()) {
      return null;
    }
    Function arrayContains = equalsOperands.get(0).getFunctionCall();
    if (arrayContains == null) {
      return null;
    }
    PinotDataType parameterType =
        FUNCTION_PARAMETER_TYPES.get(RequestUtils.canonicalizeFunctionName(arrayContains.getOperator()));
    if (parameterType == null) {
      return null;
    }
    List<Expression> arguments = arrayContains.getOperands();
    if (arguments.size() != 2) {
      return null;
    }
    Expression column = arguments.get(0);
    Literal literal = arguments.get(1).getLiteral();
    if (column.getIdentifier() == null || literal == null || literal.isSetNullValue()) {
      return null;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column.getIdentifier().getName());
    if (fieldSpec == null || fieldSpec.isSingleValueField()
        || fieldSpec.getDataType() != getColumnDataType(parameterType)) {
      return null;
    }
    Expression value = convertLiteral(literal, parameterType);
    return value != null ? RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), column, value) : null;
  }

  private static DataType getColumnDataType(PinotDataType parameterType) {
    return parameterType == PinotDataType.INT ? DataType.INT : DataType.STRING;
  }

  /// Converts the literal to the function parameter type the same way `ScalarTransformFunctionWrapper` converts a
  /// literal argument, or returns `null` if it cannot be converted.
  @Nullable
  private static Expression convertLiteral(Literal literal, PinotDataType parameterType) {
    Object value;
    try {
      LiteralContext literalContext = new LiteralContext(literal);
      switch (literalContext.getType()) {
        case BOOLEAN:
          value = parameterType.convert(literalContext.getBooleanValue(), PinotDataType.BOOLEAN);
          break;
        case INT:
          value = parameterType.convert(literalContext.getIntValue(), PinotDataType.INT);
          break;
        case LONG:
          value = parameterType.convert(literalContext.getLongValue(), PinotDataType.LONG);
          break;
        case FLOAT:
          value = parameterType.convert(literalContext.getFloatValue(), PinotDataType.FLOAT);
          break;
        case DOUBLE:
          value = parameterType.convert(literalContext.getDoubleValue(), PinotDataType.DOUBLE);
          break;
        case BIG_DECIMAL:
          value = parameterType == PinotDataType.STRING ? literalContext.getStringValue()
              : parameterType.convert(literalContext.getBigDecimalValue(), PinotDataType.BIG_DECIMAL);
          break;
        case STRING:
          value = parameterType.convert(literalContext.getStringValue(), PinotDataType.STRING);
          break;
        default:
          return null;
      }
    } catch (Exception e) {
      return null;
    }
    if (value == null) {
      return null;
    }
    return parameterType == PinotDataType.INT ? RequestUtils.getLiteralExpression((int) (Integer) value)
        : RequestUtils.getLiteralExpression((String) value);
  }
}
