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
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/// The `ArrayContainsFilterOptimizer` rewrites a boolean `arrayContains*` predicate on a multi-value column into the
/// equivalent `EQUALS` predicate, e.g. `arrayContainsString(mvCol, 'foo')` (parsed as
/// `arrayContainsString(mvCol, 'foo') = true`) becomes `mvCol = 'foo'`.
///
/// On a multi-value column, `EQUALS` matches a document when any of its values equals the literal, which is exactly
/// what `arrayContains*` evaluates. Unlike the scalar function (evaluated by scanning every document), the `EQUALS`
/// predicate can be served by the dictionary / inverted index and used for segment pruning.
///
/// The rewrite is only applied when:
/// - The first argument is an identifier of a multi-value column in the schema whose stored type matches the function
///   (e.g. `arrayContainsInt` on an `INT` column), so that value conversion behaves identically
/// - The second argument is a non-null literal
/// - The predicate is `= true`. Negations (`NOT`, `= false`, `!= true`) are left untouched because `!=` on a
///   multi-value column means "some value does not match", which differs from "no value matches"
public class ArrayContainsFilterOptimizer implements FilterOptimizer {
  private static final Map<String, DataType> FUNCTION_DATA_TYPES =
      Map.of("arraycontainsint", DataType.INT, "arraycontainsstring", DataType.STRING);

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
    DataType functionDataType =
        FUNCTION_DATA_TYPES.get(RequestUtils.canonicalizeFunctionName(arrayContains.getOperator()));
    if (functionDataType == null) {
      return null;
    }
    List<Expression> arguments = arrayContains.getOperands();
    if (arguments.size() != 2) {
      return null;
    }
    Expression column = arguments.get(0);
    Expression value = arguments.get(1);
    if (column.getIdentifier() == null || value.getLiteral() == null || value.getLiteral().isSetNullValue()) {
      return null;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column.getIdentifier().getName());
    if (fieldSpec == null || fieldSpec.isSingleValueField() || fieldSpec.getDataType() != functionDataType) {
      return null;
    }
    return RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), column, value);
  }
}
