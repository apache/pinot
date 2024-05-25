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
package org.apache.pinot.query.planner.serde;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.SerializationUtils;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class ProtoExpressionVisitor {
  private ProtoExpressionVisitor() {
  }

  public static RexExpression process(Expressions.RexExpression expression) {
    switch (expression.getExpressionCase()) {
      case INPUTREF:
        return visitInputRef(expression.getInputRef());
      case LITERAL:
        return visitLiteral(expression.getLiteral());
      case FUNCTIONCALL:
        return visitFunctionCall(expression.getFunctionCall());
      default:
    }

    throw new RuntimeException(String.format("Unknown Type Expression Type: %s", expression.getExpressionCase()));
  }

  private static RexExpression visitInputRef(Expressions.InputRef inputRef) {
    return new RexExpression.InputRef(inputRef.getIndex());
  }

  private static RexExpression visitFunctionCall(Expressions.FunctionCall functionCall) {
    List<RexExpression> functionOperands =
        functionCall.getFunctionOperandsList().stream().map(ProtoExpressionVisitor::process)
            .collect(Collectors.toList());
    return new RexExpression.FunctionCall(SqlKind.values()[functionCall.getSqlKind()],
        convertColumnDataType(functionCall.getDataType()), functionCall.getFunctionName(), functionOperands);
  }

  private static RexExpression visitLiteral(Expressions.Literal literal) {
    DataSchema.ColumnDataType dataType = convertColumnDataType(literal.getDataType());
    Object obj =
        !literal.getIsValueNull() ? SerializationUtils.deserialize(literal.getSerializedValue().toByteArray()) : null;
    return new RexExpression.Literal(dataType, obj);
  }

  private static DataSchema.ColumnDataType convertColumnDataType(Expressions.ColumnDataType dataType) {
    switch (dataType) {
      case INT:
        return DataSchema.ColumnDataType.INT;
      case LONG:
        return DataSchema.ColumnDataType.LONG;
      case FLOAT:
        return DataSchema.ColumnDataType.FLOAT;
      case DOUBLE:
        return DataSchema.ColumnDataType.DOUBLE;
      case BIG_DECIMAL:
        return DataSchema.ColumnDataType.BIG_DECIMAL;
      case BOOLEAN:
        return DataSchema.ColumnDataType.BOOLEAN;
      case TIMESTAMP:
        return DataSchema.ColumnDataType.TIMESTAMP;
      case STRING:
        return DataSchema.ColumnDataType.STRING;
      case JSON:
        return DataSchema.ColumnDataType.JSON;
      case BYTES:
        return DataSchema.ColumnDataType.BYTES;
      case INT_ARRAY:
        return DataSchema.ColumnDataType.INT_ARRAY;
      case LONG_ARRAY:
        return DataSchema.ColumnDataType.LONG_ARRAY;
      case FLOAT_ARRAY:
        return DataSchema.ColumnDataType.FLOAT_ARRAY;
      case DOUBLE_ARRAY:
        return DataSchema.ColumnDataType.DOUBLE_ARRAY;
      case BOOLEAN_ARRAY:
        return DataSchema.ColumnDataType.BOOLEAN_ARRAY;
      case TIMESTAMP_ARRAY:
        return DataSchema.ColumnDataType.TIMESTAMP_ARRAY;
      case STRING_ARRAY:
        return DataSchema.ColumnDataType.STRING_ARRAY;
      case BYTES_ARRAY:
        return DataSchema.ColumnDataType.BYTES_ARRAY;
      default:
        return DataSchema.ColumnDataType.UNKNOWN;
    }
  }
}
