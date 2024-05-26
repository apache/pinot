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

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.SerializationUtils;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class RexExpressionVisitor {
  private RexExpressionVisitor() {
  }

  public static Expressions.RexExpression process(RexExpression expression) {
    if (expression instanceof RexExpression.InputRef) {
      return visitInputRef((RexExpression.InputRef) expression);
    } else if (expression instanceof RexExpression.Literal) {
      return visitLiteral((RexExpression.Literal) expression);
    } else if (expression instanceof RexExpression.FunctionCall) {
      return visitFunctionCall((RexExpression.FunctionCall) expression);
    }

    throw new RuntimeException(String.format("Unknown Type Expression Type: %s", expression.getKind()));
  }

  private static Expressions.RexExpression visitInputRef(RexExpression.InputRef inputRef) {
    return Expressions.RexExpression.newBuilder()
        .setInputRef(Expressions.InputRef.newBuilder().setIndex(inputRef.getIndex())).build();
  }

  private static Expressions.RexExpression visitFunctionCall(RexExpression.FunctionCall functionCall) {
    List<Expressions.RexExpression> functionOperands =
        functionCall.getFunctionOperands().stream().map(RexExpressionVisitor::process).collect(Collectors.toList());
    Expressions.FunctionCall.Builder protoFunctionCallBuilder =
        Expressions.FunctionCall.newBuilder().setSqlKind(functionCall.getKind().ordinal())
            .setDataType(convertColumnDataType(functionCall.getDataType()))
            .setFunctionName(functionCall.getFunctionName()).addAllFunctionOperands(functionOperands);

    return Expressions.RexExpression.newBuilder().setFunctionCall(protoFunctionCallBuilder).build();
  }

  private static Expressions.RexExpression visitLiteral(RexExpression.Literal literal) {
    Expressions.Literal.Builder literalBuilder = Expressions.Literal.newBuilder();
    literalBuilder.setDataType(convertColumnDataType(literal.getDataType()));
    if (literal.getValue() != null) {
      Serializable value = literal.getDataType().convert(literal.getValue());
      byte[] data = SerializationUtils.serialize(value);
      literalBuilder.setSerializedValue(ByteString.copyFrom(data));
      literalBuilder.setIsValueNull(false);
    } else {
      literalBuilder.setIsValueNull(true);
    }

    return Expressions.RexExpression.newBuilder().setLiteral(literalBuilder).build();
  }

  public static Expressions.ColumnDataType convertColumnDataType(DataSchema.ColumnDataType dataType) {
    switch (dataType) {
      case INT:
        return Expressions.ColumnDataType.INT;
      case LONG:
        return Expressions.ColumnDataType.LONG;
      case FLOAT:
        return Expressions.ColumnDataType.FLOAT;
      case DOUBLE:
        return Expressions.ColumnDataType.DOUBLE;
      case BIG_DECIMAL:
        return Expressions.ColumnDataType.BIG_DECIMAL;
      case BOOLEAN:
        return Expressions.ColumnDataType.BOOLEAN;
      case TIMESTAMP:
        return Expressions.ColumnDataType.TIMESTAMP;
      case STRING:
        return Expressions.ColumnDataType.STRING;
      case JSON:
        return Expressions.ColumnDataType.JSON;
      case BYTES:
        return Expressions.ColumnDataType.BYTES;
      case INT_ARRAY:
        return Expressions.ColumnDataType.INT_ARRAY;
      case LONG_ARRAY:
        return Expressions.ColumnDataType.LONG_ARRAY;
      case FLOAT_ARRAY:
        return Expressions.ColumnDataType.FLOAT_ARRAY;
      case DOUBLE_ARRAY:
        return Expressions.ColumnDataType.DOUBLE_ARRAY;
      case BOOLEAN_ARRAY:
        return Expressions.ColumnDataType.BOOLEAN_ARRAY;
      case TIMESTAMP_ARRAY:
        return Expressions.ColumnDataType.TIMESTAMP_ARRAY;
      case STRING_ARRAY:
        return Expressions.ColumnDataType.STRING_ARRAY;
      case BYTES_ARRAY:
        return Expressions.ColumnDataType.BYTES_ARRAY;
      case OBJECT:
        return Expressions.ColumnDataType.OBJECT;
      default:
        return Expressions.ColumnDataType.UNKNOWN;
    }
  }
}
