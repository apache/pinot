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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Serialize RexExpressions to the Protobuf equivalent classes defined expressions.proto.
 */
public class RexExpressionToProtoExpression {
  private RexExpressionToProtoExpression() {
  }

  public static Expressions.RexExpression process(RexExpression expression) {
    if (expression instanceof RexExpression.InputRef) {
      return serializeInputRef((RexExpression.InputRef) expression);
    } else if (expression instanceof RexExpression.Literal) {
      return serializeLiteral((RexExpression.Literal) expression);
    } else {
      assert expression instanceof RexExpression.FunctionCall;
      return serializeFunctionCall((RexExpression.FunctionCall) expression);
    }
  }

  private static Expressions.RexExpression serializeInputRef(RexExpression.InputRef inputRef) {
    return Expressions.RexExpression.newBuilder()
        .setInputRef(Expressions.InputRef.newBuilder().setIndex(inputRef.getIndex())).build();
  }

  private static Expressions.RexExpression serializeFunctionCall(RexExpression.FunctionCall functionCall) {
    List<RexExpression> operands = functionCall.getFunctionOperands();
    List<Expressions.RexExpression> protoOperands = new ArrayList<>(operands.size());
    for (RexExpression operand : operands) {
      protoOperands.add(process(operand));
    }
    Expressions.FunctionCall.Builder protoFunctionCallBuilder =
        Expressions.FunctionCall.newBuilder().setDataType(convertColumnDataType(functionCall.getDataType()))
            .setFunctionName(functionCall.getFunctionName()).addAllFunctionOperands(protoOperands)
            .setIsDistinct(functionCall.isDistinct());
    return Expressions.RexExpression.newBuilder().setFunctionCall(protoFunctionCallBuilder).build();
  }

  private static Expressions.RexExpression serializeLiteral(RexExpression.Literal literal) {
    Expressions.Literal.Builder literalBuilder = Expressions.Literal.newBuilder();
    literalBuilder.setDataType(convertColumnDataType(literal.getDataType()));
    Object literalValue = literal.getValue();
    if (literalValue != null) {
      if (literalValue instanceof Boolean) {
        literalBuilder.setBoolField((Boolean) literalValue);
      } else if (literalValue instanceof Integer) {
        literalBuilder.setIntField((Integer) literalValue);
      } else if (literalValue instanceof Long) {
        literalBuilder.setLongField((Long) literalValue);
      } else if (literalValue instanceof Float) {
        literalBuilder.setFloatField((Float) literalValue);
      } else if (literalValue instanceof Double) {
        literalBuilder.setDoubleField((Double) literalValue);
      } else if (literalValue instanceof String) {
        literalBuilder.setStringField((String) literalValue);
      } else if (literalValue instanceof ByteArray) {
        literalBuilder.setBytesField(ByteString.copyFrom(((ByteArray) literalValue).getBytes()));
      } else {
        Serializable value = literal.getDataType().convert(literal.getValue());
        byte[] data = SerializationUtils.serialize(value);
        literalBuilder.setSerializedField(ByteString.copyFrom(data));
      }
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
