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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Serialize RexExpressions to the Protobuf equivalent classes defined expressions.proto.
 */
public class RexExpressionToProtoExpression {
  private RexExpressionToProtoExpression() {
  }

  public static Expressions.Expression convertExpression(RexExpression expression) {
    Expressions.Expression.Builder expressionBuilder = Expressions.Expression.newBuilder();
    if (expression instanceof RexExpression.InputRef) {
      expressionBuilder.setInputRef(convertInputRef((RexExpression.InputRef) expression));
    } else if (expression instanceof RexExpression.Literal) {
      expressionBuilder.setLiteral(convertLiteral((RexExpression.Literal) expression));
    } else {
      assert expression instanceof RexExpression.FunctionCall;
      expressionBuilder.setFunctionCall(convertFunctionCall((RexExpression.FunctionCall) expression));
    }
    return expressionBuilder.build();
  }

  public static Expressions.InputRef convertInputRef(RexExpression.InputRef inputRef) {
    return Expressions.InputRef.newBuilder().setIndex(inputRef.getIndex()).build();
  }

  public static Expressions.FunctionCall convertFunctionCall(RexExpression.FunctionCall functionCall) {
    List<RexExpression> operands = functionCall.getFunctionOperands();
    List<Expressions.Expression> protoOperands = new ArrayList<>(operands.size());
    for (RexExpression operand : operands) {
      protoOperands.add(convertExpression(operand));
    }
    return Expressions.FunctionCall.newBuilder()
        .setDataType(convertColumnDataType(functionCall.getDataType()))
        .setFunctionName(functionCall.getFunctionName())
        .addAllFunctionOperands(protoOperands)
        .setIsDistinct(functionCall.isDistinct())
        .setIgnoreNulls(functionCall.isIgnoreNulls())
        .build();
  }

  public static Expressions.Literal convertLiteral(RexExpression.Literal literal) {
    Expressions.Literal.Builder literalBuilder = Expressions.Literal.newBuilder();
    ColumnDataType dataType = literal.getDataType();
    literalBuilder.setDataType(convertColumnDataType(dataType));
    Object value = literal.getValue();
    if (value == null) {
      literalBuilder.setNull(true);
    } else {
      switch (dataType.getStoredType()) {
        case INT:
          literalBuilder.setInt((Integer) value);
          break;
        case LONG:
          literalBuilder.setLong((Long) value);
          break;
        case FLOAT:
          literalBuilder.setFloat((Float) value);
          break;
        case DOUBLE:
          literalBuilder.setDouble(((Number) value).doubleValue());
          break;
        case BIG_DECIMAL:
          literalBuilder.setBytes(ByteString.copyFrom(BigDecimalUtils.serialize((BigDecimal) value)));
          break;
        case STRING:
          literalBuilder.setString((String) value);
          break;
        case BYTES:
          literalBuilder.setBytes(ByteString.copyFrom(((ByteArray) value).getBytes()));
          break;
        case INT_ARRAY:
          literalBuilder.setIntArray(
              Expressions.IntArray.newBuilder().addAllValues(IntArrayList.wrap((int[]) value)).build());
          break;
        case LONG_ARRAY:
          literalBuilder.setLongArray(
              Expressions.LongArray.newBuilder().addAllValues(LongArrayList.wrap((long[]) value)).build());
          break;
        case FLOAT_ARRAY:
          literalBuilder.setFloatArray(
              Expressions.FloatArray.newBuilder().addAllValues(FloatArrayList.wrap((float[]) value)).build());
          break;
        case DOUBLE_ARRAY:
          literalBuilder.setDoubleArray(
              Expressions.DoubleArray.newBuilder().addAllValues(DoubleArrayList.wrap((double[]) value)).build());
          break;
        case STRING_ARRAY:
          literalBuilder.setStringArray(
              Expressions.StringArray.newBuilder().addAllValues(Arrays.asList((String[]) value)).build());
          break;
        default:
          throw new IllegalStateException("Unsupported ColumnDataType: " + dataType);
      }
    }
    return literalBuilder.build();
  }

  public static Expressions.ColumnDataType convertColumnDataType(ColumnDataType dataType) {
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
      case MAP:
        return Expressions.ColumnDataType.MAP;
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
      case UNKNOWN:
        return Expressions.ColumnDataType.UNKNOWN;
      default:
        throw new IllegalArgumentException("Unsupported ColumnDataType: " + dataType);
    }
  }
}
