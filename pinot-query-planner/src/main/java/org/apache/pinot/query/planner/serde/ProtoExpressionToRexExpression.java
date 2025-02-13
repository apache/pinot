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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Converts Protobuf versions of RexExpression to appropriate RexExpression classes.
 */
public class ProtoExpressionToRexExpression {
  private ProtoExpressionToRexExpression() {
  }

  public static RexExpression convertExpression(Expressions.Expression expression) {
    switch (expression.getExpressionCase()) {
      case INPUTREF:
        return convertInputRef(expression.getInputRef());
      case LITERAL:
        return convertLiteral(expression.getLiteral());
      case FUNCTIONCALL:
        return convertFunctionCall(expression.getFunctionCall());
      default:
        throw new IllegalStateException("Unsupported proto Expression type: " + expression.getExpressionCase());
    }
  }

  public static RexExpression.InputRef convertInputRef(Expressions.InputRef inputRef) {
    return new RexExpression.InputRef(inputRef.getIndex());
  }

  public static RexExpression.FunctionCall convertFunctionCall(Expressions.FunctionCall functionCall) {
    List<Expressions.Expression> protoOperands = functionCall.getFunctionOperandsList();
    List<RexExpression> operands = new ArrayList<>(protoOperands.size());
    for (Expressions.Expression protoOperand : protoOperands) {
      operands.add(convertExpression(protoOperand));
    }
    return new RexExpression.FunctionCall(convertColumnDataType(functionCall.getDataType()),
        functionCall.getFunctionName(), operands, functionCall.getIsDistinct(), true);
  }

  public static RexExpression.Literal convertLiteral(Expressions.Literal literal) {
    ColumnDataType dataType = convertColumnDataType(literal.getDataType());
    if (literal.hasNull()) {
      return new RexExpression.Literal(dataType, null);
    }
    switch (dataType.getStoredType()) {
      case INT:
        return new RexExpression.Literal(dataType, literal.getInt());
      case LONG:
        return new RexExpression.Literal(dataType, literal.getLong());
      case FLOAT:
        return new RexExpression.Literal(dataType, literal.getFloat());
      case DOUBLE:
        return new RexExpression.Literal(dataType, literal.getDouble());
      case BIG_DECIMAL:
        return new RexExpression.Literal(dataType, BigDecimalUtils.deserialize(literal.getBytes().toByteArray()));
      case STRING:
        return new RexExpression.Literal(dataType, literal.getString());
      case BYTES:
        return new RexExpression.Literal(dataType, new ByteArray(literal.getBytes().toByteArray()));
      case INT_ARRAY: {
        Expressions.IntArray intArray = literal.getIntArray();
        int numValues = intArray.getValuesCount();
        int[] values = new int[numValues];
        {
          for (int i = 0; i < numValues; i++) {
            values[i] = intArray.getValues(i);
          }
        }
        return new RexExpression.Literal(dataType, values);
      }
      case LONG_ARRAY: {
        Expressions.LongArray longArray = literal.getLongArray();
        int numValues = longArray.getValuesCount();
        long[] values = new long[numValues];
        {
          for (int i = 0; i < numValues; i++) {
            values[i] = longArray.getValues(i);
          }
        }
        return new RexExpression.Literal(dataType, values);
      }
      case FLOAT_ARRAY: {
        Expressions.FloatArray floatArray = literal.getFloatArray();
        int numValues = floatArray.getValuesCount();
        float[] values = new float[numValues];
        {
          for (int i = 0; i < numValues; i++) {
            values[i] = floatArray.getValues(i);
          }
        }
        return new RexExpression.Literal(dataType, values);
      }
      case DOUBLE_ARRAY: {
        Expressions.DoubleArray doubleArray = literal.getDoubleArray();
        int numValues = doubleArray.getValuesCount();
        double[] values = new double[numValues];
        {
          for (int i = 0; i < numValues; i++) {
            values[i] = doubleArray.getValues(i);
          }
        }
        return new RexExpression.Literal(dataType, values);
      }
      case STRING_ARRAY: {
        Expressions.StringArray stringArray = literal.getStringArray();
        int numValues = stringArray.getValuesCount();
        String[] values = new String[numValues];
        {
          for (int i = 0; i < numValues; i++) {
            values[i] = stringArray.getValues(i);
          }
        }
        return new RexExpression.Literal(dataType, values);
      }
      default:
        throw new IllegalStateException("Unsupported ColumnDataType: " + dataType);
    }
  }

  public static ColumnDataType convertColumnDataType(Expressions.ColumnDataType dataType) {
    switch (dataType) {
      case INT:
        return ColumnDataType.INT;
      case LONG:
        return ColumnDataType.LONG;
      case FLOAT:
        return ColumnDataType.FLOAT;
      case DOUBLE:
        return ColumnDataType.DOUBLE;
      case BIG_DECIMAL:
        return ColumnDataType.BIG_DECIMAL;
      case BOOLEAN:
        return ColumnDataType.BOOLEAN;
      case TIMESTAMP:
        return ColumnDataType.TIMESTAMP;
      case STRING:
        return ColumnDataType.STRING;
      case JSON:
        return ColumnDataType.JSON;
      case BYTES:
        return ColumnDataType.BYTES;
      case INT_ARRAY:
        return ColumnDataType.INT_ARRAY;
      case LONG_ARRAY:
        return ColumnDataType.LONG_ARRAY;
      case FLOAT_ARRAY:
        return ColumnDataType.FLOAT_ARRAY;
      case DOUBLE_ARRAY:
        return ColumnDataType.DOUBLE_ARRAY;
      case BOOLEAN_ARRAY:
        return ColumnDataType.BOOLEAN_ARRAY;
      case TIMESTAMP_ARRAY:
        return ColumnDataType.TIMESTAMP_ARRAY;
      case STRING_ARRAY:
        return ColumnDataType.STRING_ARRAY;
      case BYTES_ARRAY:
        return ColumnDataType.BYTES_ARRAY;
      case MAP:
        return ColumnDataType.MAP;
      case OBJECT:
        return ColumnDataType.OBJECT;
      case UNKNOWN:
        return ColumnDataType.UNKNOWN;
      default:
        throw new IllegalStateException("Unsupported proto ColumnDataType: " + dataType);
    }
  }
}
