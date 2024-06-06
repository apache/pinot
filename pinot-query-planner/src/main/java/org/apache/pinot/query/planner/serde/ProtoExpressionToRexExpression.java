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
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pinot.common.proto.Expressions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
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
        functionCall.getFunctionName(), operands, functionCall.getIsDistinct());
  }

  public static RexExpression.Literal convertLiteral(Expressions.Literal literal) {
    DataSchema.ColumnDataType dataType = convertColumnDataType(literal.getDataType());
    if (literal.getIsValueNull()) {
      return new RexExpression.Literal(dataType, null);
    }
    Object obj;
    switch (literal.getLiteralFieldCase()) {
      case BOOLFIELD:
        obj = literal.getBoolField();
        break;
      case INTFIELD:
        obj = literal.getIntField();
        break;
      case LONGFIELD:
        obj = literal.getLongField();
        break;
      case FLOATFIELD:
        obj = literal.getFloatField();
        break;
      case DOUBLEFIELD:
        obj = literal.getDoubleField();
        break;
      case STRINGFIELD:
        obj = literal.getStringField();
        break;
      case BYTESFIELD:
        obj = new ByteArray(literal.getBytesField().toByteArray());
        break;
      case SERIALIZEDFIELD:
        obj = SerializationUtils.deserialize(literal.getSerializedField().toByteArray());
        break;
      default:
        throw new IllegalStateException("Unsupported proto Literal type: " + literal.getLiteralFieldCase());
    }
    return new RexExpression.Literal(dataType, obj);
  }

  public static DataSchema.ColumnDataType convertColumnDataType(Expressions.ColumnDataType dataType) {
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
      case OBJECT:
        return DataSchema.ColumnDataType.OBJECT;
      case UNKNOWN:
        return DataSchema.ColumnDataType.UNKNOWN;
      default:
        throw new IllegalStateException("Unsupported proto ColumnDataType: " + dataType);
    }
  }
}
