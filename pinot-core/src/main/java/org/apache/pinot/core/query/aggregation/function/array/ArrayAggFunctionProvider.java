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
package org.apache.pinot.core.query.aggregation.function.array;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionProvider;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Selects the ARRAY_AGG accumulator implementation from the explicit data type and distinct arguments. The provider
/// is stateless and safe to share; each call creates a new aggregation function.
public final class ArrayAggFunctionProvider implements AggregationFunctionProvider {
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ARRAYAGG;
  }

  @Override
  public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments >= 2,
        "ARRAY_AGG expects 2 or 3 arguments, got: %s. The function can be used as "
            + "arrayAgg(dataColumn, 'dataType', ['isDistinct'])", numArguments);
    ExpressionContext expression = arguments.get(0);
    ExpressionContext dataTypeExp = arguments.get(1);
    Preconditions.checkArgument(dataTypeExp.getType() == ExpressionContext.Type.LITERAL,
        "ARRAY_AGG expects the 2nd argument to be literal, got: %s. The function can be used as "
            + "arrayAgg(dataColumn, 'dataType', ['isDistinct'])", dataTypeExp.getType());
    DataType dataType = DataType.valueOf(dataTypeExp.getLiteral().getStringValue().toUpperCase());
    boolean isDistinct = false;
    if (numArguments == 3) {
      ExpressionContext isDistinctExp = arguments.get(2);
      Preconditions.checkArgument(isDistinctExp.getType() == ExpressionContext.Type.LITERAL,
          "ARRAY_AGG expects the 3rd argument to be literal, got: %s. The function can be used as "
              + "arrayAgg(dataColumn, 'dataType', ['isDistinct'])", isDistinctExp.getType());
      isDistinct = isDistinctExp.getLiteral().getBooleanValue();
    }
    if (isDistinct) {
      switch (dataType) {
        case BOOLEAN:
        case INT:
          return new ArrayAggDistinctIntFunction(expression, dataType, nullHandlingEnabled);
        case LONG:
        case TIMESTAMP:
          return new ArrayAggDistinctLongFunction(expression, dataType, nullHandlingEnabled);
        case FLOAT:
          return new ArrayAggDistinctFloatFunction(expression, nullHandlingEnabled);
        case DOUBLE:
          return new ArrayAggDistinctDoubleFunction(expression, nullHandlingEnabled);
        case BIG_DECIMAL:
          return new ArrayAggDistinctBigDecimalFunction(expression, nullHandlingEnabled);
        case STRING:
        case JSON:
          return new ArrayAggDistinctStringFunction(expression, nullHandlingEnabled);
        case BYTES:
        case UUID:
          return new ArrayAggDistinctBytesFunction(expression, dataType, nullHandlingEnabled);
        default:
          throw new IllegalArgumentException("Unsupported data type for ARRAY_AGG: " + dataType);
      }
    }
    switch (dataType) {
      case BOOLEAN:
      case INT:
        return new ArrayAggIntFunction(expression, dataType, nullHandlingEnabled);
      case LONG:
      case TIMESTAMP:
        return new ArrayAggLongFunction(expression, dataType, nullHandlingEnabled);
      case FLOAT:
        return new ArrayAggFloatFunction(expression, nullHandlingEnabled);
      case DOUBLE:
        return new ArrayAggDoubleFunction(expression, nullHandlingEnabled);
      case BIG_DECIMAL:
        return new ArrayAggBigDecimalFunction(expression, nullHandlingEnabled);
      case STRING:
      case JSON:
        return new ArrayAggStringFunction(expression, nullHandlingEnabled);
      case BYTES:
      case UUID:
        return new ArrayAggBytesFunction(expression, dataType, nullHandlingEnabled);
      default:
        throw new IllegalArgumentException("Unsupported data type for ARRAY_AGG: " + dataType);
    }
  }
}
