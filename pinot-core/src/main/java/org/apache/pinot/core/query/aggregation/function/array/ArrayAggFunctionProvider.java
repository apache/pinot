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
import java.util.Locale;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionProvider;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Selects ARRAY_AGG's existing accumulator implementation from immutable call metadata or the legacy type option.
/// Providers are stateless and safe to share; each call creates a new aggregation function.
public final class ArrayAggFunctionProvider implements AggregationFunctionProvider {
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ARRAYAGG;
  }

  @Override
  public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    AggregateCallBinding binding = function.getAggregationBinding();
    DataType dataType;
    boolean distinct = false;
    if (binding != null) {
      Preconditions.checkArgument(numArguments == 1 || numArguments == 2,
          "Inferred ARRAY_AGG expects an expression and an optional boolean distinct literal");
      Preconditions.checkArgument(binding.getResultType().isArray(),
          "ARRAY_AGG requires an array result binding, got: %s", binding.getResultType());
      dataType = binding.getResultType().toDataType();
      if (numArguments == 2) {
        ExpressionContext option = arguments.get(1);
        Preconditions.checkArgument(option.getType() == ExpressionContext.Type.LITERAL
                && option.getLiteral().getType() == DataType.BOOLEAN && option.getLiteral().getValue() != null,
            "ARRAY_AGG distinct option must be a boolean literal");
        distinct = option.getLiteral().getBooleanValue();
      }
    } else {
      Preconditions.checkArgument(numArguments >= 2,
          "ARRAY_AGG requires a schema binding or an explicit type: arrayAgg(expression, 'dataType'[, isDistinct])");
      ExpressionContext typeOption = arguments.get(1);
      Preconditions.checkArgument(typeOption.getType() == ExpressionContext.Type.LITERAL,
          "ARRAY_AGG expects the 2nd argument to be a literal type, got: %s", typeOption.getType());
      dataType = DataType.valueOf(typeOption.getLiteral().getStringValue().toUpperCase(Locale.ROOT));
      if (numArguments == 3) {
        ExpressionContext distinctOption = arguments.get(2);
        Preconditions.checkArgument(distinctOption.getType() == ExpressionContext.Type.LITERAL,
            "ARRAY_AGG expects the 3rd argument to be a literal, got: %s", distinctOption.getType());
        distinct = distinctOption.getLiteral().getBooleanValue();
      }
    }
    ExpressionContext expression = arguments.get(0);
    if (distinct) {
      return switch (dataType) {
        case BOOLEAN, INT -> new ArrayAggDistinctIntFunction(expression, dataType, nullHandlingEnabled);
        case LONG, TIMESTAMP -> new ArrayAggDistinctLongFunction(expression, dataType, nullHandlingEnabled);
        case FLOAT -> new ArrayAggDistinctFloatFunction(expression, nullHandlingEnabled);
        case DOUBLE -> new ArrayAggDistinctDoubleFunction(expression, nullHandlingEnabled);
        case BIG_DECIMAL -> new ArrayAggDistinctBigDecimalFunction(expression, nullHandlingEnabled);
        case STRING, JSON -> new ArrayAggDistinctStringFunction(expression, nullHandlingEnabled);
        case BYTES, UUID -> new ArrayAggDistinctBytesFunction(expression, dataType, nullHandlingEnabled);
        default -> throw new IllegalArgumentException("Unsupported data type for ARRAY_AGG: " + dataType);
      };
    }
    return switch (dataType) {
      case BOOLEAN, INT -> new ArrayAggIntFunction(expression, dataType, nullHandlingEnabled);
      case LONG, TIMESTAMP -> new ArrayAggLongFunction(expression, dataType, nullHandlingEnabled);
      case FLOAT -> new ArrayAggFloatFunction(expression, nullHandlingEnabled);
      case DOUBLE -> new ArrayAggDoubleFunction(expression, nullHandlingEnabled);
      case BIG_DECIMAL -> new ArrayAggBigDecimalFunction(expression, nullHandlingEnabled);
      case STRING, JSON -> new ArrayAggStringFunction(expression, nullHandlingEnabled);
      case BYTES, UUID -> new ArrayAggBytesFunction(expression, dataType, nullHandlingEnabled);
      default -> throw new IllegalArgumentException("Unsupported data type for ARRAY_AGG: " + dataType);
    };
  }
}
