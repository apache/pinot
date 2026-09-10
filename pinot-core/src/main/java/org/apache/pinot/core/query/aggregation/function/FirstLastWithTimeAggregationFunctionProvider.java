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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Locale;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Creates FIRST/LAST_WITH_TIME kernels from immutable schema bindings or legacy explicit type arguments. This
/// stateless provider keeps type dispatch with its aggregate family and is safe for concurrent query construction.
public final class FirstLastWithTimeAggregationFunctionProvider {
  private FirstLastWithTimeAggregationFunctionProvider() {
  }

  /// Service registration for FIRST_WITH_TIME.
  public static final class First implements AggregationFunctionProvider {
    @Override
    public AggregationFunctionType getType() {
      return AggregationFunctionType.FIRSTWITHTIME;
    }

    @Override
    public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
      return createFirst(function, nullHandlingEnabled);
    }
  }

  /// Service registration for LAST_WITH_TIME.
  public static final class Last implements AggregationFunctionProvider {
    @Override
    public AggregationFunctionType getType() {
      return AggregationFunctionType.LASTWITHTIME;
    }

    @Override
    public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
      return createLast(function, nullHandlingEnabled);
    }
  }

  private static AggregationFunction<?, ?> createFirst(FunctionContext function, boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = function.getArguments();
    ExpressionContext firstArgument = arguments.get(0);
    int numArguments = arguments.size();
    DataType dataType = getValueWithTimeDataType(function);
    ExpressionContext timeCol = arguments.get(1);
    boolean typeInferred = numArguments == 2;
    switch (dataType) {
      case BOOLEAN:
        return new FirstIntValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            true, typeInferred);
      case INT:
        return new FirstIntValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            false, typeInferred);
      case LONG:
        return new FirstLongValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred, ColumnDataType.LONG);
      case TIMESTAMP:
        Preconditions.checkArgument(typeInferred, "Unsupported data type for FIRST_WITH_TIME: %s", dataType);
        return new FirstLongValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            true, ColumnDataType.TIMESTAMP);
      case FLOAT:
        return new FirstFloatValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      case DOUBLE:
        return new FirstDoubleValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      case STRING:
        return new FirstStringValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      default:
        throw new IllegalArgumentException("Unsupported data type for FIRST_WITH_TIME: " + dataType);
    }
  }

  private static AggregationFunction<?, ?> createLast(FunctionContext function, boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = function.getArguments();
    ExpressionContext firstArgument = arguments.get(0);
    int numArguments = arguments.size();
    DataType dataType = getValueWithTimeDataType(function);
    ExpressionContext timeCol = arguments.get(1);
    boolean typeInferred = numArguments == 2;
    switch (dataType) {
      case BOOLEAN:
        return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            true, typeInferred);
      case INT:
        return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            false, typeInferred);
      case LONG:
        return new LastLongValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred, ColumnDataType.LONG);
      case TIMESTAMP:
        Preconditions.checkArgument(typeInferred, "Unsupported data type for LAST_WITH_TIME: %s", dataType);
        return new LastLongValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            true, ColumnDataType.TIMESTAMP);
      case FLOAT:
        return new LastFloatValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      case DOUBLE:
        return new LastDoubleValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      case STRING:
        return new LastStringValueWithTimeAggregationFunction(firstArgument, timeCol, nullHandlingEnabled,
            typeInferred);
      default:
        throw new IllegalArgumentException("Unsupported data type for LAST_WITH_TIME: " + dataType);
    }
  }

  private static DataType getValueWithTimeDataType(FunctionContext function) {
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 2 || numArguments == 3,
        "%s expects two arguments or three with an explicit data type, got: %s", function.getFunctionName(),
        numArguments);
    if (numArguments == 2) {
      AggregateCallBinding binding = function.getAggregationBinding();
      Preconditions.checkArgument(binding != null, "%s requires resolved input types", function.getFunctionName());
      return binding.getResultType().toDataType();
    }
    ExpressionContext dataTypeExpression = arguments.get(2);
    Preconditions.checkArgument(dataTypeExpression.getType() == ExpressionContext.Type.LITERAL,
        "%s expects the third argument to be a data type literal", function.getFunctionName());
    return DataType.valueOf(dataTypeExpression.getLiteral().getStringValue().toUpperCase(Locale.ROOT));
  }
}
