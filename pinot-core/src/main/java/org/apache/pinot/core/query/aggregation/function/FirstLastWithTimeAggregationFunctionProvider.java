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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Creates FIRST_WITH_TIME and LAST_WITH_TIME kernels from their explicit data type argument. The providers are
/// stateless and safe for concurrent query construction; each call creates a new aggregation function.
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
      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
      Preconditions.checkArgument(numArguments == 3,
          "FIRST_WITH_TIME expects 3 arguments, got: %s. The function can be used as "
              + "firstWithTime(dataColumn, timeColumn, 'dataType')", numArguments);
      ExpressionContext dataCol = arguments.get(0);
      ExpressionContext timeCol = arguments.get(1);
      ExpressionContext dataTypeExp = arguments.get(2);
      Preconditions.checkArgument(dataTypeExp.getType() == ExpressionContext.Type.LITERAL,
          "FIRST_WITH_TIME expects the 3rd argument to be literal, got: %s. The function can be used as "
              + "firstWithTime(dataColumn, timeColumn, 'dataType')", dataTypeExp.getType());
      DataType dataType = DataType.valueOf(dataTypeExp.getLiteral().getStringValue().toUpperCase());
      switch (dataType) {
        case BOOLEAN:
          return new FirstIntValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled, true);
        case INT:
          return new FirstIntValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled, false);
        case LONG:
          return new FirstLongValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case FLOAT:
          return new FirstFloatValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case DOUBLE:
          return new FirstDoubleValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case STRING:
          return new FirstStringValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        default:
          throw new IllegalArgumentException("Unsupported data type for FIRST_WITH_TIME: " + dataType);
      }
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
      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
      Preconditions.checkArgument(numArguments == 3,
          "LAST_WITH_TIME expects 3 arguments, got: %s. The function can be used as "
              + "lastWithTime(dataColumn, timeColumn, 'dataType')", numArguments);
      ExpressionContext dataCol = arguments.get(0);
      ExpressionContext timeCol = arguments.get(1);
      ExpressionContext dataTypeExp = arguments.get(2);
      Preconditions.checkArgument(dataTypeExp.getType() == ExpressionContext.Type.LITERAL,
          "LAST_WITH_TIME expects the 3rd argument to be literal, got: %s. The function can be used as "
              + "lastWithTime(dataColumn, timeColumn, 'dataType')", dataTypeExp.getType());
      DataType dataType = DataType.valueOf(dataTypeExp.getLiteral().getStringValue().toUpperCase());
      switch (dataType) {
        case BOOLEAN:
          return new LastIntValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled, true);
        case INT:
          return new LastIntValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled, false);
        case LONG:
          return new LastLongValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case FLOAT:
          return new LastFloatValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case DOUBLE:
          return new LastDoubleValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        case STRING:
          return new LastStringValueWithTimeAggregationFunction(dataCol, timeCol, nullHandlingEnabled);
        default:
          throw new IllegalArgumentException("Unsupported data type for LAST_WITH_TIME: " + dataType);
      }
    }
  }
}
