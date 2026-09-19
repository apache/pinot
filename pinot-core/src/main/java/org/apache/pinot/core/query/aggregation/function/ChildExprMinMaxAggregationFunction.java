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

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Placeholder for an ExprMin/Max projection. Its immutable bound type is available before the parent produces rows.
public class ChildExprMinMaxAggregationFunction extends ChildAggregationFunction {

  private final boolean _isMax;

  public ChildExprMinMaxAggregationFunction(List<ExpressionContext> operands, boolean isMax) {
    this(operands, isMax, ColumnDataType.UNKNOWN);
  }

  public ChildExprMinMaxAggregationFunction(List<ExpressionContext> operands, boolean isMax,
      ColumnDataType resultType) {
    super(operands, resultType);
    _isMax = isMax;
  }

  @Override
  public AggregationFunctionType getType() {
    return _isMax ? AggregationFunctionType.EXPRMAX : AggregationFunctionType.EXPRMIN;
  }

  /// Constructs schema-bound ExprMin projection placeholders.
  public static final class MinProvider implements AggregationFunctionProvider {
    @Override
    public AggregationFunctionType getType() {
      return AggregationFunctionType.PINOTCHILDAGGEXPRMIN;
    }

    @Override
    public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
      return new ChildExprMinMaxAggregationFunction(function.getArguments(), false,
          function.getAggregationBinding() != null
              ? function.getAggregationBinding().getResultType()
              : ColumnDataType.UNKNOWN);
    }
  }

  /// Constructs schema-bound ExprMax projection placeholders.
  public static final class MaxProvider implements AggregationFunctionProvider {
    @Override
    public AggregationFunctionType getType() {
      return AggregationFunctionType.PINOTCHILDAGGEXPRMAX;
    }

    @Override
    public AggregationFunction<?, ?> create(FunctionContext function, boolean nullHandlingEnabled) {
      return new ChildExprMinMaxAggregationFunction(function.getArguments(), true,
          function.getAggregationBinding() != null
              ? function.getAggregationBinding().getResultType()
              : ColumnDataType.UNKNOWN);
    }
  }
}
