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
package org.apache.pinot.core.query.aggregation;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.Schema;


/// Binds schema-dependent aggregation calls without changing their names or SQL operands. Binding a request mutates
/// its metadata and must happen before publishing it to execution threads. Binding metadata is immutable; bound
/// function contexts retain the original argument list.
public final class AggregationFunctionBinder {
  private AggregationFunctionBinder() {
  }

  /// Attaches type bindings throughout a request, including aggregates nested in scalar expressions and FILTER.
  public static void bind(PinotQuery query, Schema schema) {
    bind(query, new ExpressionTypeResolver(schema));
  }

  /// Binds a broker-side query to the actual output columns of its inner query.
  public static void bind(PinotQuery query, DataSchema schema) {
    bind(query, new ExpressionTypeResolver(schema));
  }

  private static void bind(PinotQuery query, ExpressionTypeResolver resolver) {
    bind(query.getSelectList(), resolver);
    bind(query.getGroupByList(), resolver);
    bind(query.getOrderByList(), resolver);
    bind(query.getFilterExpression(), resolver);
    bind(query.getHavingExpression(), resolver);
  }

  /// Binds a call created directly by a server-side caller. An existing request binding remains authoritative.
  public static FunctionContext bind(FunctionContext function, Schema schema) {
    if (function.getType() != FunctionContext.Type.AGGREGATION || function.getAggregationBinding() != null) {
      return function;
    }
    AggregateCallBinding binding = bind(function, new ExpressionTypeResolver(schema));
    return binding == null
        ? function
        : new FunctionContext(function.getType(), function.getFunctionName(), function.getArguments(), binding);
  }

  private static void bind(@Nullable List<Expression> expressions, ExpressionTypeResolver resolver) {
    if (expressions != null) {
      for (Expression expression : expressions) {
        bind(expression, resolver);
      }
    }
  }

  private static void bind(@Nullable Expression expression, ExpressionTypeResolver resolver) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }
    Function function = expression.getFunctionCall();
    bind(function.getOperands(), resolver);
    if (function.isSetAggregationBinding() || !AggregationFunctionType.isAggregationFunction(function.getOperator())) {
      return;
    }
    AggregateCallBinding binding = bind(RequestContextUtils.getFunction(function), resolver);
    if (binding != null) {
      function.setAggregationBinding(binding.toThrift());
    }
  }

  @Nullable
  private static AggregateCallBinding bind(FunctionContext function, ExpressionTypeResolver resolver) {
    AggregationFunctionType functionType =
        AggregationFunctionType.getAggregationFunctionType(function.getFunctionName());
    List<ExpressionContext> arguments = function.getArguments();
    return AggregationFunctionTypeResolver.bind(functionType, arguments, i -> resolver.resolve(arguments.get(i)));
  }
}
