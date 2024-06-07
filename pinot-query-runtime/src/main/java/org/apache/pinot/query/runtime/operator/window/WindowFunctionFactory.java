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
package org.apache.pinot.query.runtime.operator.window;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;
import org.apache.pinot.query.runtime.operator.window.aggregate.AggregateWindowFunction;
import org.apache.pinot.query.runtime.operator.window.range.RangeWindowFunction;
import org.apache.pinot.query.runtime.operator.window.value.ValueWindowFunction;


/**
 * Factory class to construct WindowFunction instances.
 */
public class WindowFunctionFactory {
  private WindowFunctionFactory() {
  }

  public static final Map<String, Class<? extends WindowFunction>> WINDOW_FUNCTION_MAP =
      ImmutableMap.<String, Class<? extends WindowFunction>>builder()
          .putAll(RangeWindowFunction.WINDOW_FUNCTION_MAP)
          .putAll(ValueWindowFunction.WINDOW_FUNCTION_MAP)
          .build();

  public static WindowFunction construnctWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      WindowAggregateOperator.OrderSetInfo orderSetInfo) {
    String functionName = aggCall.getFunctionName();
    Class<? extends WindowFunction> windowFunctionClass =
        WINDOW_FUNCTION_MAP.getOrDefault(functionName, AggregateWindowFunction.class);
    try {
      Constructor<? extends WindowFunction> constructor =
          windowFunctionClass.getConstructor(RexExpression.FunctionCall.class, String.class, DataSchema.class,
              WindowAggregateOperator.OrderSetInfo.class);
      return constructor.newInstance(aggCall, functionName, inputSchema, orderSetInfo);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException("Failed to instantiate WindowFunction for function name: " + functionName, e);
    }
  }
}
