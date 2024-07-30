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
package org.apache.pinot.core.query.postaggregation;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.PinotDataType;


/**
 * Post-aggregation function on the annotated scalar function.
 */
public class PostAggregationFunction {
  private final FunctionInvoker _functionInvoker;
  private final PinotDataType[] _argumentTypes;
  private final ColumnDataType _resultType;

  public PostAggregationFunction(String functionName, ColumnDataType[] argumentTypes) {
    String canonicalName = FunctionRegistry.canonicalize(functionName);
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes);
    if (functionInfo == null) {
      if (FunctionRegistry.contains(canonicalName)) {
        throw new IllegalArgumentException(
            String.format("Unsupported function: %s with argument types: %s", functionName,
                Arrays.toString(argumentTypes)));
      } else {
        throw new IllegalArgumentException(String.format("Unsupported function: %s", functionName));
      }
    }
    _functionInvoker = new FunctionInvoker(functionInfo);
    Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    int numArguments = argumentTypes.length;
    int numParameters = parameterClasses.length;
    Preconditions.checkArgument(numArguments == numParameters,
        "Wrong number of arguments for method: %s, expected: %s, actual: %s", functionInfo.getMethod(), numParameters,
        numArguments);
    for (int i = 0; i < numParameters; i++) {
      Preconditions.checkArgument(parameterTypes[i] != null, "Unsupported parameter class: %s for method: %s",
          parameterClasses[i], functionInfo.getMethod());
    }
    _argumentTypes = new PinotDataType[numArguments];
    for (int i = 0; i < numArguments; i++) {
      _argumentTypes[i] = PinotDataType.getPinotDataTypeForExecution(argumentTypes[i]);
    }
    ColumnDataType resultType = FunctionUtils.getColumnDataType(_functionInvoker.getResultClass());
    // Handle unrecognized result class with STRING
    _resultType = resultType != null ? resultType : ColumnDataType.STRING;
  }

  /**
   * Returns the ColumnDataType of the result.
   */
  public ColumnDataType getResultType() {
    return _resultType;
  }

  /**
   * Invoke the function with the given arguments.
   * NOTE: The passed in arguments could be modified during the type conversion.
   */
  public Object invoke(Object[] arguments) {
    int numArguments = arguments.length;
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < numArguments; i++) {
      PinotDataType parameterType = parameterTypes[i];
      PinotDataType argumentType = _argumentTypes[i];
      if (parameterType != argumentType) {
        arguments[i] = parameterType.convert(arguments[i], argumentType);
      }
    }
    Object result = _functionInvoker.invoke(arguments);
    return _resultType == ColumnDataType.STRING ? result.toString() : result;
  }
}
