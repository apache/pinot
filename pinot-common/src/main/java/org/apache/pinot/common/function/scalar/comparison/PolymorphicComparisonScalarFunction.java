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
package org.apache.pinot.common.function.scalar.comparison;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Base class for polymorphic comparison scalar functions
 */
public abstract class PolymorphicComparisonScalarFunction implements PinotScalarFunction {

  protected static final double DOUBLE_COMPARISON_TOLERANCE = 1e-7d;

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }

    // In case of heterogeneous argument types, fall back to double based comparison and allow FunctionInvoker to
    // convert argument types for v1 engine support.
    if (argumentTypes[0] != argumentTypes[1]) {
      return functionInfoForType(ColumnDataType.DOUBLE);
    }

    return functionInfoForType(argumentTypes[0].getStoredType());
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 2) {
      return null;
    }

    // For backward compatibility
    return functionInfoForType(ColumnDataType.DOUBLE);
  }

  /**
   * Get the comparison scalar function's {@link FunctionInfo} for the given argument type. Comparison scalar functions
   * take two arguments of the same type.
   */
  protected abstract FunctionInfo functionInfoForType(ColumnDataType argumentType);
}
