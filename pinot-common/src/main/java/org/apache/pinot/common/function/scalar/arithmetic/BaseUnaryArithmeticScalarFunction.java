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
package org.apache.pinot.common.function.scalar.arithmetic;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Base class for unary arithmetic scalar functions.
 *
 * <p>Instances are immutable and thread-safe.
 */
public abstract class BaseUnaryArithmeticScalarFunction implements PinotScalarFunction {

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 1) {
      return null;
    }

    FunctionInfo functionInfo = functionInfoForType(argumentTypes[0].getStoredType());
    return functionInfo != null ? functionInfo : defaultFunctionInfo();
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 1) {
      return null;
    }

    // For backward compatibility
    return defaultFunctionInfo();
  }

  /**
   * Get the unary arithmetic scalar function's {@link FunctionInfo} for the given argument type.
   */
  protected abstract FunctionInfo functionInfoForType(ColumnDataType argumentType);

  protected FunctionInfo defaultFunctionInfo() {
    return functionInfoForType(ColumnDataType.DOUBLE);
  }
}
