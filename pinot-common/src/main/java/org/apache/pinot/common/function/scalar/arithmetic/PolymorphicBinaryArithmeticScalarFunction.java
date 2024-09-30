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
 * Base class for polymorphic binary arithmetic scalar functions
 */
public abstract class PolymorphicBinaryArithmeticScalarFunction implements PinotScalarFunction {

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }

    return functionInfoForTypes(argumentTypes[0].getStoredType(), argumentTypes[1].getStoredType());
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

  private FunctionInfo functionInfoForTypes(ColumnDataType argumentType1, ColumnDataType argumentType2) {
    if ((argumentType1 == ColumnDataType.LONG || argumentType1 == ColumnDataType.INT) && (
        argumentType2 == ColumnDataType.LONG || argumentType2 == ColumnDataType.INT)) {
      return functionInfoForType(ColumnDataType.LONG);
    }

    // Fall back to double based comparison by default
    return functionInfoForType(ColumnDataType.DOUBLE);
  }

  /**
   * Get the binary arithmetic scalar function's {@link FunctionInfo} for the given argument type.
   */
  protected abstract FunctionInfo functionInfoForType(ColumnDataType argumentType);
}
