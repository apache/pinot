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
package org.apache.pinot.common.function.scalar.bitwise;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Base class for polymorphic integral shift scalar functions.
 *
 * <p>Implementations are stateless and thread-safe.
 */
abstract class BaseShiftScalarFunction implements PinotScalarFunction {

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }
    ColumnDataType valueType = argumentTypes[0].getStoredType();
    ColumnDataType shiftType = argumentTypes[1].getStoredType();
    if (!BitFunctionUtils.isIntegral(shiftType)) {
      return null;
    }
    if (valueType == ColumnDataType.INT) {
      return intFunctionInfo();
    }
    if (valueType == ColumnDataType.LONG) {
      return longFunctionInfo();
    }
    return null;
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    // LONG overload as fallback for arity-only resolution (e.g. ingestion transforms).
    // INT inputs are sign-extended to LONG, which preserves values but uses 64-bit shift semantics.
    return numArguments == 2 ? longFunctionInfo() : null;
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return BitFunctionUtils.shiftSqlFunction(getName());
  }

  protected abstract FunctionInfo intFunctionInfo();

  protected abstract FunctionInfo longFunctionInfo();
}
