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
package org.apache.pinot.common.function.scalar.uuid;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Base class for UUID scalar functions that accept either a STRING or BYTES argument.
 * Subclasses provide the two {@link FunctionInfo} constants and delegate the
 * polymorphic dispatch here.
 */
abstract class AbstractStringOrBytesUuidFunction implements PinotScalarFunction {

  /** {@link FunctionInfo} for the {@code String} overload. */
  protected abstract FunctionInfo getStringFunctionInfo();

  /** {@link FunctionInfo} for the {@code byte[]} overload. */
  protected abstract FunctionInfo getBytesFunctionInfo();

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 1) {
      return null;
    }
    switch (argumentTypes[0]) {
      case STRING:
        return getStringFunctionInfo();
      case BYTES:
        return getBytesFunctionInfo();
      default:
        return null;
    }
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    return numArguments == 1 ? getStringFunctionInfo() : null;
  }
}
