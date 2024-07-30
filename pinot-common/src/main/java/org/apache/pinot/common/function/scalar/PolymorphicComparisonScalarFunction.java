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
package org.apache.pinot.common.function.scalar;

import javax.annotation.Nullable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Base class for polymorphic comparison scalar functions
 */
public abstract class PolymorphicComparisonScalarFunction implements PinotScalarFunction {

  protected static final double DOUBLE_COMPARISON_TOLERANCE = 1e-7d;

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction(getName(), ReturnTypes.BOOLEAN_FORCE_NULLABLE, new SameOperandTypeChecker(2));
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(DataSchema.ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }

    // Only support comparing arguments of the same type. Explicit type casts should be added to compare arguments of
    // different types.
    if (argumentTypes[0] != argumentTypes[1]) {
      return null;
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
    return functionInfoForType(DataSchema.ColumnDataType.DOUBLE);
  }

  /**
   * Get the comparison scalar function's {@link FunctionInfo} for the given argument type. Comparison scalar functions
   * take two arguments of the same type.
   */
  protected abstract FunctionInfo functionInfoForType(DataSchema.ColumnDataType argumentType);
}
