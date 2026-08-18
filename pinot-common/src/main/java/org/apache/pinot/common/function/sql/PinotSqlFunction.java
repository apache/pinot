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
package org.apache.pinot.common.function.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;


/// Pinot custom SqlFunction to be registered into SqlOperatorTable.
public class PinotSqlFunction extends SqlFunction {
  private final boolean _deterministic;
  private final boolean _isVolatile;

  public PinotSqlFunction(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, boolean deterministic, boolean isVolatile) {
    super(name.toUpperCase(), SqlKind.OTHER_FUNCTION, returnTypeInference, null, operandTypeChecker,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    _deterministic = deterministic;
    _isVolatile = isVolatile;
  }

  /// Derives volatility from determinism: a non-deterministic function is always volatile, and a deterministic one is
  /// assumed not to be.
  ///
  /// Only the second half is an assumption -- a deterministic function can still be
  /// `FunctionVolatility.VOLATILE` (that is exactly what `now()` is). Use the constructor above to say so explicitly;
  /// this overload is for operators that are plain immutable functions of their arguments.
  public PinotSqlFunction(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, boolean deterministic) {
    this(name, returnTypeInference, operandTypeChecker, deterministic, !deterministic);
  }

  public PinotSqlFunction(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    this(name, returnTypeInference, operandTypeChecker, true);
  }

  @Override
  public boolean isDeterministic() {
    return _deterministic;
  }

  /// Whether the function is `FunctionVolatility.VOLATILE`, i.e. its result can change on every invocation or it has
  /// side effects.
  ///
  /// This is independent of [#isDeterministic()], which is Pinot's compile-time-evaluation hint: `now()` is
  /// deterministic (so it can be constant-folded once at plan time) but volatile (so it must not be re-evaluated at a
  /// different point in the plan). `FunctionVolatility.STABLE` is not reported here, since a stable function is
  /// constant within a single query and is therefore safe to relocate.
  public boolean isVolatile() {
    return _isVolatile;
  }
}
