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

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/// Base class for UUID semantic functions that accept STRING, BYTES, or UUID.
///
/// The overload is selected from the logical argument type. For evaluators that only expose the
/// argument count, the UUID overload is used by default and Pinot's standard type conversion bridges
/// STRING and BYTES storage into the logical UUID value. Validators can override that default to
/// preserve false-on-invalid behavior.
///
/// Implementations are stateless and thread-safe.
abstract class AbstractUuidInputFunction implements PinotScalarFunction {
  private static final SqlOperandTypeChecker UUID_INPUT_TYPE_CHECKER =
      OperandTypes.or(OperandTypes.family(List.of(SqlTypeFamily.CHARACTER)),
          OperandTypes.family(List.of(SqlTypeFamily.BINARY)),
          OperandTypes.family(List.of(SqlTypeFamily.UUID)));

  private final String _name;
  private final SqlTypeName _returnType;
  private final FunctionInfo _stringFunctionInfo;
  private final FunctionInfo _bytesFunctionInfo;
  private final FunctionInfo _uuidFunctionInfo;

  protected AbstractUuidInputFunction(Class<?> functionClass, String name, String methodName, SqlTypeName returnType) {
    _name = name;
    _returnType = returnType;
    try {
      _stringFunctionInfo =
          new FunctionInfo(functionClass.getMethod(methodName, String.class), functionClass, false);
      _bytesFunctionInfo =
          new FunctionInfo(functionClass.getMethod(methodName, byte[].class), functionClass, false);
      _uuidFunctionInfo =
          new FunctionInfo(functionClass.getMethod(methodName, UUID.class), functionClass, false);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Missing UUID input overload for function: " + name, e);
    }
  }

  @Override
  public final String getName() {
    return _name;
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 1) {
      return null;
    }
    switch (argumentTypes[0]) {
      case STRING:
        return _stringFunctionInfo;
      case BYTES:
        return _bytesFunctionInfo;
      case UUID:
        // UUID columns are stored as fixed-width BYTES. Keep them byte-backed on the query path so scalar
        // evaluation does not materialize one java.util.UUID object per row.
        return _bytesFunctionInfo;
      default:
        return null;
    }
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    return numArguments == 1 ? getDefaultFunctionInfo() : null;
  }

  protected FunctionInfo getDefaultFunctionInfo() {
    return _uuidFunctionInfo;
  }

  protected final FunctionInfo getStringFunctionInfo() {
    return _stringFunctionInfo;
  }

  @Override
  public final PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction(_name, opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      RelDataType type = typeFactory.createSqlType(_returnType);
      boolean nullable = opBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
      return typeFactory.createTypeWithNullability(type, nullable);
    }, UUID_INPUT_TYPE_CHECKER);
  }
}
