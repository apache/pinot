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
package org.apache.pinot.common.function.schema;

import java.lang.reflect.Method;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.pinot.common.function.FunctionInfo;


/**
 * Pinot specific implementation of the {@link ScalarFunction}.
 *
 * @see "{@link org.apache.calcite.schema.impl.ScalarFunctionImpl}"
 */
public class PinotScalarFunction extends ReflectiveFunctionBase implements PinotFunction, ScalarFunction {
  private final FunctionInfo _functionInfo;
  private final String _name;
  private final Method _method;
  private final SqlOperandTypeChecker _sqlOperandTypeChecker;
  private final SqlReturnTypeInference _sqlReturnTypeInference;

  public PinotScalarFunction(String name, Method method, boolean isNullableParameter) {
    this(name, method, isNullableParameter, null, null);
  }

  public PinotScalarFunction(String name, Method method, boolean isNullableParameter,
    SqlOperandTypeChecker sqlOperandTypeChecker, SqlReturnTypeInference sqlReturnTypeInference) {
    super(method);
    _name = name;
    _method = method;
    _functionInfo = new FunctionInfo(method, method.getDeclaringClass(), isNullableParameter);
    _sqlOperandTypeChecker = sqlOperandTypeChecker;
    _sqlReturnTypeInference = sqlReturnTypeInference;
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(method.getReturnType());
  }

  public String getName() {
    return _name;
  }

  public Method getMethod() {
    return _method;
  }

  public FunctionInfo getFunctionInfo() {
    return _functionInfo;
  }

  @Override
  public SqlOperandTypeChecker getOperandTypeChecker() {
    return _sqlOperandTypeChecker;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return _sqlReturnTypeInference;
  }
}
