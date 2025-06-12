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
package org.apache.pinot.common.function;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Provides finer control to the scalar functions annotated with {@link ScalarFunction}
 * <p>See more details in {@link FunctionRegistry}.
 */
public interface PinotScalarFunction {

  /**
   * Returns the name of the function.
   */
  String getName();

  /**
   * Returns the set of names of the function, including the primary name returned by {@link #getName()}.
   *
   * The value of this function may be used to register the function in the OperatorTable, although the names included
   * an optional {@link ScalarFunction}} annotation higher priority.
   */
  default Set<String> getNames() {
    return Set.of(getName());
  }

  /**
   * Returns the corresponding {@link PinotSqlFunction} to be registered into the OperatorTable, or {@code null} if it
   * doesn't need to be registered (e.g. standard SqlFunction).
   */
  @Nullable
  default PinotSqlFunction toPinotSqlFunction() {
    return null;
  }

  /**
   * Returns the {@link FunctionInfo} for the given argument types, or {@code null} if there is no matching.
   */
  @Nullable
  default FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    return getFunctionInfo(argumentTypes.length);
  }

  /**
   * Returns the {@link FunctionInfo} for the given number of arguments, or {@code null} if there is no matching.
   */
  @Nullable
  FunctionInfo getFunctionInfo(int numArguments);

  static Set<PinotScalarFunction> fromAnnotatedMethod(Method method) {
    ScalarFunction annotation = method.getAnnotation(ScalarFunction.class);
    if (annotation == null) {
      throw new IllegalArgumentException("Method " + method + " is not annotated with @ScalarFunction");
    }
    return fromMethod(method, annotation.isVarArg(), annotation.nullableParameters(), annotation.names());
  }

  static Set<PinotScalarFunction> fromMethod(Method method, boolean isVarArg, boolean supportNullArgs,
      @Nullable String... names) {
    int numArguments = isVarArg ? FunctionRegistry.VAR_ARG_KEY : method.getParameterCount();
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), supportNullArgs);
    Map<Integer, FunctionInfo> functionInfoMap = Map.of(numArguments, functionInfo);

    List<String> nameList = names != null && names.length > 0
        ? Arrays.asList(names)
        : List.of(method.getName());


    return nameList.stream()
        .map(FunctionRegistry::canonicalize)
        .distinct()
        .map(name -> new FunctionRegistry.ArgumentCountBasedScalarFunction(name, functionInfoMap))
        .collect(Collectors.toSet());
  }
}
