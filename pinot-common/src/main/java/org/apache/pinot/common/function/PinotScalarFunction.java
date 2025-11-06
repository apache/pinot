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
   * Returns an identifier for the scalar function, which is used to identify the actual PinotScalarFunction class that
   * is registered in the FunctionRegistry.
   *
   * This was originally created to facilitate the debug process, so it is possible to identify the class that
   * implements a given scalar function. This is for example used to generate the all-functions.yml file, which lists
   * all the scalar and transform functions available in Pinot in order to test UDF implementations.
   *
   * See for example {@link FunctionRegistry.ArgumentCountBasedScalarFunction} which overrides this method to also
   * include the different FunctionInfo for the different argument counts.
   *
   * It is important that this method returns a stable identifier. This means it should not change unless we change the
   * class. For example, this should not be a call to {@link java.util.Objects#toIdentityString(Object)}, given it will
   * include the hash code of the object id, which will be different for each instance of the object. Instead, it should
   * depend on the class.
   */
  default String getScalarFunctionId() {
    return getClass().getCanonicalName();
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

  static PinotScalarFunction fromMethod(Method method, boolean isVarArg, boolean supportNullArgs,
      @Nullable String... names) {
    int numArguments = isVarArg ? FunctionRegistry.VAR_ARG_KEY : method.getParameterCount();
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), supportNullArgs);
    Map<Integer, FunctionInfo> functionInfoMap = Map.of(numArguments, functionInfo);

    List<String> nameList = names != null && names.length > 0
        ? Arrays.asList(names)
        : List.of(method.getName());

    return new FunctionRegistry.ArgumentCountBasedScalarFunction(nameList, functionInfoMap);
  }
}
