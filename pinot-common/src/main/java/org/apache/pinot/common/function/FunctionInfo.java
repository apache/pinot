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
import java.util.Objects;
import org.apache.pinot.spi.annotations.FunctionVolatility;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class FunctionInfo {
  private final Method _method;
  private final Class<?> _clazz;
  private final boolean _nullableParameters;
  private final boolean _deterministic;
  private final FunctionVolatility _volatility;

  public FunctionInfo(Method method, Class<?> clazz, boolean nullableParameters) {
    this(method, clazz, nullableParameters, true);
  }

  public FunctionInfo(Method method, Class<?> clazz, boolean nullableParameters, boolean deterministic) {
    this(method, clazz, nullableParameters, deterministic,
        deterministic ? resolveVolatility(method, clazz) : FunctionVolatility.VOLATILE);
  }

  public FunctionInfo(Method method, Class<?> clazz, boolean nullableParameters, boolean deterministic,
      FunctionVolatility volatility) {
    _method = method;
    _clazz = clazz;
    _nullableParameters = nullableParameters;
    _deterministic = deterministic;
    _volatility = Objects.requireNonNull(volatility, "volatility must not be null");
  }

  public Method getMethod() {
    return _method;
  }

  public Class<?> getClazz() {
    return _clazz;
  }

  public boolean hasNullableParameters() {
    return _nullableParameters;
  }

  public boolean isDeterministic() {
    return _deterministic;
  }

  public FunctionVolatility getVolatility() {
    return _volatility;
  }

  public static FunctionInfo fromMethod(Method method) {
    ScalarFunction annotation = method.getAnnotation(ScalarFunction.class);
    boolean nullableParameters = annotation != null && annotation.nullableParameters();
    boolean deterministic = annotation == null || annotation.isDeterministic();
    return new FunctionInfo(method, method.getDeclaringClass(), nullableParameters, deterministic);
  }

  private static FunctionVolatility resolveVolatility(Method method, Class<?> clazz) {
    ScalarFunction methodAnnotation = method.getAnnotation(ScalarFunction.class);
    ScalarFunction classAnnotation = clazz.getAnnotation(ScalarFunction.class);
    FunctionVolatility methodVolatility = getVolatility(methodAnnotation);
    FunctionVolatility classVolatility = getVolatility(classAnnotation);
    return mostVolatile(methodVolatility, classVolatility);
  }

  private static FunctionVolatility getVolatility(ScalarFunction annotation) {
    if (annotation == null) {
      return FunctionVolatility.IMMUTABLE;
    }
    return annotation.isDeterministic() ? annotation.volatility() : FunctionVolatility.VOLATILE;
  }

  private static FunctionVolatility mostVolatile(FunctionVolatility first, FunctionVolatility second) {
    if (first == FunctionVolatility.VOLATILE || second == FunctionVolatility.VOLATILE) {
      return FunctionVolatility.VOLATILE;
    }
    if (first == FunctionVolatility.STABLE || second == FunctionVolatility.STABLE) {
      return FunctionVolatility.STABLE;
    }
    return FunctionVolatility.IMMUTABLE;
  }
}
