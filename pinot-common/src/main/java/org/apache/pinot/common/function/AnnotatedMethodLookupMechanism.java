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

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// This class implements a lookup mechanism for methods that extend [PinotScalarFunction] that are also annotated with
/// [ScalarFunction].
///
/// This mechanism is the first one that was implemented and it is very limited in terms of functionality. For example,
/// it does not support method overloading, so it is not possible to register multiple functions with the same name but
/// different argument types (ie summing int + int = int but also double + double = double). This was the reason why
/// before the introduction of other mechanisms, Pinot used doubles when applying most of the numeric UDFs.
///
/// Since then we introduced the [AnnotatedClassLookupMechanism] which supports method overloading and the UDF
/// mechanism, which is the preferred way to register functions in Pinot, as it can also be used to register
/// transform functions, create tests and documentation.
///
/// This is why any scalar function registered using this mechanism is registered using a very small negative priority,
///  which is lower than the default priority and also the priority of the [AnnotatedClassLookupMechanism].
@AutoService(FunctionRegistry.ScalarFunctionLookupMechanism.class)
public class AnnotatedMethodLookupMechanism implements FunctionRegistry.ScalarFunctionLookupMechanism {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnnotatedMethodLookupMechanism.class);
  /// The default priority for methods that are registered using this mechanism.
  public static final int DEFAULT_PRIORITY = -10_000;

  @Override
  public Set<ScalarFunctionProvider> getProviders() {
    Set<Method> methods = PinotReflectionUtils.getMethodsThroughReflection(".*\\.function\\..*",
        ScalarFunction.class);
    Map<String, Map<Integer, FunctionInfo>> functionInfoMap = new HashMap<>();
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) {
        LOGGER.debug("Skipping method {} because it is not public", method);
        continue;
      }
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (!scalarFunction.enabled()) {
        LOGGER.debug("Skipping method {} because the annotation says it is not enabled", method);
        continue;
      }

      FunctionInfo functionInfo =
          new FunctionInfo(method, method.getDeclaringClass(), scalarFunction.nullableParameters());
      int numArguments = scalarFunction.isVarArg() ? FunctionRegistry.VAR_ARG_KEY : method.getParameterCount();
      String[] names = scalarFunction.names();
      if (names.length == 0) {
        register(FunctionRegistry.canonicalize(method.getName()), functionInfo, numArguments, functionInfoMap);
      } else {
        Set<String> canonicalNames = new HashSet<>();
        for (String name : names) {
          if (!canonicalNames.add(FunctionRegistry.canonicalize(name))) {
            LOGGER.warn("Duplicate names: {} in method: {}", Arrays.toString(names), method);
          }
        }
        for (String canonicalName : canonicalNames) {
          register(canonicalName, functionInfo, numArguments, functionInfoMap);
        }
      }
    }

    ScalarFunctionProvider provider = new ScalarFunctionProvider() {
      @Override
      public String name() {
        return "annotated functions";
      }

      @Override
      public int priority() {
        return DEFAULT_PRIORITY; // Default priority for methods
      }

      @Override
      public Set<PinotScalarFunction> getFunctions() {
        HashSet<PinotScalarFunction> functions = Sets.newHashSetWithExpectedSize(functionInfoMap.size());
        for (Map.Entry<String, Map<Integer, FunctionInfo>> entry : functionInfoMap.entrySet()) {
          String canonicalName = entry.getKey();
          functions.add(new FunctionRegistry.ArgumentCountBasedScalarFunction(canonicalName, entry.getValue()));
        }
        return functions;
      }
    };
    return Set.of(provider);
  }

  /**
   * Registers a {@link FunctionInfo} under the given canonical name.
   */
  private static void register(String canonicalName, FunctionInfo functionInfo, int numArguments,
      Map<String, Map<Integer, FunctionInfo>> functionInfoMap) {
    Preconditions.checkState(
        functionInfoMap.computeIfAbsent(canonicalName, k -> new HashMap<>()).put(numArguments, functionInfo) == null,
        "Function: %s with %s arguments is already registered", canonicalName,
        numArguments == FunctionRegistry.VAR_ARG_KEY ? "variable" : numArguments);
  }
}
