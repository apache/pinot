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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.registry.PinotFunction;
import org.apache.pinot.common.function.registry.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotFunctionRegistry;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 */
public class FunctionRegistry {
  public static final boolean CASE_SENSITIVITY = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();

  private FunctionRegistry() {
  }

  /**
   * Registers the scalar functions via reflection.
   * NOTE: In order to plugin methods using reflection, the methods should be inside a class that includes ".function."
   *       in its class path. This convention can significantly reduce the time of class scanning.
   */
  static {
    long startTimeMs = System.currentTimeMillis();
    Set<Method> methods = PinotReflectionUtils.getMethodsThroughReflection(".*\\.function\\..*", ScalarFunction.class);
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction.enabled()) {
        // Parse annotated function names and alias
        Set<String> scalarFunctionNames = Arrays.stream(scalarFunction.names()).collect(Collectors.toSet());
        if (scalarFunctionNames.size() == 0) {
          scalarFunctionNames.add(method.getName());
        }
        boolean nullableParameters = scalarFunction.nullableParameters();
        FunctionRegistry.registerFunction(method, scalarFunctionNames, nullableParameters);
      }
    }
    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", FUNCTION_INFO_MAP.size(),
        FUNCTION_INFO_MAP.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  @VisibleForTesting
  public static void registerFunction(Method method, boolean nullableParameters) {
    registerFunction(method, Collections.singleton(method.getName()), nullableParameters);
  }

  @VisibleForTesting
  public static Set<String> getRegisteredCalciteFunctionNames() {
    return PinotFunctionRegistry.getFunctionMap().map().keySet();
  }

  /**
   * Returns the full list of all registered ScalarFunction to Calcite.
   */
  public static Map<String, List<PinotFunction>> getRegisteredCalciteFunctionMap() {
    return PinotFunctionRegistry.getFunctionMap().map();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    // TODO: remove deprecated FUNCTION_INFO_MAP
    return PinotFunctionRegistry.getFunctionMap().containsKey(functionName, CASE_SENSITIVITY)
        || FUNCTION_INFO_MAP.containsKey(canonicalize(functionName));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParams) {
    try {
      return getFunctionInfoFromCalciteNamedMap(functionName, numParams);
    } catch (IllegalArgumentException iae) {
      // TODO: remove deprecated FUNCTION_INFO_MAP
      return getFunctionInfoFromFunctionInfoMap(functionName, numParams);
    }
  }

  // TODO: remove deprecated FUNCTION_INFO_MAP
  private static void registerFunction(Method method, Set<String> alias, boolean nullableParameters) {
    if (method.getAnnotation(Deprecated.class) == null) {
      for (String name : alias) {
        registerFunctionInfoMap(name, method, nullableParameters);
      }
    }
  }

  private static void registerFunctionInfoMap(String functionName, Method method, boolean nullableParameters) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), nullableParameters);
    String canonicalName = canonicalize(functionName);
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    FunctionInfo existFunctionInfo = functionInfoMap.put(method.getParameterCount(), functionInfo);
    Preconditions.checkState(existFunctionInfo == null || existFunctionInfo.getMethod() == functionInfo.getMethod(),
        "Function: %s with %s parameters is already registered", functionName, method.getParameterCount());
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromFunctionInfoMap(String functionName, int numParams) {
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(canonicalize(functionName));
    return functionInfoMap != null ? functionInfoMap.get(numParams) : null;
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromCalciteNamedMap(String functionName, int numParams) {
    List<PinotScalarFunction> candidates = PinotFunctionRegistry.getFunctionMap()
        .range(functionName, CASE_SENSITIVITY).stream()
        .filter(e -> e.getValue() instanceof PinotScalarFunction && e.getValue().getParameters().size() == numParams)
        .map(e -> (PinotScalarFunction) e.getValue()).collect(Collectors.toList());
    if (candidates.size() == 1) {
      return candidates.get(0).getFunctionInfo();
    } else {
      throw new IllegalArgumentException(
          "Unable to lookup function: " + functionName + " by parameter count: " + numParams + " Found "
              + candidates.size() + " candidates. Try to use argument types to resolve the correct one!");
    }
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  /**
   * Placeholders for scalar function, they register and represents the signature for transform and filter predicate
   * so that v2 engine can understand and plan them correctly.
   */
  private static class PlaceholderScalarFunctions {

    @ScalarFunction(names = {"textContains", "text_contains"}, isPlaceholder = true)
    public static boolean textContains(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"textMatch", "text_match"}, isPlaceholder = true)
    public static boolean textMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonMatch", "json_match"}, isPlaceholder = true)
    public static boolean jsonMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"vectorSimilarity", "vector_similarity"}, isPlaceholder = true)
    public static double vectorSimilarity(float[] vector1, float[] vector2) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }
  }
}
