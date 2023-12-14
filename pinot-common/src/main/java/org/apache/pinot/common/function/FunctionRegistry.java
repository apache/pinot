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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.util.NameMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 */
public class FunctionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  // TODO: remove both when FunctionOperatorTable is in used.
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();
  private static final NameMultimap<PinotScalarFunction> FUNCTION_MAP = new NameMultimap<>();

  private FunctionRegistry() {
  }


  private static final int VAR_ARG_KEY = -1;

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
    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", FUNCTION_MAP.map().size(),
        FUNCTION_MAP.map().keySet(), System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  /**
   * Registers a method with the name of the method.
   */
  @VisibleForTesting
  public static void registerFunction(Method method, boolean nullableParameters) {
    registerFunction(method, Collections.singleton(method.getName()), nullableParameters);
  }

  private static void registerFunction(Method method, Set<String> alias, boolean nullableParameters) {
    if (method.getAnnotation(Deprecated.class) == null) {
      for (String name : alias) {
        registerFunctionInfoMap(name, method, nullableParameters);
        registerCalciteNamedFunctionMap(name, method, nullableParameters);
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

  private static void registerCalciteNamedFunctionMap(String name, Method method, boolean nullableParameters) {
    FUNCTION_MAP.put(name, new PinotScalarFunction(name, method, nullableParameters));
  }

  public static Map<String, List<PinotScalarFunction>> getRegisteredCalciteFunctionMap() {
    return FUNCTION_MAP.map();
  }

  public static Set<String> getRegisteredCalciteFunctionNames() {
    return FUNCTION_MAP.map().keySet();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    // TODO: remove fallback to FUNCTION_INFO_MAP
    return FUNCTION_MAP.containsKey(canonicalize(functionName), false)
        || FUNCTION_INFO_MAP.containsKey(canonicalize(functionName));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParameters) {
    // TODO: remove fallback to FUNCTION_INFO_MAP
    try {
      return getFunctionInfoFromCalciteNamedMap(functionName, numParameters);
    } catch (IllegalArgumentException iae) {
      return getFunctionInfoFromFunctionInfoMap(functionName, numParameters);
    }
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromFunctionInfoMap(String functionName, int numParameters) {
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(canonicalize(functionName));
    return functionInfoMap != null ? functionInfoMap.get(numParameters) : null;
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromCalciteNamedMap(String functionName, int numParameters) {
    List<PinotScalarFunction> candidates = findByNumParameters(FUNCTION_MAP.range(functionName, false), numParameters);
    if (candidates.size() <= 1) {
      return candidates.size() == 1 ? candidates.get(0).getFunctionInfo() : null;
    } else {
      throw new IllegalArgumentException(
          "Unable to lookup function: " + functionName + " by parameter count: " + numParameters
              + " Found multiple candidates. Try to use argument types to resolve the correct one!");
    }
  }

  private static List<PinotScalarFunction> findByNumParameters(
      Collection<Map.Entry<String, PinotScalarFunction>> scalarFunctionList, int numParameters) {
    return scalarFunctionList == null ? Collections.emptyList()
        : scalarFunctionList.stream().filter(e -> e.getValue().getParameters().size() == numParameters)
            .map(Map.Entry::getValue).collect(Collectors.toList());
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

  /**
   * Pinot specific implementation of the {@link org.apache.calcite.schema.ScalarFunction}.
   *
   * @see "{@link org.apache.calcite.schema.impl.ScalarFunctionImpl}"
   */
  public static class PinotScalarFunction extends ReflectiveFunctionBase
      implements org.apache.calcite.schema.ScalarFunction {
    private final FunctionInfo _functionInfo;
    private final String _name;
    private final Method _method;

    public PinotScalarFunction(String name, Method method, boolean isNullableParameter) {
      super(method);
      _name = name;
      _method = method;
      _functionInfo = new FunctionInfo(method, method.getDeclaringClass(), isNullableParameter);
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
  }
}
