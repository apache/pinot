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

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.NameMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 * <p>TODO: Merge FunctionRegistry and FunctionDefinitionRegistry to provide one single registry for all functions.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  // TODO: consolidate the following 2
  // This FUNCTION_INFO_MAP is used by Pinot server to look up function by # of arguments
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();
  // This FUNCTION_MAP is used by Calcite function catalog to look up function by function signature.
  private static final NameMultimap<Function> FUNCTION_MAP = new NameMultimap<>();

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
        // Annotated function names
        String[] scalarFunctionNames = scalarFunction.names();
        boolean nullableParameters = scalarFunction.nullableParameters();
        if (scalarFunctionNames.length > 0) {
          for (String name : scalarFunctionNames) {
            FunctionRegistry.registerFunction(name, method, nullableParameters, scalarFunction.isPlaceholder());
          }
        } else {
          FunctionRegistry.registerFunction(method, nullableParameters, scalarFunction.isPlaceholder());
        }
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

  /**
   * Registers a method with the name of the method.
   */
  public static void registerFunction(Method method, boolean nullableParameters, boolean isPlaceholder) {
    registerFunction(method.getName(), method, nullableParameters, isPlaceholder);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerFunction(String functionName, Method method, boolean nullableParameters,
      boolean isPlaceholder) {
    if (!isPlaceholder) {
      registerFunctionInfoMap(functionName, method, nullableParameters);
    }
    registerCalciteNamedFunctionMap(functionName, method, nullableParameters);
  }

  private static void registerFunctionInfoMap(String functionName, Method method, boolean nullableParameters) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), nullableParameters);
    String canonicalName = canonicalize(functionName);
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    FunctionInfo existFunctionInfo = functionInfoMap.put(method.getParameterCount(), functionInfo);
    Preconditions.checkState(existFunctionInfo == null || existFunctionInfo.getMethod() == functionInfo.getMethod(),
        "Function: %s with %s parameters is already registered", functionName, method.getParameterCount());
  }

  private static void registerCalciteNamedFunctionMap(String functionName, Method method, boolean nullableParameters) {
    if (method.getAnnotation(Deprecated.class) == null) {
      FUNCTION_MAP.put(functionName, ScalarFunctionImpl.create(method));
    }
  }

  public static Map<String, List<Function>> getRegisteredCalciteFunctionMap() {
    return FUNCTION_MAP.map();
  }

  public static Collection<Function> getRegisteredCalciteFunctions(String name) {
    return FUNCTION_MAP.map().get(name);
  }

  public static Set<String> getRegisteredCalciteFunctionNames() {
    return FUNCTION_MAP.map().keySet();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return FUNCTION_INFO_MAP.containsKey(canonicalize(functionName));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParameters) {
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(canonicalize(functionName));
    return functionInfoMap != null ? functionInfoMap.get(numParameters) : null;
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  /**
   * Placeholders for scalar function, they register and represents the signature for transform and filter predicate
   * so that v2 engine can understand and plan them correctly.
   */
  private static class PlaceholderScalarFunctions {

    /**
     * Noted that {@code dateTimeConvert} with String as first input is actually supported.
     *
     * @see org.apache.pinot.common.function.scalar.DateTimeConvert#dateTimeConvert(String, String, String, String)
     */
    @ScalarFunction(names = {"dateTimeConvert", "date_time_convert"}, isPlaceholder = true)
    public static String dateTimeConvert(long timeValueNumeric, String inputFormatStr, String outputFormatStr,
        String outputGranularityStr) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonExtractScalar", "json_extract_scalar"}, isPlaceholder = true)
    public static Object jsonExtractScalar(String jsonFieldName, String jsonPath, String resultsType) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonExtractScalar", "json_extract_scalar"}, isPlaceholder = true)
    public static Object jsonExtractScalar(String jsonFieldName, String jsonPath, String resultsType,
        Object defaultValue) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonExtractKey", "json_extract_key"}, isPlaceholder = true)
    public static String jsonExtractKey(String jsonFieldName, String jsonPath) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

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

    @ScalarFunction(names = {"clpDecode", "clp_decode"}, isPlaceholder = true)
    public static Object clpDecode(String logtypeFieldName, String dictVarsFieldName, String encodedVarsFieldName) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"clpDecode", "clp_decode"}, isPlaceholder = true)
    public static Object clpDecode(String logtypeFieldName, String dictVarsFieldName, String encodedVarsFieldName,
        String defaultValue) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }
  }
}
