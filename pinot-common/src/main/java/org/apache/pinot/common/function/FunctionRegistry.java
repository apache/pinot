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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
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
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();

  /**
   * Registers the scalar functions via reflection.
   * NOTE: In order to plugin methods using reflection, the methods should be inside a class that includes ".function."
   *       in its class path. This convention can significantly reduce the time of class scanning.
   */
  static {
    long startTimeMs = System.currentTimeMillis();
    Reflections reflections = new Reflections(
        new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
            .filterInputsBy(new FilterBuilder.Include(".*\\.function\\..*"))
            .setScanners(new MethodAnnotationsScanner()));
    Set<Method> methodSet = reflections.getMethodsAnnotatedWith(ScalarFunction.class);
    for (Method method : methodSet) {
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction.enabled()) {
        if (!scalarFunction.name().isEmpty()) {
          FunctionRegistry.registerFunction(scalarFunction.name(), method);
        } else {
          FunctionRegistry.registerFunction(method);
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
  public static void registerFunction(Method method) {
    registerFunction(method.getName(), method);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerFunction(String functionName, Method method) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    String canonicalName = canonicalize(functionName);
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    Preconditions.checkState(functionInfoMap.put(method.getParameterCount(), functionInfo) == null,
        "Function: %s with %s parameters is already registered", functionName, method.getParameterCount());
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
}
