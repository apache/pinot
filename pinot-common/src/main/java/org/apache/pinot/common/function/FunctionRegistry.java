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

import com.google.common.collect.Iterables;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.NameMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  private static final NameMultimap<Function> FUNCTION_MAP = new NameMultimap<>();

  /*
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
        if (scalarFunctionNames.length == 0) {
          registerFunction(method);
        }

        for (String name : scalarFunctionNames) {
          FunctionRegistry.registerFunction(name, method);
        }
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

  public static void registerFunction(Method method) {
    registerFunction(method.getName(), method);
  }

  /**
   * Registers a method with the name of the method.
   */
  public static void registerFunction(String name, Method method) {
    if (method.getAnnotation(Deprecated.class) == null) {
      Function function = ScalarFunctionImpl.create(method);
      FUNCTION_MAP.put(name, function);
      if (!canonicalize(name).equals(name)) {
        // this is for backwards compatibility with V1 engine, which
        // always looks up case-insensitive names for functions but
        // case sensitive names for other identifiers (calcite does
        // not have an option to do that, it is either entirely case
        // sensitive or insensitive)
        FUNCTION_MAP.put(canonicalize(name), function);
      }
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
    return FUNCTION_MAP.containsKey(canonicalize(functionName), true);
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParameters) {
    Collection<Map.Entry<String, Function>> allFunctions = FUNCTION_MAP.range(canonicalize(functionName), true);
    if (allFunctions.isEmpty()) {
      return null;
    }

    List<Function> matchingFunctions = allFunctions
        .stream()
        .map(Map.Entry::getValue)
        .filter(fun -> fun.getParameters().size() == numParameters)
        .collect(Collectors.toList());
    if (matchingFunctions.size() > 1) {
      // this should never happen (yet) because we restrict the registering of multiple methods
      // with the same number of arguments and the same name
      throw new BadQueryRequestException(
          String.format("Could not resolve function %s with %s parameters as multiple were registered: %s",
              functionName,
              numParameters,
              matchingFunctions.stream().map(fun -> ((ScalarFunctionImpl) fun).method)
                  .map(Method::toString).collect(Collectors.joining(","))));
    } else if (matchingFunctions.isEmpty()) {
      return null;
    }

    return new FunctionInfo(Iterables.getOnlyElement(matchingFunctions));
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }
}
