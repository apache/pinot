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
import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for functions.
 *
 * <p>TODO: make it register generic FunctionDefinition, not just BuiltIn ones.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  /**
   * Map to store all the registered {@link BuiltInFunctionDefinition}s that wraps method an annotated by
   * {@link ScalarFunction}.
   *
   * <ul>
   *   <li>the first map key is the functionCanonicalName</li>
   *   <li>the second map is the function signature:
   *     <ul>
   *       <li>first {@link PinotDataType} of the function signature is the result type;</li>
   *       <li>the following {@link PinotDataType}s are all the argument types, if applicable.</li>
   *     </ul>
   *   </li>
   * </ul>
   */
  private static final Map<String, Map<Collection<PinotDataType>, BuiltInFunctionDefinition>>
      FUNCTION_INFO_MAP = new HashMap<>();

  /**
   * Map to store all the registered {@link FunctionDefinition} classes.
   */
  private static final Map<String, FunctionDefinition> FUNCTION_CLASS_MAP = new HashMap<>();

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
            .setScanners(new MethodAnnotationsScanner(), new TypeAnnotationsScanner(), new SubTypesScanner()));
    // STEP 1: register all ScalarFunctions
    /**
     * STEP 1.1 Register all {@link BuiltInFunctionDefinition}s (annotated methods)
     */
    Set<Method> methodSet = reflections.getMethodsAnnotatedWith(ScalarFunction.class);
    for (Method method : methodSet) {
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
            FunctionRegistry.registerMethod(name, method, nullableParameters);
          }
        } else {
          FunctionRegistry.registerMethod(method, nullableParameters);
        }
      }
    }
    Set<Class<?>> classSet = reflections.getTypesAnnotatedWith(ScalarFunction.class);
    for (Class<?> c : classSet) {
      if (FunctionDefinition.class.isAssignableFrom(c)) {
        @SuppressWarnings("unchecked")
        Class<? extends FunctionDefinition> clazz = (Class<? extends FunctionDefinition>) c;
        ScalarFunction scalarFunction = clazz.getAnnotation(ScalarFunction.class);
        if (scalarFunction.enabled()) {
          // Annotated function names
          String[] scalarFunctionNames = scalarFunction.names();
          boolean nullableParameters = scalarFunction.nullableParameters();
          if (scalarFunctionNames.length > 0) {
            for (String name : scalarFunctionNames) {
              FunctionRegistry.registerClass(name, clazz, nullableParameters);
            }
          } else {
            FunctionRegistry.registerClass(clazz, nullableParameters);
          }
        }
      }
    }
    /**
     * STEP 1.2 Register all extended {@link FunctionDefinition} (annotated classes)
     */
    //
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

  // --------------------------------------------------------------------------
  // Registration util - register functions at static startup time.
  // --------------------------------------------------------------------------

  /**
   * Registers a method with the name of the method.
   */
  public static void registerMethod(Method method, boolean nullableParameters) {
    registerMethod(method.getName(), method, nullableParameters);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerMethod(String functionName, Method method, boolean nullableParameters) {
    String canonicalName = canonicalize(functionName);
    BuiltInFunctionDefinition
        functionInfo = new BuiltInFunctionDefinition(functionName, method, method.getDeclaringClass(),
        nullableParameters);
    Map<Collection<PinotDataType>, BuiltInFunctionDefinition> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(
        canonicalName, k -> new HashMap<>());
    Preconditions.checkState(functionInfoMap.put(getSignature(functionInfo), functionInfo) == null,
        "Function: %s with (%s) -> %s is already registered", functionName,
        functionInfo.getArgumentTypes(), functionInfo.getResultType());
  }

  /**
   * Registers a method with the name of the method.
   */
  public static void registerClass(Class<? extends FunctionDefinition> clazz, boolean nullableParameters) {
    registerClass(clazz.getSimpleName(), clazz, nullableParameters);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerClass(String functionName, Class<? extends FunctionDefinition> clazz,
      boolean nullableParameters) {
    FunctionDefinition functionDefinition;
    try {
      functionDefinition = clazz.getConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Unable to register function definition class", e);
    }
    String canonicalName = canonicalize(functionName);
    Preconditions.checkState(FUNCTION_CLASS_MAP.put(canonicalName, functionDefinition) == null,
        "Function class: %s with is already registered", functionName);
  }

  // --------------------------------------------------------------------------
  // Retrieval util - retrieve function from the registry.
  // --------------------------------------------------------------------------

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    String canonicalizeName = canonicalize(functionName);
    return FUNCTION_INFO_MAP.containsKey(canonicalizeName) || FUNCTION_CLASS_MAP.containsKey(canonicalizeName);
  }

  /**
   * Returns the {@link BuiltInFunctionDefinition} associated with the given function name and number of parameters,
   * or {@code null} if there is no matching method. This method should be called after the FunctionRegistry is
   * initialized and all methods are already registered.
   */
  @Nullable
  @Deprecated
  public static BuiltInFunctionDefinition getFunctionInfo(String functionName, int numParameters) {
    Map<Collection<PinotDataType>, BuiltInFunctionDefinition> functionInfoMap = FUNCTION_INFO_MAP.get(
        canonicalize(functionName));
    if (functionInfoMap == null) {
      return null;
    }
    List<Collection<PinotDataType>> matchingKeys =
        functionInfoMap.keySet().stream().filter(k -> k.size() == numParameters + 1).collect(Collectors.toList());
    return matchingKeys.size() == 1 ? functionInfoMap.get(matchingKeys.get(0)) : null;
  }

  public static BuiltInFunctionDefinition getFunctionInfo(String functionName, Collection<PinotDataType> argumentTypes,
      PinotDataType resultType) {
    Map<Collection<PinotDataType>, BuiltInFunctionDefinition> functionInfoMap = FUNCTION_INFO_MAP.get(
        canonicalize(functionName));
    if (functionInfoMap == null) {
      return null;
    }
    List<PinotDataType> signature = Lists.asList(resultType, argumentTypes.toArray(new PinotDataType[]{}));
    return functionInfoMap.get(signature);
  }

  /**
   * Returns the {@link FunctionDefinition} class associated with the given function name, or {@code null} if there is
   * no matching function class. This method should be called after the FunctionRegistry is
   * initialized and all methods are already registered.
   */
  @Nullable
  public static FunctionDefinition getFunction(String functionName) {
    return FUNCTION_CLASS_MAP.get(canonicalize(functionName));
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  private static Collection<PinotDataType> getSignature(BuiltInFunctionDefinition functionInfo) {
    return Lists.asList(functionInfo.getResultType(), functionInfo.getArgumentTypes());
  }
}
