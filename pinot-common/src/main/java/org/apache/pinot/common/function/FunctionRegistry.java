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
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
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
import org.apache.pinot.spi.exception.BadQueryRequestException;
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
  private static final Map<String, Map<List<Class<?>>, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();
  // This FUNCTION_MAP is used by Calcite function catalog tolook up function by function signature.
  private static final NameMultimap<Function> FUNCTION_MAP = new NameMultimap<>();

  // checks casts between primitives
  private static final Map<Class<?>, Integer> WIDENING = ImmutableMap.<Class<?>, Integer>builder()
      .put(byte.class, 0)
      .put(short.class, 1)
      .put(char.class, 2)
      .put(int.class, 3)
      .put(long.class, 4)
      .put(float.class, 5)
      .put(double.class, 6)
      .build();

  /**
   * Indicates whether a type can be cast to another type, and (if it can be cast) the
   * strictness with which the cast was executed.
   *
   * <p><b>NOTE</b>: the ordering of the enumeration matters, enums with lower ordinals
   * are considered higher precedence to those with higher ordinals.
   */
  private enum CastType {
    /**
     * The types are equivalent to one another from a SQL perspective, but are represented
     * by different Java types
     */
    EQUIVALENCE_CAST,

    /**
     * The types can be cast to one another under general SQL type casting rules
     */
    STRICT_CAST,

    /**
     * The types can be "loosely" casted to one another under custom Pinot rules - this
     * includes implicit string casting and type narrowing
     */
    LOOSE_CAST,

    /**
     * Two types cannot be cast to one another
     */
    INVALID_CAST
  }

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
        boolean nullableParameters = scalarFunction.nullableParameters();
        boolean narrowing = scalarFunction.supportsNarrowing();
        boolean implicitStrings = scalarFunction.supportsImplicitStringCast();
        if (scalarFunctionNames.length > 0) {
          for (String name : scalarFunctionNames) {
            FunctionRegistry.registerFunction(name, method, nullableParameters, narrowing, implicitStrings);
          }
        } else {
          FunctionRegistry.registerFunction(method, nullableParameters, narrowing, implicitStrings);
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
  public static void registerFunction(Method method, boolean nullableParameters, boolean narrowing,
      boolean implicitStrings) {
    registerFunction(method.getName(), method, nullableParameters, narrowing, implicitStrings);

    // Calcite ScalarFunctionImpl doesn't allow customized named functions. TODO: fix me.
    if (method.getAnnotation(Deprecated.class) == null) {
      FUNCTION_MAP.put(method.getName(), ScalarFunctionImpl.create(method));
    }
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerFunction(String functionName, Method method, boolean nullableParameters, boolean narrowing,
      boolean implicitStrings) {
    FunctionInfo functionInfo = new FunctionInfo(
        method, method.getDeclaringClass(), nullableParameters, narrowing, implicitStrings);
    String canonicalName = canonicalize(functionName);
    Map<List<Class<?>>, FunctionInfo> functionInfoMap =
        FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());

    Preconditions.checkState(functionInfoMap.put(functionInfo.getParameterTypes(), functionInfo) == null,
        "Function: %s with %s parameters is already registered", functionName, functionInfo.getParameterTypes());
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
   * Returns the {@link FunctionInfo} associated with the given function name and signature, or {@code null}
   * if there is no matching method.
   *
   * <p>Note that operandTypes may include nulls, if the input at that position resolves to {@code null}.
   * In this case, the function must support {@link FunctionInfo#hasNullableParameters()}.
   *
   * <p>This method should be called after the FunctionRegistry is initialized and all methods
   * are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, List<Class<?>> operandTypes) {
    Map<List<Class<?>>, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(canonicalize(functionName));

    if (functionInfoMap == null) {
      return null;
    }

    // try to find exact match first
    FunctionInfo fun = functionInfoMap.get(operandTypes);
    if (fun != null) {
      return fun;
    }

    // find non-exact match, with nulls and casting
    EnumMap<CastType, List<FunctionInfo>> potentialMatches = new EnumMap<>(CastType.class);

    for (Map.Entry<List<Class<?>>, FunctionInfo> entry: functionInfoMap.entrySet()) {
      List<Class<?>> funTypes = entry.getKey();
      if (funTypes.size() != operandTypes.size()) {
        continue;
      }

      CastType match = CastType.INVALID_CAST;
      for (int i = 0; i < funTypes.size(); i++) {
        Class<?> opType = operandTypes.get(i);
        if (opType == null && entry.getValue().hasNullableParameters()) {
          match = CastType.STRICT_CAST;
          continue;
        } else if (opType == null) {
          break;
        }

        boolean stringCast = entry.getValue().isImplicitStrings();
        boolean narrowing = entry.getValue().isNarrowing();
        Class<?> funType = funTypes.get(i);

        CastType castType = canCast(opType, funType, stringCast, narrowing);
        if (castType == CastType.INVALID_CAST) {
          break;
        }
        match = castType.compareTo(match) < 0 ? castType : match;
      }

      if (match == CastType.EQUIVALENCE_CAST) {
        // we can early exist, we found an exact match that only differed
        // with wrapping of primitives
        return entry.getValue();
      } else if (match != CastType.INVALID_CAST) {
        potentialMatches.computeIfAbsent(match, k -> new ArrayList<>()).add(entry.getValue());
      }
    }

    for (List<FunctionInfo> functions : potentialMatches.values()) {
      // EnumMap maintains the natural order of iteration, so we know that the first
      // element we get will be the bets cast type
      if (functions.size() > 1) {
        throw new BadQueryRequestException(
            "Multiple equal-precedence matches for function " + functionName
                + " with operand types " + operandTypes
                + ": " + potentialMatches);
      }

      return functions.get(0);
    }

    // didn't find any match
    return null;
  }

  private static CastType canCast(Class<?> srcType, Class<?> destType, boolean stringCast, boolean narrowing) {
    Class<?> src = Primitives.isWrapperType(srcType) ? Primitives.unwrap(srcType) : srcType;
    Class<?> dest = Primitives.isWrapperType(destType) ? Primitives.unwrap(destType) : destType;

    if (src.equals(dest)) {
      return CastType.EQUIVALENCE_CAST;
    } else if (src.isAssignableFrom(srcType) || dest.equals(Object.class)) {
      // primitives fail the Object.class.isAssignableFrom(<primitive.class>) check, but
      // everything can be case to Object
      return CastType.STRICT_CAST;
    } else if (src.isPrimitive() && dest.isPrimitive() && (WIDENING.get(src) < WIDENING.get(dest))) {
      // SQL semantics will allow you to pass a widening option
      return CastType.STRICT_CAST;
    } else if (src.isPrimitive() && dest.isPrimitive() && narrowing) {
      // all primitives can be cast to one another if we allow narrowing, which was supported
      // by the original Pinot logic
      return CastType.LOOSE_CAST;
    } else if (destType.equals(byte[].class) && srcType.equals(String.class)) {
      // this is a custom conversion because internally we allow casting
      // directly from String to byte[] - literals created with a byte[]
      // are automatically
      return CastType.LOOSE_CAST;
    } else if (stringCast && (destType.equals(String.class) || srcType.equals(String.class))) {
      // this is for backwards compatibility - by default
      return CastType.LOOSE_CAST;
    }

    return CastType.INVALID_CAST;
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }
}
