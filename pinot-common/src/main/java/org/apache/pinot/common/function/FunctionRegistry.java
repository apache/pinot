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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 *
 * The registry registers functions using canonical functionName and argument type as DataType.
 * It doesn't differentiate function name in different canonical forms or argument types whose DataType is the same such
 * as primitive numerical type and its wrapper class.
 *
 * To be backward compatible, the registry falls back functionName + param number matching when there are no type match
 * for parameters.
 * <p>TODO: Merge FunctionRegistry and FunctionDefinitionRegistry to provide one single registry for all functions.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  // Map from function name to parameter types to function info.
  private static final Map<String, Map<List<FieldSpec.DataType>, FunctionInfo>> FUNC_PARAM_INFO_MAP = new HashMap<>();

  // FUNCTION_INFO_MAP is still used to be backward compatible.
  // When we support all sql data types, we can deprecate this.
  @Deprecated
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();

  private static List<FieldSpec.DataType> getParamTypes(Class<?>[] types) {
    List<FieldSpec.DataType> paramTypes = new ArrayList<>();
    for (Class<?> t : types) {
      paramTypes.add(FunctionUtils.getDataType(t));
    }
    return paramTypes;
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   *
   * Assuming functionName is canonicalized.
   */
  @Nullable
  private static FunctionInfo getFunctionInfo(String functionName, int numParameters) {
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(functionName);
    return functionInfoMap != null ? functionInfoMap.get(numParameters) : null;
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
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
        // Annotated function names
        String[] scalarFunctionNames = scalarFunction.names();
        boolean nullableParameters = scalarFunction.nullableParameters();
        if (scalarFunctionNames.length > 0) {
          for (String name : scalarFunctionNames) {
            FunctionRegistry.registerFunction(name, method, nullableParameters);
          }
        } else {
          FunctionRegistry.registerFunction(method, nullableParameters);
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
  public static void registerFunction(Method method, boolean nullableParameters) {
    registerFunction(method.getName(), method, nullableParameters);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerFunction(String functionName, Method method, boolean nullableParameters) {
    final FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), nullableParameters);
    final String canonicalName = canonicalize(functionName);
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    // Only put one default implementation for # of params.
    if (!functionInfoMap.containsKey(method.getParameterCount())) {
      functionInfoMap.put(method.getParameterCount(), functionInfo);
    }
    List<FieldSpec.DataType> paramTypes = getParamTypes(method.getParameterTypes());
    Map<List<FieldSpec.DataType>, FunctionInfo> functionParamInfoMap =
        FUNC_PARAM_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    functionParamInfoMap.put(paramTypes, functionInfo);
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return FUNCTION_INFO_MAP.containsKey(canonicalize(functionName));
  }

  /**
   * All functions should be directly or indirectly registered via this call to ensure function name is canonical.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, List<FieldSpec.DataType> dataTypes) {
    String canonicalFunctionName = canonicalize(functionName);
    Map<List<FieldSpec.DataType>, FunctionInfo> paramMaps =
        FUNC_PARAM_INFO_MAP.getOrDefault(canonicalFunctionName, null);
    FunctionInfo info = paramMaps.getOrDefault(dataTypes, null);
    if (info == null) {
      return getFunctionInfo(canonicalFunctionName, dataTypes.size());
    }
    return info;
  }

  @Nullable
  public static FunctionInfo getFunctionInfo(Function function) {
    String functionName = function.getOperator();
    List<Expression> operands = function.getOperands();
    List<FieldSpec.DataType> args = new ArrayList<>();
    for (Expression exp : operands) {
      ExpressionContext ctx = ExpressionContext.forLiteralContext(exp.getLiteral());
      args.add(ctx.getLiteralContext().getType());
    }
    return getFunctionInfo(functionName, args);
  }

  @Nullable
  public static FunctionInfo getFunctionInfo(FunctionContext function) {
    List<ExpressionContext> args = function.getArguments();
    List<FieldSpec.DataType> argTypes = new ArrayList<>();
    for (ExpressionContext exp : args) {
      argTypes.add(exp.getLiteralContext().getType());
    }
    return getFunctionInfo(function.getFunctionName(), argTypes);
  }

  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, DataSchema.ColumnDataType[] argTypes) {
    List<FieldSpec.DataType> paramTypes = new ArrayList<>();
    for (DataSchema.ColumnDataType type : argTypes) {
      paramTypes.add(FunctionUtils.getDataType(type));
    }
    return getFunctionInfo(functionName, paramTypes);
  }
}
