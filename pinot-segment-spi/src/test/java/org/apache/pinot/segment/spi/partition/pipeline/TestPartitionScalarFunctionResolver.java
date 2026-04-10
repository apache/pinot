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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.function.scalar.PartitionFunctionExprRacyTestFunctions;
import org.apache.pinot.segment.spi.function.scalar.PartitionFunctionExprTestFunctions;
import org.apache.pinot.spi.annotations.ScalarFunction;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Test-only partition scalar-function resolver for the {@code pinot-segment-spi} module.
 */
public class TestPartitionScalarFunctionResolver implements PartitionScalarFunctionResolver {
  private static final Object DYNAMIC_ARGUMENT = new Object();
  private static final Map<String, List<ScalarFunctionMethod>> FUNCTION_METHODS = loadFunctionMethods();

  @Override
  public ResolvedFunction resolve(String functionName, List<Argument> arguments) {
    List<ScalarFunctionMethod> methods = FUNCTION_METHODS.get(canonicalize(functionName));
    if (methods == null) {
      throw new IllegalArgumentException(String.format("Unsupported partition scalar function: %s", functionName));
    }

    BoundFunction bestMatch = null;
    StringBuilder supportedSignatures = new StringBuilder();
    boolean sawNonDeterministicCandidate = false;
    boolean sawDeterministicCandidate = false;
    for (ScalarFunctionMethod method : methods) {
      if (supportedSignatures.length() > 0) {
        supportedSignatures.append(", ");
      }
      supportedSignatures.append(method.getSignature());

      if (!method.isDeterministic()) {
        sawNonDeterministicCandidate = true;
        continue;
      }
      sawDeterministicCandidate = true;

      BoundFunction candidate = method.bind(arguments);
      if (candidate != null) {
        if (bestMatch == null || candidate.getCost() < bestMatch.getCost()) {
          bestMatch = candidate;
        } else if (candidate.getCost() == bestMatch.getCost()) {
          throw new IllegalArgumentException(String.format(
              "Ambiguous partition scalar function '%s' for argument types (%s). Matching signatures: %s",
              functionName, formatArgumentTypes(arguments), supportedSignatures));
        }
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    }
    if (sawNonDeterministicCandidate && !sawDeterministicCandidate) {
      throw new IllegalArgumentException(String.format(
          "Partition scalar function '%s' is not allowed because it is non-deterministic", functionName));
    }
    throw new IllegalArgumentException(String.format(
        "Function '%s' does not accept argument types (%s). Supported signatures: %s", functionName,
        formatArgumentTypes(arguments), supportedSignatures));
  }

  private static Map<String, List<ScalarFunctionMethod>> loadFunctionMethods() {
    Map<String, List<ScalarFunctionMethod>> functionMethods = new HashMap<>();
    registerMethods(functionMethods, PartitionFunctionExprTestFunctions.class);
    registerMethods(functionMethods, PartitionFunctionExprRacyTestFunctions.class);

    Map<String, List<ScalarFunctionMethod>> immutable = new HashMap<>(functionMethods.size());
    for (Map.Entry<String, List<ScalarFunctionMethod>> entry : functionMethods.entrySet()) {
      immutable.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
    }
    return Collections.unmodifiableMap(immutable);
  }

  private static void registerMethods(Map<String, List<ScalarFunctionMethod>> functionMethods, Class<?> clazz) {
    for (Method method : clazz.getMethods()) {
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction == null || !scalarFunction.enabled() || !Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      ScalarFunctionMethod functionMethod = new ScalarFunctionMethod(method, scalarFunction.isDeterministic());
      Set<String> canonicalNames = new LinkedHashSet<>();
      canonicalNames.add(canonicalize(method.getName()));
      for (String name : scalarFunction.names()) {
        canonicalNames.add(canonicalize(name));
      }
      for (String canonicalName : canonicalNames) {
        functionMethods.computeIfAbsent(canonicalName, ignored -> new ArrayList<>()).add(functionMethod);
      }
    }
  }

  private static String canonicalize(String name) {
    return name.replace("_", "").toLowerCase(Locale.ROOT);
  }

  private static String formatArgumentTypes(List<Argument> arguments) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(arguments.get(i).getType());
    }
    return builder.toString();
  }

  private static int getDynamicConversionCost(PartitionValueType sourceType, Class<?> targetType) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    if (sourceType == targetValueType) {
      return 0;
    }
    switch (sourceType) {
      case STRING:
        if (targetValueType == PartitionValueType.BYTES || targetValueType.isNumeric()) {
          return 1;
        }
        return -1;
      case BYTES:
        return -1;
      case INT:
        return wideningNumericCost(targetValueType,
            EnumSet.of(PartitionValueType.LONG, PartitionValueType.FLOAT, PartitionValueType.DOUBLE));
      case LONG:
        return wideningNumericCost(targetValueType, EnumSet.of(PartitionValueType.DOUBLE));
      case FLOAT:
        return wideningNumericCost(targetValueType, EnumSet.of(PartitionValueType.DOUBLE));
      case DOUBLE:
      default:
        return -1;
    }
  }

  private static int wideningNumericCost(PartitionValueType targetType, Set<PartitionValueType> supportedTypes) {
    return supportedTypes.contains(targetType) ? 1 : -1;
  }

  private static int getConstantConversionCost(PartitionValue value, Class<?> targetType) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    PartitionValueType sourceType = value.getType();
    if (sourceType == targetValueType) {
      return 0;
    }
    switch (sourceType) {
      case STRING:
        if (targetValueType == PartitionValueType.BYTES || targetValueType.isNumeric()) {
          return 1;
        }
        return -1;
      case LONG:
        if (targetValueType == PartitionValueType.INT) {
          long longValue = value.getLongValue();
          return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE ? 2 : -1;
        }
        return getDynamicConversionCost(sourceType, targetType);
      case DOUBLE:
        if (targetValueType == PartitionValueType.INT) {
          double doubleValue = value.getDoubleValue();
          return doubleValue >= Integer.MIN_VALUE && doubleValue <= Integer.MAX_VALUE
              && doubleValue == Math.rint(doubleValue) ? 2 : -1;
        }
        if (targetValueType == PartitionValueType.LONG) {
          double doubleValue = value.getDoubleValue();
          return doubleValue >= Long.MIN_VALUE && doubleValue <= Long.MAX_VALUE
              && doubleValue == Math.rint(doubleValue) ? 2 : -1;
        }
        return getDynamicConversionCost(sourceType, targetType);
      default:
        return getDynamicConversionCost(sourceType, targetType);
    }
  }

  private static Object convertValue(PartitionValue value, Class<?> targetType, boolean constant) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    switch (targetValueType) {
      case STRING:
        Preconditions.checkArgument(value.getType() == PartitionValueType.STRING,
            "Expected STRING argument but got %s", value.getType());
        return value.getStringValue();
      case BYTES:
        if (value.getType() == PartitionValueType.BYTES) {
          return value.getBytesValue();
        }
        Preconditions.checkArgument(value.getType() == PartitionValueType.STRING,
            "Expected STRING or BYTES argument for BYTES parameter but got %s", value.getType());
        return value.getStringValue().getBytes(UTF_8);
      case INT:
        switch (value.getType()) {
          case INT:
            return value.getIntValue();
          case LONG:
            Preconditions.checkArgument(constant, "Cannot narrow LONG pipeline value to INT dynamically");
            return Math.toIntExact(value.getLongValue());
          case DOUBLE:
            Preconditions.checkArgument(constant, "Cannot narrow DOUBLE pipeline value to INT dynamically");
            return (int) value.getDoubleValue();
          case STRING:
            return Integer.parseInt(value.getStringValue());
          default:
            throw new IllegalArgumentException(String.format("Cannot convert %s partition value to INT",
                value.getType()));
        }
      case LONG:
        switch (value.getType()) {
          case INT:
            return (long) value.getIntValue();
          case LONG:
            return value.getLongValue();
          case DOUBLE:
            Preconditions.checkArgument(constant, "Cannot narrow DOUBLE pipeline value to LONG dynamically");
            return (long) value.getDoubleValue();
          case STRING:
            return Long.parseLong(value.getStringValue());
          default:
            throw new IllegalArgumentException(String.format("Cannot convert %s partition value to LONG",
                value.getType()));
        }
      case FLOAT:
        switch (value.getType()) {
          case INT:
            return (float) value.getIntValue();
          case LONG:
            return (float) value.getLongValue();
          case FLOAT:
            return value.getFloatValue();
          case STRING:
            return Float.parseFloat(value.getStringValue());
          default:
            throw new IllegalArgumentException(String.format("Cannot convert %s partition value to FLOAT",
                value.getType()));
        }
      case DOUBLE:
        switch (value.getType()) {
          case INT:
            return (double) value.getIntValue();
          case LONG:
            return (double) value.getLongValue();
          case FLOAT:
            return (double) value.getFloatValue();
          case DOUBLE:
            return value.getDoubleValue();
          case STRING:
            return Double.parseDouble(value.getStringValue());
          default:
            throw new IllegalArgumentException(String.format("Cannot convert %s partition value to DOUBLE",
                value.getType()));
        }
      default:
        throw new IllegalStateException("Unsupported partition parameter type: " + targetType);
    }
  }

  @Nullable
  private static Constructor<?> getEmptyConstructor(Class<?> clazz) {
    try {
      return clazz.getConstructor();
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private static final class ScalarFunctionMethod {
    private final Method _method;
    private final boolean _staticMethod;
    @Nullable
    private final ThreadLocal<Object> _threadLocalTarget;
    private final Class<?>[] _parameterTypes;
    private final boolean _varArgs;
    private final boolean _deterministic;
    private final PartitionValueType _outputType;
    private final String _signature;

    private ScalarFunctionMethod(Method method, boolean deterministic) {
      _method = method;
      _staticMethod = Modifier.isStatic(method.getModifiers());
      _parameterTypes = method.getParameterTypes();
      _varArgs = method.isVarArgs();
      _deterministic = deterministic;
      _outputType = PartitionValueType.fromJavaType(method.getReturnType());
      _signature = buildSignature(method);
      _threadLocalTarget = _staticMethod ? null : buildThreadLocalTarget(method);
    }

    public boolean isDeterministic() {
      return _deterministic;
    }

    public String getSignature() {
      return _signature;
    }

    @Nullable
    public BoundFunction bind(List<Argument> arguments) {
      int parameterCount = _parameterTypes.length;
      int fixedParameterCount = _varArgs ? parameterCount - 1 : parameterCount;
      if ((!_varArgs && arguments.size() != parameterCount) || (_varArgs && arguments.size() < fixedParameterCount)) {
        return null;
      }

      Object[] constantArguments = new Object[arguments.size()];
      int totalCost = 0;
      int dynamicIndex = -1;
      PartitionValueType inputType = PartitionValueType.STRING;
      Class<?> dynamicParameterType = String.class;
      for (int i = 0; i < arguments.size(); i++) {
        Argument argument = arguments.get(i);
        Class<?> parameterType = getParameterType(i);
        if (argument.isDynamic()) {
          if (dynamicIndex >= 0) {
            return null;
          }
          int cost = getDynamicConversionCost(argument.getType(), parameterType);
          if (cost < 0) {
            return null;
          }
          totalCost += cost;
          dynamicIndex = i;
          inputType = argument.getType();
          dynamicParameterType = parameterType;
          constantArguments[i] = DYNAMIC_ARGUMENT;
        } else {
          PartitionValue constantValue =
              Preconditions.checkNotNull(argument.getConstantValue(), "Constant argument must be configured");
          int cost = getConstantConversionCost(constantValue, parameterType);
          if (cost < 0) {
            return null;
          }
          totalCost += cost;
          constantArguments[i] = convertValue(constantValue, parameterType, true);
        }
      }
      return new BoundFunction(this, inputType, dynamicParameterType, dynamicIndex, constantArguments, totalCost);
    }

    public PartitionValueType getOutputType() {
      return _outputType;
    }

    private Class<?> getParameterType(int index) {
      if (_varArgs && index >= _parameterTypes.length - 1) {
        return _parameterTypes[_parameterTypes.length - 1].getComponentType();
      }
      return _parameterTypes[index];
    }

    private Object invoke(Object[] expressionArguments) {
      try {
        Object target = _staticMethod ? null : Preconditions.checkNotNull(_threadLocalTarget).get();
        return _method.invoke(target, adaptArguments(expressionArguments));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to invoke partition scalar function: " + _signature, e);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getTargetException();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new IllegalStateException("Caught checked exception while invoking partition scalar function: "
            + _signature, cause);
      }
    }

    private Object[] adaptArguments(Object[] expressionArguments) {
      if (!_varArgs) {
        return expressionArguments;
      }

      int fixedParameterCount = _parameterTypes.length - 1;
      Object[] methodArguments = new Object[_parameterTypes.length];
      for (int i = 0; i < fixedParameterCount; i++) {
        methodArguments[i] = expressionArguments[i];
      }
      Class<?> componentType = _parameterTypes[_parameterTypes.length - 1].getComponentType();
      int varArgCount = expressionArguments.length - fixedParameterCount;
      Object varArgArray = Array.newInstance(componentType, varArgCount);
      for (int i = 0; i < varArgCount; i++) {
        Array.set(varArgArray, i, expressionArguments[fixedParameterCount + i]);
      }
      methodArguments[_parameterTypes.length - 1] = varArgArray;
      return methodArguments;
    }

    private static ThreadLocal<Object> buildThreadLocalTarget(Method method) {
      Constructor<?> constructor = getEmptyConstructor(method.getDeclaringClass());
      Preconditions.checkState(constructor != null,
          "Non-static partition scalar function must have an empty constructor: %s", method);
      return ThreadLocal.withInitial(() -> instantiateTarget(constructor, method));
    }

    private static Object instantiateTarget(Constructor<?> constructor, Method method) {
      try {
        return constructor.newInstance();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to instantiate partition scalar function target: " + method, e);
      }
    }

    private static String buildSignature(Method method) {
      StringBuilder builder = new StringBuilder(method.getName()).append('(');
      Class<?>[] parameterTypes = method.getParameterTypes();
      for (int i = 0; i < parameterTypes.length; i++) {
        if (i > 0) {
          builder.append(", ");
        }
        Class<?> parameterType = parameterTypes[i];
        if (method.isVarArgs() && i == parameterTypes.length - 1) {
          builder.append(parameterType.getComponentType().getSimpleName()).append("...");
        } else {
          builder.append(parameterType.getSimpleName());
        }
      }
      return builder.append(')').toString();
    }
  }

  private static final class BoundFunction implements ResolvedFunction {
    private final ScalarFunctionMethod _method;
    private final PartitionValueType _inputType;
    private final Class<?> _dynamicParameterType;
    private final int _dynamicIndex;
    private final Object[] _constantArguments;
    @Nullable
    private final ThreadLocal<Object[]> _threadLocalArguments;
    private final int _cost;

    private BoundFunction(ScalarFunctionMethod method, PartitionValueType inputType, Class<?> dynamicParameterType,
        int dynamicIndex, Object[] constantArguments, int cost) {
      _method = method;
      _inputType = inputType;
      _dynamicParameterType = dynamicParameterType;
      _dynamicIndex = dynamicIndex;
      _constantArguments = constantArguments;
      _threadLocalArguments = dynamicIndex >= 0 ? ThreadLocal.withInitial(_constantArguments::clone) : null;
      _cost = cost;
    }

    @Override
    public int getCost() {
      return _cost;
    }

    @Override
    public boolean isDynamic() {
      return _dynamicIndex >= 0;
    }

    @Override
    public PartitionValueType getOutputType() {
      return _method.getOutputType();
    }

    @Override
    public PartitionValue invoke(@Nullable PartitionValue dynamicInput) {
      Object[] expressionArguments = _dynamicIndex >= 0 ? Preconditions.checkNotNull(_threadLocalArguments).get()
          : _constantArguments;
      if (_dynamicIndex >= 0) {
        Preconditions.checkNotNull(dynamicInput, "Dynamic partition step input must be configured");
        Preconditions.checkArgument(dynamicInput.getType() == _inputType,
            "Expected %s dynamic input but got %s", _inputType, dynamicInput.getType());
        expressionArguments[_dynamicIndex] = convertValue(dynamicInput, _dynamicParameterType, false);
      }
      return PartitionValue.fromObject(_method.invoke(expressionArguments));
    }
  }
}
