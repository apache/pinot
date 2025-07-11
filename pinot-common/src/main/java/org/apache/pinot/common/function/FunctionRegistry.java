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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 *
 * <p>To plug in a class:
 * <ul>
 *   <li>It should be annotated with {@link ScalarFunction}</li>
 *   <li>It should implement {@link PinotScalarFunction}</li>
 *   <li>It should be public and has an empty constructor</li>
 *   <li>It should be under the package of name '*.function.*'</li>
 * </ul>
 * <p>To plug in a method:
 * <ul>
 *   <li>It should be annotated with {@link ScalarFunction}</li>
 *   <li>It should be public</li>
 *   <li>It should be either static, or within a class with an empty constructor</li>
 *   <li>It should be within a class under the package of name '*.function.*'</li>
 * </ul>
 * <p>Multiple methods with different number of arguments can be registered under the same canonical name. Otherwise,
 * each canonical name can only be registered once.
 * <p>Class implementing {@link PinotScalarFunction} gives finer control on return type inference and operand type
 * check, and allows polymorphism based on the argument types.
 * <p>Method is easier to implement but has less control. If different return type inference or operand type check is
 * desired over the default java class inference, they can be directly registered into {@code PinotOperatorTable}.
 * <p>The package name convention is used to reduce the time of class scanning.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  // Key is canonical name
  public static final Map<String, PinotScalarFunction> FUNCTION_MAP;

  public static final int VAR_ARG_KEY = -1;

  static {
    long startTimeMs = System.currentTimeMillis();

    FUNCTION_MAP = fromServiceLoader();

    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", FUNCTION_MAP.size(),
        FUNCTION_MAP.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  private static Map<String, PinotScalarFunction> fromServiceLoader() {
    Map<String, ScalarFunctionLookupMechanism.ScalarFunctionProvider> providerByFunName = new HashMap<>();

    Map<String, PinotScalarFunction> functionMap = new HashMap<>();
    for (ScalarFunctionLookupMechanism mechanism : ServiceLoader.load(ScalarFunctionLookupMechanism.class)) {
      for (ScalarFunctionLookupMechanism.ScalarFunctionProvider provider : mechanism.getProviders()) {
        int priority = provider.priority();

        for (PinotScalarFunction function : provider.getFunctions()) {
          for (String name : function.getNames()) {
            String canonicalName = canonicalize(name);
            ScalarFunctionLookupMechanism.ScalarFunctionProvider oldValue = providerByFunName.get(canonicalName);
            if (oldValue == null) {
              providerByFunName.put(canonicalName, provider);
              functionMap.put(canonicalName, function);
              LOGGER.debug("Registered function: {} using {} provider", canonicalName, provider.name());
            } else {
              if (oldValue.priority() < priority) {
                providerByFunName.put(canonicalName, provider);
                functionMap.put(canonicalName, function);
                LOGGER.info("Function: {} already registered with a lower priority provider {}, replacing it with {}",
                    canonicalName, oldValue.name(), provider.name());
              } else {
                LOGGER.info("Function: {} already registered with a higher or equal priority using {}, skipping "
                        + "definitions from  {}",
                    canonicalName, oldValue.name(), provider.name());
              }
            }
          }
        }
      }
    }
    return functionMap;
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  /**
   * Registers a {@link PinotScalarFunction} under the given canonical name.
   */
  private static void register(String canonicalName, PinotScalarFunction function,
      Map<String, PinotScalarFunction> functionMap) {
    Preconditions.checkState(functionMap.put(canonicalName, function) == null, "Function: %s is already registered",
        canonicalName);
  }

  public static Map<String, PinotScalarFunction> getFunctions() {
    return Collections.unmodifiableMap(FUNCTION_MAP);
  }

  /**
   * Returns {@code true} if the given canonical name is registered, {@code false} otherwise.
   *
   * TODO: Consider adding a way to look up the usage of a function for better error message when there is no matching
   *       FunctionInfo.
   */
  public static boolean contains(String canonicalName) {
    return FUNCTION_MAP.containsKey(canonicalName);
  }

  /**
   * @deprecated For performance concern, use {@link #contains(String)} instead to avoid invoking
   *             {@link #canonicalize(String)} multiple times.
   */
  @Deprecated
  public static boolean containsFunction(String name) {
    return contains(canonicalize(name));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given canonical name and argument types, or {@code null} if
   * there is no matching method. This method should be called after the FunctionRegistry is initialized and all methods
   * are already registered.
   */
  @Nullable
  public static FunctionInfo lookupFunctionInfo(String canonicalName, ColumnDataType[] argumentTypes) {
    PinotScalarFunction function = FUNCTION_MAP.get(canonicalName);
    return function != null ? function.getFunctionInfo(argumentTypes) : null;
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given canonical name and number of arguments, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   * TODO: Move all usages to {@link #lookupFunctionInfo(String, ColumnDataType[])}.
   */
  @Nullable
  public static FunctionInfo lookupFunctionInfo(String canonicalName, int numArguments) {
    PinotScalarFunction function = FUNCTION_MAP.get(canonicalName);
    return function != null ? function.getFunctionInfo(numArguments) : null;
  }

  /**
   * @deprecated For performance concern, use {@link #lookupFunctionInfo(String, int)} instead to avoid invoking
   *             {@link #canonicalize(String)} multiple times.
   */
  @Deprecated
  @Nullable
  public static FunctionInfo getFunctionInfo(String name, int numArguments) {
    return lookupFunctionInfo(canonicalize(name), numArguments);
  }

  public static String canonicalize(String name) {
    return StringUtils.remove(name, '_').toLowerCase();
  }

  public static class ArgumentCountBasedScalarFunction implements PinotScalarFunction {
    private final String _name;
    private final Map<Integer, FunctionInfo> _functionInfoMap;

    public ArgumentCountBasedScalarFunction(String name, Map<Integer, FunctionInfo> functionInfoMap) {
      _name = name;
      _functionInfoMap = functionInfoMap;
    }

    @Override
    public String getName() {
      return _name;
    }

    @Override
    public PinotSqlFunction toPinotSqlFunction() {
      return new PinotSqlFunction(_name, getReturnTypeInference(), getOperandTypeChecker());
    }

    private SqlReturnTypeInference getReturnTypeInference() {
      return opBinding -> {
        int numArguments = opBinding.getOperandCount();
        FunctionInfo functionInfo = getFunctionInfo(numArguments);
        Preconditions.checkState(functionInfo != null, "Failed to find function: %s with %s arguments", _name,
            numArguments);
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        Method method = functionInfo.getMethod();
        RelDataType returnType = FunctionUtils.getRelDataType(opBinding.getTypeFactory(), method.getReturnType());

        if (!functionInfo.hasNullableParameters()) {
          // When any parameter is null, return is null
          for (RelDataType type : opBinding.collectOperandTypes()) {
            if (type.isNullable()) {
              return typeFactory.createTypeWithNullability(returnType, true);
            }
          }
        }

        return method.isAnnotationPresent(Nullable.class) ? typeFactory.createTypeWithNullability(returnType, true)
            : returnType;
      };
    }

    private SqlOperandTypeChecker getOperandTypeChecker() {
      if (_functionInfoMap.containsKey(VAR_ARG_KEY)) {
        return OperandTypes.VARIADIC;
      }
      int numCheckers = _functionInfoMap.size();
      if (numCheckers == 1) {
        return getOperandTypeChecker(_functionInfoMap.values().iterator().next().getMethod());
      }
      SqlOperandTypeChecker[] operandTypeCheckers = new SqlOperandTypeChecker[numCheckers];
      int index = 0;
      for (FunctionInfo functionInfo : _functionInfoMap.values()) {
        operandTypeCheckers[index++] = getOperandTypeChecker(functionInfo.getMethod());
      }
      return OperandTypes.or(operandTypeCheckers);
    }

    private static SqlOperandTypeChecker getOperandTypeChecker(Method method) {
      Class<?>[] parameterTypes = method.getParameterTypes();
      int length = parameterTypes.length;
      SqlTypeFamily[] typeFamilies = new SqlTypeFamily[length];
      for (int i = 0; i < length; i++) {
        typeFamilies[i] = getSqlTypeFamily(parameterTypes[i]);
      }
      return OperandTypes.family(typeFamilies);
    }

    private static SqlTypeFamily getSqlTypeFamily(Class<?> clazz) {
      // NOTE: Pinot allows some non-standard type conversions such as Timestamp <-> long, boolean <-> int etc. Do not
      //       restrict the type family for now. We only restrict the type family for String so that cast can be added.
      //       Explicit cast is required to correctly convert boolean and Timestamp to String. Without explicit cast,
      //       BOOLEAN and TIMESTAMP type will be converted with their internal stored format which is INT and LONG
      //       respectively. E.g. true will be converted to "1", timestamp will be converted to long value string.
      // TODO: Revisit this.
      return clazz == String.class ? SqlTypeFamily.CHARACTER : SqlTypeFamily.ANY;
    }

    @Nullable
    @Override
    public FunctionInfo getFunctionInfo(int numArguments) {
      FunctionInfo functionInfo = _functionInfoMap.get(numArguments);
      return functionInfo != null ? functionInfo : _functionInfoMap.get(VAR_ARG_KEY);
    }

    @Override
    public String toString() {
      if (_functionInfoMap.size() == 1) {
        String singleFunInfo = printFunctionInfo(_functionInfoMap.values().iterator().next());
        return "ArgumentCountBasedScalarFunction{" + singleFunInfo + "}";
      }
      String mapDescription = _functionInfoMap.entrySet().stream()
          .map(pair -> pair.getKey() + ": " + printFunctionInfo(pair.getValue()))
          .collect(Collectors.joining(", ", "[", "]"));
      return "ArgumentCountBasedScalarFunction{" + mapDescription + "}";
    }

    private String printFunctionInfo(FunctionInfo functionInfo) {
      Method method = functionInfo.getMethod();
      return method.getDeclaringClass().getTypeName() + '.' + method.getName();
    }
  }

  /**
   * Interface for looking up scalar functions.
   *
   * Each instance of this interface represents a mechanism to look up scalar functions. They should be registered
   * as a service provider in the META-INF/services directory to be discovered by the ServiceLoader.
   * Alternatively, they can be registered using the {@link com.google.auto.service.AutoService} annotation.
   *
   * @see AnnotatedClassLookupMechanism
   * @see AnnotatedMethodLookupMechanism
   */
  public interface ScalarFunctionLookupMechanism {

    /**
     * Returns the set of {@link ScalarFunctionProvider} instances that can be used to look up scalar functions.
     */
    Set<ScalarFunctionProvider> getProviders();

    /**
     * Interface for providing scalar functions.
     * <p>Each provider can provide multiple scalar functions, and they can have different priorities.
     * <p>If two functions have the same canonical name, the one with higher priority will be used.
     */
    interface ScalarFunctionProvider {
      /**
       * Returns the name of the provider.
       * <p>This is used for logging and debugging purposes when there are multiple providers for the same function.
       */
      String name();

      /**
       * Returns the priority of the provider. Higher priority providers are loaded first.
       * <p>Default priority is 0.
       */
      int priority();

      /**
       * Returns a set of {@link PinotScalarFunction} instances.
       */
      Set<PinotScalarFunction> getFunctions();
    }
  }
}
