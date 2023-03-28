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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.apache.pinot.common.utils.PinotDataType;


/**
 * The {@code FunctionInvoker} is a wrapper on a java method which supports arguments type conversion and method
 * invocation via reflection.
 */
public class FunctionInvoker {
  private final Method _method;
  // If true, the function should return null if any of its argument is null
  // Otherwise, the function should deal with null in its own implementation.
  private final boolean _isNullIntolerant;

  private final Class<?>[] _parameterClasses;
  private final PinotDataType[] _parameterTypes;
  private final Object _instance;

  public FunctionInvoker(FunctionInfo functionInfo) {
    _method = functionInfo.getMethod();
    _isNullIntolerant = !functionInfo.hasNullableParameters();
    Class<?>[] parameterClasses = _method.getParameterTypes();
    int numParameters = parameterClasses.length;
    _parameterClasses = new Class<?>[numParameters];
    _parameterTypes = new PinotDataType[numParameters];
    for (int i = 0; i < numParameters; i++) {
      Class<?> parameterClass = parameterClasses[i];
      _parameterClasses[i] = parameterClass;
      _parameterTypes[i] = FunctionUtils.getParameterType(parameterClass);
    }
    if (Modifier.isStatic(_method.getModifiers())) {
      _instance = null;
    } else {
      Class<?> clazz = functionInfo.getClazz();
      try {
        Constructor<?> constructor = functionInfo.getClazz().getDeclaredConstructor();
        _instance = constructor.newInstance();
      } catch (Exception e) {
        throw new IllegalStateException("Caught exception while constructing class: " + clazz, e);
      }
    }
  }

  /**
   * Returns the underlying java method.
   */
  public Method getMethod() {
    return _method;
  }

  /**
   * Returns the class of the parameters.
   */
  public Class<?>[] getParameterClasses() {
    return _parameterClasses;
  }

  /**
   * Returns the PinotDataType of the parameters for type conversion purpose. Puts {@code null} for the parameter class
   * that does not support type conversion.
   */
  public PinotDataType[] getParameterTypes() {
    return _parameterTypes;
  }

  /**
   * Converts the type of the given arguments to match the parameter classes. Leaves the argument as is if type
   * conversion is not needed or supported.
   */
  public void convertTypes(Object[] arguments) {
    int numParameters = _parameterClasses.length;
    Preconditions.checkArgument(arguments.length == numParameters,
        "Wrong number of arguments for method: %s, expected: %s, actual: %s", _method, numParameters, arguments.length);
    for (int i = 0; i < numParameters; i++) {
      // Skip conversion for null
      Object argument = arguments[i];
      if (argument == null) {
        continue;
      }
      // Skip conversion if argument can be directly assigned
      Class<?> parameterClass = _parameterClasses[i];
      Class<?> argumentClass = argument.getClass();
      if (parameterClass.isAssignableFrom(argumentClass)) {
        continue;
      }

      PinotDataType parameterType = _parameterTypes[i];
      PinotDataType argumentType = FunctionUtils.getArgumentType(argumentClass);
      Preconditions.checkArgument(parameterType != null && argumentType != null,
          "Cannot convert value from class: %s to class: %s", argumentClass, parameterClass);
      arguments[i] = parameterType.convert(argument, argumentType);
    }
  }

  /**
   * Returns the class of the result value.
   */
  public Class<?> getResultClass() {
    return _method.getReturnType();
  }

  /**
   * Invoke the function with the given arguments. The arguments should match the parameter classes. Use
   * {@link #convertTypes(Object[])} to convert the argument types if needed before calling this method.
   */
  public Object invoke(Object[] arguments) {
    if (_isNullIntolerant) {
      for (Object arg : arguments) {
        if (arg == null) {
          return null;
        }
      }
    }
    try {
      return _method.invoke(_instance, arguments);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Caught exception while invoking method: " + _method + " with arguments: " + Arrays.toString(arguments), e);
    }
  }
}
