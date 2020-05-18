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
package org.apache.pinot.core.data.function;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for inbuilt Pinot functions
 */
public class InbuiltFunctionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(InbuiltFunctionRegistry.class);
  private static final Map<String, FunctionInfo> _functionInfoMap = new HashMap<>();

  static {
    try {
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochSeconds", Long.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochMinutes", Long.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochHours", Long.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDays", Long.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochSecondsRounded", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochMinutesRounded", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochHoursRounded", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDaysRounded", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochSecondsBucket", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochMinutesBucket", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochHoursBucket", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDaysBucket", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochSeconds", Long.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochMinutes", Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochHours", Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochDays", Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochSecondsBucket", Long.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochMinutesBucket", Number.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochHoursBucket", Number.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochDaysBucket", Number.class, Number.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("toDateTime", Long.class, String.class));
      registerFunction(DateTimeFunctions.class.getDeclaredMethod("fromDateTime", String.class, String.class));

      registerFunction(JsonFunctions.class.getDeclaredMethod("toJsonMapStr", Map.class));
    } catch (NoSuchMethodException e) {
      LOGGER.error("Caught exception when registering function", e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Given a function name and a set of argument types, asserts that a corresponding function
   * was registered during construction and returns it
   */
  public static FunctionInfo getFunctionByNameWithApplicableArgumentTypes(String functionName,
      Class<?>[] argumentTypes) {
    Preconditions.checkArgument(_functionInfoMap.containsKey(functionName.toLowerCase()));
    FunctionInfo functionInfo = _functionInfoMap.get(functionName.toLowerCase());
    Preconditions.checkArgument(functionInfo.isApplicable(argumentTypes));
    return functionInfo;
  }

  static void registerFunction(Method method) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    _functionInfoMap.put(method.getName().toLowerCase(), functionInfo);
  }
}
