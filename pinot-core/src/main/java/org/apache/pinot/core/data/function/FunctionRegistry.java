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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for inbuilt Pinot functions
 */
public class FunctionRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  static Map<String, List<FunctionInfo>> _functionInfoMap = new HashMap<>();

  static {
    try {
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochSeconds", Long.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochMinutes", Long.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochHours", Long.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDays", Long.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("toEpochSecondsRounded", Long.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("toEpochMinutesRounded", Long.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("toEpochHoursRounded", Long.class, String.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDaysRounded", Long.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("toEpochSecondsBucket", Long.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("toEpochMinutesBucket", Long.class, String.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochHoursBucket", Long.class, String.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("toEpochDaysBucket", Long.class, String.class));

      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochSeconds", Long.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochMinutes", Number.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochHours", Number.class));
      registerStaticFunction(DateTimeFunctions.class.getDeclaredMethod("fromEpochDays", Number.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("fromEpochSecondsBucket", Long.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("fromEpochMinutesBucket", Number.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("fromEpochHoursBucket", Number.class, String.class));
      registerStaticFunction(
          DateTimeFunctions.class.getDeclaredMethod("fromEpochDaysBucket", Number.class, String.class));
    } catch (NoSuchMethodException e) {
      LOGGER.error("Caught exception when registering function", e);
    }
  }

  public static FunctionInfo resolve(String functionName, Class<?>[] argumentTypes) {
    List<FunctionInfo> list = _functionInfoMap.get(functionName.toLowerCase());
    FunctionInfo bestMatch = null;
    if (list != null && list.size() > 0) {
      for (FunctionInfo functionInfo : list) {
        if (functionInfo.isApplicable(argumentTypes)) {
          bestMatch = functionInfo;
          break;
        }
      }
    }
    return bestMatch;
  }

  public static void registerStaticFunction(Method method) {
    Preconditions.checkArgument(Modifier.isStatic(method.getModifiers()), "Method needs to be static:" + method);
    List<FunctionInfo> list = new ArrayList<>();
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    list.add(functionInfo);
    _functionInfoMap.put(method.getName().toLowerCase(), list);
  }
}
