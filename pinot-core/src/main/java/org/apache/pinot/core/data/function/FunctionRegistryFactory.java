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

import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to create a FunctionRegistry (currently only {@link InbuiltFunctionRegistry})
 */
public class FunctionRegistryFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistryFactory.class);

  private FunctionRegistryFactory() {

  }

  /**
   * Creates an {@link InbuiltFunctionRegistry}
   *
   * The functionsToRegister list inside includes all the methods added to the InbuiltFunctionRegistry
   */
  public static InbuiltFunctionRegistry getInbuiltFunctionRegistry() {
    List<Method> functionsToRegister;
    DateTimeFunctions dateTimeFunctions = new DateTimeFunctions();

    try {
      functionsToRegister = Lists
          .newArrayList(dateTimeFunctions.getClass().getDeclaredMethod("toEpochSeconds", Long.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochMinutes", Long.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochHours", Long.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochDays", Long.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochSecondsRounded", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochMinutesRounded", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochHoursRounded", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochDaysRounded", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochSecondsBucket", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochMinutesBucket", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochHoursBucket", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toEpochDaysBucket", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochSeconds", Long.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochMinutes", Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochHours", Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochDays", Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochSecondsBucket", Long.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochMinutesBucket", Number.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochHoursBucket", Number.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromEpochDaysBucket", Number.class, Number.class),
              dateTimeFunctions.getClass().getDeclaredMethod("toDateTime", Long.class, String.class),
              dateTimeFunctions.getClass().getDeclaredMethod("fromDateTime", String.class, String.class),

              JsonFunctions.class.getDeclaredMethod("toJsonMapStr", Map.class));
    } catch (NoSuchMethodException e) {
      LOGGER.error("Caught exception when registering function", e);
      throw new IllegalStateException(e);
    }

    return new InbuiltFunctionRegistry(functionsToRegister);
  }
}
