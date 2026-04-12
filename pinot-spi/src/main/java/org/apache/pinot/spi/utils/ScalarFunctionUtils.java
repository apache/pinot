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
package org.apache.pinot.spi.utils;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Helper methods for discovering and registering scalar functions.
 */
public final class ScalarFunctionUtils {
  public static final String SCALAR_FUNCTION_PACKAGE_REGEX = ".*\\.function\\..*";

  private ScalarFunctionUtils() {
  }

  public static Set<Class<?>> getScalarFunctionClasses() {
    return PinotReflectionUtils.getClassesThroughReflection(SCALAR_FUNCTION_PACKAGE_REGEX, ScalarFunction.class);
  }

  public static Set<Method> getScalarFunctionMethods() {
    return PinotReflectionUtils.getMethodsThroughReflection(SCALAR_FUNCTION_PACKAGE_REGEX, ScalarFunction.class);
  }

  public static List<String> getScalarFunctionNames(ScalarFunction scalarFunction, String defaultName) {
    String[] names = scalarFunction.names();
    if (names.length == 0) {
      return List.of(canonicalize(defaultName));
    }

    Set<String> canonicalNames = new LinkedHashSet<>();
    for (String name : names) {
      canonicalNames.add(canonicalize(name));
    }
    return List.copyOf(canonicalNames);
  }

  public static String canonicalize(String name) {
    return StringUtils.remove(name, '_').toLowerCase(Locale.ROOT);
  }
}
