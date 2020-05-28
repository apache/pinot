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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.annotations.ScalarFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for inbuilt Pinot functions
 */
public class FunctionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  private static final Map<String, FunctionInfo> _functionInfoMap = new HashMap<>();

  /**
   * Given a function name, asserts that a corresponding function was registered during construction and returns it
   */
  public static FunctionInfo getFunctionByName(String functionName) {
    Preconditions.checkArgument(_functionInfoMap.containsKey(functionName.toLowerCase()));
    return _functionInfoMap.get(functionName.toLowerCase());
  }

  /**
   * Given a function name and a set of argument types, asserts that a corresponding function
   * was registered during construction and returns it
   */
  public static FunctionInfo getFunctionByNameWithApplicableArgumentTypes(String functionName,
      Class<?>[] argumentTypes) {
    FunctionInfo functionInfo = getFunctionByName(functionName);
    Preconditions.checkArgument(functionInfo.isApplicable(argumentTypes));
    return functionInfo;
  }

  public static void registerFunction(Method method) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    _functionInfoMap.put(method.getName().toLowerCase(), functionInfo);
  }

  public static boolean containsFunctionByName(String funcName) {
    return _functionInfoMap.containsKey(funcName.toLowerCase());
  }


  static {
    try {

      Reflections reflections = new Reflections(new ConfigurationBuilder()
          .setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
          .setScanners(new MethodAnnotationsScanner()));

      Set<Method> methodSet = reflections.getMethodsAnnotatedWith(ScalarFunction.class);
      for(Method method : methodSet) {
        if(method.getAnnotation(ScalarFunction.class).enabled()) {
          FunctionRegistry.registerFunction(method);
        }
      }

    } catch (Exception e) {
      LOGGER.error("Caught exception when registering function", e);
      throw new IllegalStateException(e);
    }
  }
}
