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
package org.apache.pinot.common.config.tuner;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.tuner.TableConfigTuner;
import org.apache.pinot.spi.data.Schema;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to dynamically register all annotated {@link TableConfigTuner} methods
 */
public class TableConfigTunerRegistry {
  private static final int NUM_PARAMETERS = 2;

  private TableConfigTunerRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigTunerRegistry.class);
  private static final Map<String, FunctionInfo> tunerFunctionInfoMap = new HashMap<>();

  static {
    long startTimeMs = System.currentTimeMillis();
    Reflections reflections = new Reflections(
        new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
            .filterInputsBy(new FilterBuilder.Include(".*\\.tuner\\..*")).setScanners(new MethodAnnotationsScanner()));
    Set<Method> methodSet = reflections.getMethodsAnnotatedWith(TableConfigTuner.class);
    for (Method method : methodSet) {
      TableConfigTuner tableConfigTuner = method.getAnnotation(TableConfigTuner.class);
      if (tableConfigTuner.enabled()) {
        if (!tableConfigTuner.name().isEmpty()) {
          TableConfigTunerRegistry.registerTuner(tableConfigTuner.name(), method);
        } else {
          LOGGER.error("Cannot register an unnamed config tuner");
        }
      }
    }
    LOGGER.info("Initialized TableConfigTunerRegistry with {} tuners: {} in {}ms", tunerFunctionInfoMap.size(),
        tunerFunctionInfoMap.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  private static void registerTuner(String name, Method method) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    Preconditions.checkState(method.getParameterCount() == NUM_PARAMETERS,
        "TableConfigTuner method must have 2 parameters of type TableConfig and Schema");
    Class<?>[] parameterTypes = method.getParameterTypes();
    Preconditions.checkState(parameterTypes[0] == TableConfig.class && parameterTypes[1] == Schema.class,
        "TableConfigTuner method must have 2 parameters of type TableConfig and Schema");
    Preconditions.checkState(method.getReturnType() == TableConfig.class,
        "TableConfigTuner method must return an object of type TableConfig");
    Preconditions
        .checkState(tunerFunctionInfoMap.put(name, functionInfo) == null, "TableConfigTuner: %s is already registered",
            name);
  }

  /**
   * Helper to invoke the registered TableConfigTuner method
   */
  public static TableConfig invokeTableConfigTuner(String name, TableConfig initialConfig, Schema schema) {
    FunctionInfo functionInfo = tunerFunctionInfoMap.get(name);
    Preconditions.checkNotNull(functionInfo, "TableConfigTuner with name %s is not registered", name);
    FunctionInvoker invoker = new FunctionInvoker(functionInfo);
    Object[] arguments = {initialConfig, schema};
    TableConfig result = (TableConfig) invoker.invoke(arguments);
    return result;
  }
}
