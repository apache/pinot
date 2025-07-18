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

import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// This class implements a lookup mechanism for classes that extend [PinotScalarFunction] that are also annotated with
/// [ScalarFunction].
///
/// This mechanism is newer than [AnnotatedMethodLookupMechanism] and supports method overloading. But ideally all
/// functions should be registered using the UDF mechanism given it can also be used to register transform functions,
/// create tests and documentation.
/// This is why any scalar function registered using this mechanism is registered using a negative priority, which is
/// lower than the default priority, which is 0.
@AutoService(FunctionRegistry.ScalarFunctionLookupMechanism.class)
public class AnnotatedClassLookupMechanism implements FunctionRegistry.ScalarFunctionLookupMechanism {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnnotatedClassLookupMechanism.class);
  /// The default priority for classes that are registered using this mechanism.
  public static final int DEFAULT_PRIORITY = -1_000;

  @Override
  public Set<ScalarFunctionProvider> getProviders() {
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.function\\..*", ScalarFunction.class);
    Set<FunctionRegistry.ScalarFunctionLookupMechanism.ScalarFunctionProvider> providers = Sets
        .newHashSetWithExpectedSize(classes.size());
    for (Class<?> clazz : classes) {
      if (!Modifier.isPublic(clazz.getModifiers())) {
        LOGGER.debug("Skipping class {} because it is not public", clazz);
        continue;
      }
      ScalarFunction scalarFunction = clazz.getAnnotation(ScalarFunction.class);
      if (!scalarFunction.enabled()) {
        LOGGER.debug("Skipping class {} because the annotation says it is not enabled", clazz);
        continue;
      }
      PinotScalarFunction function;
      try {
        function = (PinotScalarFunction) clazz.getConstructor().newInstance();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to instantiate PinotScalarFunction with class: " + clazz);
      }
      providers.add(new FunctionRegistry.ScalarFunctionLookupMechanism.ScalarFunctionProvider() {
        @Override
        public String name() {
          return clazz.getCanonicalName();
        }

        @Override
        public int priority() {
          return DEFAULT_PRIORITY; // Default priority for classes
        }

        @Override
        public Set<PinotScalarFunction> getFunctions() {
          return Set.of(function);
        }
      });
    }
    return providers;
  }
}
