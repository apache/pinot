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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotReflectionUtils {
  private PinotReflectionUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotReflectionUtils.class);
  private static final String PINOT_PACKAGE_NAME = "org.apache.pinot";

  // We use a lock to prevent multiple threads accessing the same jar in the same time which can cause exception
  // See https://github.com/ronmamo/reflections/issues/81 for more details
  private static final Object REFLECTION_LOCK = new Object();

  public static Set<Class<?>> getClassesThroughReflection(String regexPattern, Class<? extends Annotation> annotation) {
    return getClassesThroughReflection(PINOT_PACKAGE_NAME, regexPattern, annotation);
  }

  public static Set<Class<?>> getClassesThroughReflection(String packageName, String regexPattern,
      Class<? extends Annotation> annotation) {
    try {
      synchronized (REFLECTION_LOCK) {
        return new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(packageName))
            .filterInputsBy(new FilterBuilder.Include(regexPattern))).getTypesAnnotatedWith(annotation);
      }
    } catch (Throwable t) {
      // Log an error then re-throw it because this method is usually called in a static block, where exception might
      // not be properly handled
      LOGGER.error("Error scanning classes within package: '{}' with regex pattern: '{}', annotation: {}", packageName,
          regexPattern, annotation.getSimpleName(), t);
      throw t;
    }
  }

  public static Set<Class<?>> getClassesThroughReflection(List<String> packages, String regexPattern,
      Class<? extends Annotation> annotation) {
    try {
      synchronized (REFLECTION_LOCK) {
        List<URL> urls = new ArrayList<>();
        for (String packageName : packages) {
          urls.addAll(ClasspathHelper.forPackage(packageName));
        }
        return new Reflections(new ConfigurationBuilder().setUrls(urls)
            .filterInputsBy(new FilterBuilder.Include(regexPattern))).getTypesAnnotatedWith(annotation);
      }
    } catch (Throwable t) {
      // Log an error then re-throw it because this method is usually called in a static block, where exception might
      // not be properly handled
      LOGGER.error("Error scanning classes within packages: {} with regex pattern: '{}', annotation: {}", packages,
          regexPattern, annotation.getSimpleName(), t);
      throw t;
    }
  }

  public static Set<Method> getMethodsThroughReflection(String regexPattern, Class<? extends Annotation> annotation) {
    return getMethodsThroughReflection(PINOT_PACKAGE_NAME, regexPattern, annotation);
  }

  public static Set<Method> getMethodsThroughReflection(String packageName, String regexPattern,
      Class<? extends Annotation> annotation) {
    try {
      synchronized (REFLECTION_LOCK) {
        return new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(packageName))
            .filterInputsBy(new FilterBuilder.Include(regexPattern))
            .setScanners(new MethodAnnotationsScanner())).getMethodsAnnotatedWith(annotation);
      }
    } catch (Throwable t) {
      // Log an error then re-throw it because this method is usually called in a static block, where exception might
      // not be properly handled
      LOGGER.error("Error scanning methods within package: '{}' with regex pattern: '{}', annotation: {}", packageName,
          regexPattern, annotation.getSimpleName(), t);
      throw t;
    }
  }

  /**
   * Executes the given runnable within the reflection lock.
   */
  public static void runWithLock(Runnable runnable) {
    synchronized (REFLECTION_LOCK) {
      runnable.run();
    }
  }

  /**
   * Due to the multi-threading issue in org.reflections.vfs.ZipDir, we need to put a lock before calling the
   * reflection related methods.
   *
   * Deprecated: use {@link #runWithLock(Runnable)} instead
   */
  @Deprecated
  public static Object getReflectionLock() {
    return REFLECTION_LOCK;
  }
}
