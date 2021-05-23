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
import java.util.Set;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;


public class PinotReflectionUtils {
  private static final String PINOT_PACKAGE_PREFIX = "org.apache.pinot";
  private static final Object REFLECTION_LOCK = new Object();

  public static Set<Class<?>> getClassesThroughReflection(final String regexPattern,
      final Class<? extends Annotation> annotation) {
    synchronized (getReflectionLock()) {
      Reflections reflections = new Reflections(
          new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(PINOT_PACKAGE_PREFIX)).filterInputsBy(new FilterBuilder.Include(regexPattern)).setScanners(new TypeAnnotationsScanner()));
      return reflections.getTypesAnnotatedWith(annotation, true);
    }
  }

  /**
   * Due to the multi-threading issue in org.reflections.vfs.ZipDir, we need to put a lock before calling the
   * reflection related methods.
   *
   * @return
   */
  public static Object getReflectionLock() {
    return REFLECTION_LOCK;
  }
}
