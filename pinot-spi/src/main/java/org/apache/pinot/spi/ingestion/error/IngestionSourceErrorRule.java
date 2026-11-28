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
package org.apache.pinot.spi.ingestion.error;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Nullable;


@FunctionalInterface
public interface IngestionSourceErrorRule {

  @Nullable
  IngestionSourceErrorVerdict decide(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence);

  static boolean hasClass(Class<?> clazz, String className) {
    return IngestionSourceErrorRules.classNames(clazz).contains(className);
  }

  static String messageOf(Throwable throwable) {
    try {
      String message = throwable.getMessage();
      return message != null ? message : "";
    } catch (Throwable t) {
      IngestionSourceErrorClassifier.rethrowIfFatal(t);
      return "";
    }
  }

  @Nullable
  static Object invoke(Object target, String methodName) {
    try {
      Method method = target.getClass().getMethod(methodName);
      method.trySetAccessible();
      return method.invoke(target);
    } catch (Throwable t) {
      IngestionSourceErrorClassifier.rethrowIfFatal(t);
      return null;
    }
  }
}
