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


/// A rule for an SDK Pinot doesn't ship, registered with [IngestionSourceErrorClassifier#registerRule].
///
/// A rule decides one throwable at a time. It never sees the rest of the tree's verdicts and can't change how they
/// combine: the classifier still walks every branch and applies its precedence. A registered rule is consulted only
/// for a throwable no built-in rule decides, and a rule that throws counts as no decision.
///
/// SDK classes live in plugin classloaders, so match them by name with [#hasClass], never with `instanceof`, and read
/// their codes with [#invoke], treating a failed read as "undecided".
@FunctionalInterface
public interface IngestionSourceErrorRule {

  /// Decides `node`: a category, [IngestionSourceErrorVerdict#none] when it shows the failure is not the source's
  /// verdict, or null to leave it undecided.
  ///
  /// @param node the throwable to decide
  /// @param below the rest of its cause chain, nearest cause first
  /// @param evidence the nearest evidence attached at or above `node` in its chain, or null
  @Nullable
  IngestionSourceErrorVerdict decide(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence);

  /// Whether `clazz`, a superclass or an implemented interface is named `className`.
  static boolean hasClass(Class<?> clazz, String className) {
    for (Class<?> current = clazz; current != null; current = current.getSuperclass()) {
      if (current.getName().equals(className)) {
        return true;
      }
      for (Class<?> iface : current.getInterfaces()) {
        if (hasClass(iface, className)) {
          return true;
        }
      }
    }
    return false;
  }

  /// The message of `throwable`, or "" when it has none or reading it fails: third-party `getMessage()`
  /// implementations can throw, and classification must never fail the caller.
  static String messageOf(Throwable throwable) {
    try {
      String message = throwable.getMessage();
      return message != null ? message : "";
    } catch (RuntimeException | LinkageError | AssertionError e) {
      return "";
    }
  }

  /// Calls a public no-arg method by name, or returns null when anything fails.
  @Nullable
  static Object invoke(Object target, String methodName) {
    try {
      Method method = target.getClass().getMethod(methodName);
      method.trySetAccessible();
      return method.invoke(target);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      return null;
    }
  }
}
