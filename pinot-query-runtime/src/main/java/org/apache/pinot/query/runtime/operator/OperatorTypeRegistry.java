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
package org.apache.pinot.query.runtime.operator;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;


/**
 * Registry of all known MSE {@link OperatorTypeDescriptor}s. Built-in types ({@link MultiStageOperator.Type} enum
 * entries) are always present. Plugin-defined types are discovered at class-loading time via {@link ServiceLoader}:
 * any jar on the classpath that ships a
 * {@code META-INF/services/org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor} file will have its
 * descriptors automatically registered without configuration.
 *
 * <p>Thread-safe: the registry map is built once in a static initializer and never mutated afterward.
 */
public final class OperatorTypeRegistry {

  private static final Map<Integer, OperatorTypeDescriptor> ID_TO_DESCRIPTOR;

  static {
    Map<Integer, OperatorTypeDescriptor> map = new HashMap<>();
    for (MultiStageOperator.Type builtIn : MultiStageOperator.Type.values()) {
      map.put(builtIn.getId(), builtIn);
    }
    for (OperatorTypeDescriptor plugin : ServiceLoader.load(OperatorTypeDescriptor.class)) {
      // Enforce the documented id contract: ids below PLUGIN_ID_FLOOR are reserved for built-ins (current and
      // future). Without this check a plugin could squat on a reserved id and work until a built-in claims it —
      // and ids that fit in the legacy single-byte stat format would be silently emitted there, defeating the
      // plugin-ids-are-stream-mode-only guarantee (see OperatorTypeDescriptor#getId).
      if (plugin.getId() < OperatorTypeDescriptor.PLUGIN_ID_FLOOR) {
        throw new IllegalStateException("Plugin operator type " + plugin.name() + " uses id " + plugin.getId()
            + ", which is reserved for built-in types. Plugin ids must be >= "
            + OperatorTypeDescriptor.PLUGIN_ID_FLOOR);
      }
      OperatorTypeDescriptor prev = map.put(plugin.getId(), plugin);
      if (prev != null) {
        throw new IllegalStateException(
            "Duplicate operator type id " + plugin.getId() + ": " + prev.name() + " vs " + plugin.name());
      }
    }
    ID_TO_DESCRIPTOR = Collections.unmodifiableMap(map);
  }

  private OperatorTypeRegistry() {
  }

  /**
   * Returns the descriptor registered for the given id, or {@code null} if no descriptor has that id.
   * Built-in types (ids 0–15) are always present. Plugin types are available if their jar was on the classpath at
   * startup.
   */
  @Nullable
  public static OperatorTypeDescriptor fromId(int id) {
    return ID_TO_DESCRIPTOR.get(id);
  }

  /** Returns the total number of registered descriptors (built-ins + plugins). */
  @VisibleForTesting
  static int size() {
    return ID_TO_DESCRIPTOR.size();
  }
}
