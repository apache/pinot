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
package org.apache.pinot.core.data.manager;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Thread-local task context for segment operations.
 */
public final class SegmentOperationsTaskContext {
  private static final ThreadLocal<Context> CONTEXT = new ThreadLocal<>();

  private SegmentOperationsTaskContext() {
  }

  public static void set(SegmentOperationsTaskType taskType, @Nullable String tableNameWithType) {
    CONTEXT.set(new Context(taskType, tableNameWithType));
  }

  public static void clear() {
    CONTEXT.remove();
  }

  @Nullable
  public static SegmentOperationsTaskType getTaskType() {
    Context context = CONTEXT.get();
    return context != null ? context._taskType : null;
  }

  @Nullable
  public static String getTableNameWithType() {
    Context context = CONTEXT.get();
    return context != null ? context._tableNameWithType : null;
  }

  /**
   * Wraps a runnable task with segment operations context.
   */
  public static Runnable wrap(Runnable runnable, SegmentOperationsTaskType taskType,
      @Nullable String tableNameWithType) {
    return () -> {
      set(taskType, tableNameWithType);
      try {
        runnable.run();
      } finally {
        clear();
      }
    };
  }

  /**
   * Wraps a callable task with segment operations context.
   */
  public static <T> Callable<T> wrap(Callable<T> callable, SegmentOperationsTaskType taskType,
      @Nullable String tableNameWithType) {
    return () -> {
      set(taskType, tableNameWithType);
      try {
        return callable.call();
      } finally {
        clear();
      }
    };
  }

  private static final class Context {
    private final SegmentOperationsTaskType _taskType;
    private final String _tableNameWithType;

    private Context(SegmentOperationsTaskType taskType, @Nullable String tableNameWithType) {
      _taskType = taskType;
      _tableNameWithType = tableNameWithType;
    }
  }
}
