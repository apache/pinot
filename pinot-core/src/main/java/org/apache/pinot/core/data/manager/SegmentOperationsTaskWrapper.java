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
 * Wraps tasks to bind a segment operations task context.
 */
public final class SegmentOperationsTaskWrapper {

  private SegmentOperationsTaskWrapper() {
  }

  public static Runnable wrap(Runnable runnable, SegmentOperationsTaskType taskType,
      @Nullable String tableNameWithType) {
    return () -> {
      SegmentOperationsTaskContext.set(taskType, tableNameWithType);
      try {
        runnable.run();
      } finally {
        SegmentOperationsTaskContext.clear();
      }
    };
  }

  public static <T> Callable<T> wrap(Callable<T> callable, SegmentOperationsTaskType taskType,
      @Nullable String tableNameWithType) {
    return () -> {
      SegmentOperationsTaskContext.set(taskType, tableNameWithType);
      try {
        return callable.call();
      } finally {
        SegmentOperationsTaskContext.clear();
      }
    };
  }
}
