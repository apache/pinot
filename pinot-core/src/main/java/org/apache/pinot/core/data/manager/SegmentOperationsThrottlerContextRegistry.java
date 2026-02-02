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

/**
 * Registry for optional segment operations throttler context hooks.
 */
public final class SegmentOperationsThrottlerContextRegistry {
  private static final SegmentOperationsThrottlerContext NO_OP_CONTEXT = new SegmentOperationsThrottlerContext() {
    @Override
    public Runnable wrap(Runnable runnable, SegmentOperationsTaskType taskType) {
      return runnable;
    }
  };

  private static volatile SegmentOperationsThrottlerContext _context = NO_OP_CONTEXT;

  private SegmentOperationsThrottlerContextRegistry() {
  }

  public static SegmentOperationsThrottlerContext get() {
    return _context;
  }

  public static void set(SegmentOperationsThrottlerContext context) {
    _context = context != null ? context : NO_OP_CONTEXT;
  }
}
