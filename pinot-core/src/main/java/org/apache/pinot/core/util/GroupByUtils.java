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
package org.apache.pinot.core.util;

public final class GroupByUtils {
  private GroupByUtils() {
  }

  public static final int DEFAULT_MIN_NUM_GROUPS = 5000;

  /**
   * Returns the capacity of the table required by the given query.
   * NOTE: It returns {@code max(limit * 5, 5000)} to ensure the result accuracy.
   */
  public static int getTableCapacity(int limit) {
    return Math.max(limit * 5, DEFAULT_MIN_NUM_GROUPS);
  }

  /**
   * Returns the capacity of the table required by the given query.
   * NOTE: It returns {@code max(limit * 5, minNumGroups)} where minNumGroups is configurable to tune the table size and
   *       result accuracy.
   */
  public static int getTableCapacity(int limit, int minNumGroups) {
    return Math.max(limit * 5, minNumGroups);
  }
}
