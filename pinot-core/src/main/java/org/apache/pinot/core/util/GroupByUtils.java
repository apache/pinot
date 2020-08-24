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

import org.apache.pinot.core.query.request.context.QueryContext;


public final class GroupByUtils {
  private GroupByUtils() {
  }

  private static final int NUM_RESULTS_LOWER_LIMIT = 5000;

  /**
   * (For PQL semantic) Returns the capacity of the table required by the given query.
   * NOTE: It returns {@code max(limit * 5, 5000)} to ensure the result accuracy because the results are always ordered
   *       in PQL semantic.
   */
  public static int getTableCapacity(int limit) {
    return Math.max(limit * 5, NUM_RESULTS_LOWER_LIMIT);
  }

  /**
   * (For SQL semantic) Returns the capacity of the table required by the given query.
   * <ul>
   *   <li>For GROUP-BY with ORDER-BY or HAVING, returns {@code max(limit * 5, 5000)} to ensure the result accuracy</li>
   *   <li>For GROUP-BY without ORDER-BY or HAVING, returns the limit</li>
   * </ul>
   */
  public static int getTableCapacity(QueryContext queryContext) {
    int limit = queryContext.getLimit();
    if (queryContext.getOrderByExpressions() != null || queryContext.getHavingFilter() != null) {
      return Math.max(limit * 5, NUM_RESULTS_LOWER_LIMIT);
    } else {
      return limit;
    }
  }
}
