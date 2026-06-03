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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Implementations of the SQL {@code GROUPING} / {@code GROUPING_ID} indicator functions for grouping-set queries.
 *
 * <p>These operate on the synthetic {@code $groupingId} column (see {@link GroupingSets}) that the engine attaches
 * to every grouping-set result row. The single-stage parser rewrites the user-facing {@code GROUPING(col)} /
 * {@code GROUPING_ID(c1, ..., cp)} into calls to these functions, passing the {@code $groupingId} column and the
 * pre-computed bit shift(s) ({@code numUnionColumns - 1 - columnUnionIndex}) so that no per-row column lookup is
 * needed. They are plain scalar (transform) functions, not aggregations.
 */
public class GroupingSetFunctions {
  private GroupingSetFunctions() {
  }

  /** {@code GROUPING(col)} => 1 if the column is rolled up (aggregated away) in this row, else 0. */
  @ScalarFunction
  public static int grouping(int groupingId, int shift) {
    return (groupingId >>> shift) & 1;
  }

  /**
   * {@code GROUPING_ID(c1, ..., cp)} => the per-column grouping bits gathered at the given shifts with the first
   * argument as the most significant bit (SQL convention).
   */
  @ScalarFunction(names = {"groupingId", "grouping_id"})
  public static int groupingId(int groupingId, int[] shifts) {
    int result = 0;
    for (int shift : shifts) {
      result = (result << 1) | ((groupingId >>> shift) & 1);
    }
    return result;
  }
}
