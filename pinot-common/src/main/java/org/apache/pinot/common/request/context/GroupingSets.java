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
package org.apache.pinot.common.request.context;

import java.util.stream.IntStream;

/// Engine-agnostic conventions for GROUP BY GROUPING SETS / ROLLUP / CUBE, shared so both query engines
/// agree on the discriminator column and the GROUPING()/GROUPING_ID() semantics.
///
/// A grouping-set query is represented as the union of all grouping columns plus, per set, a "grouping-id"
/// bitmask over that union where bit `i` is set iff union column `i` is **rolled up** (aggregated away) in
/// that set — matching the SQL/PostgreSQL convention that GROUPING(col) returns 1 for an aggregated-away
/// column. The bitmask is carried through the result as the synthetic key column named {@link
/// #GROUPING_ID_COLUMN}, which the discriminator keeps distinct across sets and which powers
/// GROUPING()/GROUPING_ID().
public class GroupingSets {
  private GroupingSets() {
  }

  /// Name of the synthetic INT key column that carries the per-set grouping-id bitmask. It is internal: it is
  /// appended after the union group-by columns in intermediate results and dropped from the user-facing
  /// result (never projected unless referenced via GROUPING()/GROUPING_ID()).
  public static final String GROUPING_ID_COLUMN = "$groupingId";

  /// Computes the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value from a row's grouping-id
  /// bitmask. For each argument column (identified by its index in the union of grouping columns) the
  /// corresponding bit is read from {@code groupingId} (1 = rolled up), and the bits are packed with the
  /// first argument as the most significant bit, per the SQL standard.
  ///
  /// @param groupingId the row's grouping-id bitmask (bit i set iff union column i is rolled up)
  /// @param unionColumnIndexes the union-column index of each GROUPING argument, in argument order
  /// @return the GROUPING / GROUPING_ID integer value
  public static int groupingValue(int groupingId, int[] unionColumnIndexes) {
    int result = 0;
    for (int unionColumnIndex : unionColumnIndexes) {
      result = (result << 1) | ((groupingId >>> unionColumnIndex) & 1);
    }
    return result;
  }

  /// Builds the per-set participation bitmask over the union group-by columns -- the wire encoding the single-stage
  /// engine carries in {@code PinotQuery.groupingSetMasks}. Bit {@code p} is set iff union position {@code p}
  /// participates in the set (the rolled-up inverse read by {@link #groupingValue} from the {@link #GROUPING_ID_COLUMN}
  /// column is its complement). Centralizing the {@code 1 << position} convention here keeps the producers in agreement.
  /// (The multi-stage engine instead expands grouping sets natively; see {@code GroupingSetsExpandNode}.)
  public static int participationMask(IntStream unionPositions) {
    return unionPositions.map(position -> 1 << position).reduce(0, (left, right) -> left | right);
  }

  /// Returns true if the given function name denotes {@code GROUPING} or {@code GROUPING_ID}. The argument is
  /// the post-canonicalization form (lower-cased, underscores removed), so {@code GROUPING_ID} appears as
  /// {@code "groupingid"}. Centralizes the name contract shared by the broker post-aggregation handler and the
  /// ORDER BY comparator so the two sites cannot drift.
  public static boolean isGroupingFunction(String functionName) {
    return functionName.equalsIgnoreCase("grouping") || functionName.equalsIgnoreCase("groupingid");
  }
}
