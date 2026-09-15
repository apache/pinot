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

import java.util.ArrayList;
import java.util.List;

/// Engine-agnostic conventions for GROUP BY GROUPING SETS / ROLLUP / CUBE, shared so both query engines
/// agree on the discriminator column and the GROUPING()/GROUPING_ID() semantics.
///
/// A grouping-set query is represented as the ordered union of all grouping columns plus an ordered list of
/// grouping sets, each set given as the indexes of its participating union columns (mirroring Calcite's
/// per-set column bitset, so the number of grouping columns is unlimited). A set's position in that list is
/// its **ordinal**, carried through the result as the synthetic INT key column named {@link
/// #GROUPING_ID_COLUMN}: the discriminator keeps rows from different sets distinct — e.g. set `{a}` with `b`
/// rolled up to NULL stays apart from set `{a, b}` where `b` is genuinely NULL — and identifies the set that
/// produced a row, which is what powers GROUPING()/GROUPING_ID().
public class GroupingSets {
  private GroupingSets() {
  }

  /// Name of the synthetic INT key column that carries the grouping-set ordinal (the set's position in the
  /// query's grouping-set list). It is internal: it is appended after the union group-by columns in
  /// intermediate results and dropped from the user-facing result (never projected unless referenced via
  /// GROUPING()/GROUPING_ID()). Note: despite the name, the value identifies the grouping set (its ordinal);
  /// it is NOT the SQL {@code GROUPING_ID()} bitmask, which is computed from it per call.
  public static final String GROUPING_ID_COLUMN = "$groupingId";

  /// Prefix of the synthetic per-set group-key copy columns the multi-stage RepeatOperator appends (one per union
  /// group-by column, suffixed with the union index). These carry the group-key value where the column
  /// participates in a set and NULL where it is rolled up, so the original input columns stay intact for
  /// aggregation arguments. Like {@link #GROUPING_ID_COLUMN} they are internal and dropped from the result.
  public static final String GROUPING_SET_KEY_COLUMN_PREFIX = "$groupingSetKey";

  /// GROUPING()/GROUPING_ID() pack one bit per argument into an INT (first argument = most significant bit),
  /// so a single call accepts at most 31 arguments. This is a per-call limit on the function's arguments —
  /// the number of grouping columns in the query is unlimited.
  public static final int MAX_GROUPING_FUNCTION_ARGS = 31;

  /// Upper bound on the number of grouping sets a single query may expand to. Guards against CUBE blow-up:
  /// every set is materialized in the plan, expanded per input row at runtime, and enumerated by the
  /// GROUPING()/GROUPING_ID() projection, so the set count bounds plan size and per-row work. Shared by the
  /// single-stage parser and the multi-stage plan conversion.
  public static final int MAX_GROUPING_SETS = 4096;

  /// Computes the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value for one grouping set. For
  /// each argument column (identified by its index in the union of grouping columns) the bit is 1 iff the
  /// column is **rolled up** (does not participate) in the set, and the bits are packed with the first
  /// argument as the most significant bit, per the SQL/PostgreSQL convention.
  ///
  /// @param setColumnIndexes the union-column indexes participating in the grouping set
  /// @param argUnionIndexes the union-column index of each GROUPING argument, in argument order
  /// @return the GROUPING / GROUPING_ID integer value for rows of that set
  public static int groupingValue(int[] setColumnIndexes, int[] argUnionIndexes) {
    int result = 0;
    for (int argUnionIndex : argUnionIndexes) {
      int rolledUp = 1;
      for (int setColumnIndex : setColumnIndexes) {
        if (setColumnIndex == argUnionIndex) {
          rolledUp = 0;
          break;
        }
      }
      result = (result << 1) | rolledUp;
    }
    return result;
  }

  /// Precomputes the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value for every grouping set of
  /// a query, indexed by the set's ordinal (the value of the {@link #GROUPING_ID_COLUMN} discriminator). The
  /// value of a GROUPING call is fully determined by the grouping set a row belongs to, so evaluation per row
  /// is a single lookup: {@code values[ordinal]}.
  ///
  /// @param groupingSets per grouping set (in ordinal order), the union-column indexes participating in it
  /// @param argUnionIndexes the union-column index of each GROUPING argument, in argument order
  /// @return the GROUPING / GROUPING_ID value per grouping-set ordinal
  public static int[] groupingValuesByOrdinal(List<int[]> groupingSets, int[] argUnionIndexes) {
    if (argUnionIndexes.length > MAX_GROUPING_FUNCTION_ARGS) {
      throw new IllegalStateException(
          "GROUPING / GROUPING_ID supports at most " + MAX_GROUPING_FUNCTION_ARGS + " arguments, got "
              + argUnionIndexes.length);
    }
    int numSets = groupingSets.size();
    int[] values = new int[numSets];
    for (int ordinal = 0; ordinal < numSets; ordinal++) {
      values[ordinal] = groupingValue(groupingSets.get(ordinal), argUnionIndexes);
    }
    return values;
  }

  /// Boxed-list overload of {@link #groupingValuesByOrdinal(List, int[])} for callers holding the wire / plan
  /// representation ({@code List<List<Integer>>} sets and {@code List<Integer>} argument indexes).
  public static int[] groupingValuesByOrdinal(List<? extends List<Integer>> groupingSets,
      List<Integer> argUnionIndexes) {
    List<int[]> sets = new ArrayList<>(groupingSets.size());
    for (List<Integer> groupingSet : groupingSets) {
      int[] set = new int[groupingSet.size()];
      for (int i = 0; i < set.length; i++) {
        set[i] = groupingSet.get(i);
      }
      sets.add(set);
    }
    int[] args = new int[argUnionIndexes.size()];
    for (int i = 0; i < args.length; i++) {
      args[i] = argUnionIndexes.get(i);
    }
    return groupingValuesByOrdinal(sets, args);
  }

  /// Returns true if the given function name denotes {@code GROUPING} or {@code GROUPING_ID}. The argument is
  /// the post-canonicalization form (lower-cased, underscores removed), so {@code GROUPING_ID} appears as
  /// {@code "groupingid"}. Centralizes the name contract shared by the broker post-aggregation handler and the
  /// ORDER BY comparator so the two sites cannot drift.
  public static boolean isGroupingFunction(String functionName) {
    return functionName.equalsIgnoreCase("grouping") || functionName.equalsIgnoreCase("groupingid");
  }
}
