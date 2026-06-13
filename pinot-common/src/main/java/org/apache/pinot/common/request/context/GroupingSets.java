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


/**
 * Conventions and pure helpers for SQL {@code GROUPING SETS} / {@code ROLLUP} / {@code CUBE} support.
 *
 * <p>This is the single source of truth for the bit conventions intended to be shared by both query front-ends
 * (the single-stage {@code CalciteSqlParser} path and, as it is added incrementally, the multi-stage Calcite
 * planner path) and the server-side executor. It is intentionally free of any Calcite or engine dependency so it
 * can be unit-tested in isolation and reused everywhere. (Today only the single-stage parser consumes it.)
 *
 * <h3>Representation</h3>
 * A grouping-set query has {@code N} group-by columns: the de-duplicated <i>union</i> of every column that
 * appears in any grouping set, in a stable order ({@code groupByList} on the request). Each grouping set is a
 * subset of those columns, encoded as a <b>participation mask</b>:
 * <pre>
 *   participationMask bit i (i.e. {@code (mask &gt;&gt;&gt; i) &amp; 1}) == 1  &lt;=&gt;  union column i participates in the set
 * </pre>
 * The list of participation masks is what travels on the wire ({@code PinotQuery.groupingSetsMasks}) and what
 * the executor consumes to decide, per set, which columns to group by.
 *
 * <h3>The {@code $groupingId} value</h3>
 * Each output row carries a synthetic INT column, {@code $groupingId}, holding the SQL {@code GROUPING_ID} over
 * <i>all</i> {@code N} union columns in union order (column 0 is the most significant bit):
 * <pre>
 *   $groupingId bit (N-1-i) == 1  &lt;=&gt;  union column i is rolled up (NOT in the set) in this row
 * </pre>
 * so an all-columns-present row has {@code $groupingId == 0} and a grand-total row has {@code $groupingId ==
 * (1&lt;&lt;N)-1}. This value disambiguates a rolled-up NULL from a real data NULL and powers the indicator
 * functions:
 * <ul>
 *   <li>{@code GROUPING(col_i)}      = {@code grouping($groupingId, N, i)}            (0 or 1)</li>
 *   <li>{@code GROUPING_ID(c1..cp)}  = {@code groupingId($groupingId, N, [i1..ip])}   (first arg most significant)</li>
 * </ul>
 *
 * <h3>Limits</h3>
 * Masks are 32-bit, so at most 31 union group-by columns are supported (already far beyond any practical
 * {@code CUBE}, whose set count is {@code 2^N}).
 */
public final class GroupingSets {
  private GroupingSets() {
  }

  /**
   * Name of the synthetic INT column the engine attaches to every grouping-set result row, holding that row's
   * {@code $groupingId} value (the {@code GROUPING_ID} over all union columns). It is internal: never user-selectable,
   * surfaced only through {@code GROUPING()} / {@code GROUPING_ID()}. The {@code $} prefix avoids collision with real
   * columns (Pinot reserves it for internal columns).
   */
  public static final String GROUPING_ID_COLUMN = "$groupingId";

  /** Maximum number of union group-by columns (mask is a 32-bit int; bit 31 reserved for sign-safety). */
  public static final int MAX_GROUP_BY_COLUMNS = 31;

  /**
   * Safety cap on the total number of grouping sets a single query may expand to. {@code CUBE(n)} alone produces
   * {@code 2^n} sets and the Cartesian product of grouping elements multiplies, so without a cap a short query
   * (e.g. {@code CUBE} of ~30 columns) would allocate billions of masks and OOM the broker at parse time. Callers
   * must enforce this <i>before</i> eagerly materializing the masks.
   */
  public static final int MAX_GROUPING_SETS = 1 << 13;

  /** Returns whether union column {@code columnIndex} participates in the set described by {@code participationMask}. */
  public static boolean participates(int participationMask, int columnIndex) {
    return (participationMask & (1 << columnIndex)) != 0;
  }

  /** Builds a participation mask for the given union column indices. */
  public static int maskOf(int... columnIndices) {
    int mask = 0;
    for (int columnIndex : columnIndices) {
      mask |= 1 << columnIndex;
    }
    return mask;
  }

  /**
   * Returns the {@code $groupingId} value for a set: the {@code GROUPING_ID} over all {@code numColumns} union
   * columns (column 0 most significant), i.e. bit {@code (numColumns-1-i)} is set when column {@code i} is rolled up.
   */
  public static int groupingIdValue(int participationMask, int numColumns) {
    int groupingId = 0;
    for (int i = 0; i < numColumns; i++) {
      if (!participates(participationMask, i)) {
        groupingId |= 1 << (numColumns - 1 - i);
      }
    }
    return groupingId;
  }

  /**
   * {@code GROUPING(col_i)} decoded from a {@code $groupingId} value: 1 if column {@code columnIndex} is rolled up
   * (aggregated away) in this row, else 0.
   */
  public static int grouping(int groupingIdValue, int numColumns, int columnIndex) {
    return (groupingIdValue >>> (numColumns - 1 - columnIndex)) & 1;
  }

  /**
   * {@code GROUPING_ID(c1, ..., cp)} decoded from a {@code $groupingId} value: the per-column grouping bits gathered
   * with the first argument as the most significant bit (SQL convention).
   */
  public static int groupingId(int groupingIdValue, int numColumns, int[] columnIndices) {
    int result = 0;
    for (int columnIndex : columnIndices) {
      result = (result << 1) | grouping(groupingIdValue, numColumns, columnIndex);
    }
    return result;
  }

  /**
   * Expands {@code ROLLUP(c0, c1, ..., ck)} (column indices in order) into participation masks for the prefixes
   * {@code (c0..ck), (c0..c(k-1)), ..., (c0), ()} — {@code k+1} masks, finest first.
   */
  public static List<Integer> rollup(int[] columnIndices) {
    int n = columnIndices.length;
    List<Integer> masks = new ArrayList<>(n + 1);
    for (int prefixLen = n; prefixLen >= 0; prefixLen--) {
      int mask = 0;
      for (int i = 0; i < prefixLen; i++) {
        mask |= 1 << columnIndices[i];
      }
      masks.add(mask);
    }
    return masks;
  }

  /**
   * Expands {@code CUBE(c0, ..., ck)} into participation masks for the full power set ({@code 2^k} masks, the full
   * set first).
   */
  public static List<Integer> cube(int[] columnIndices) {
    int n = columnIndices.length;
    int numSets = 1 << n;
    List<Integer> masks = new ArrayList<>(numSets);
    // Enumerate subsets so that the full set (all present) comes first and the empty set last.
    for (int subset = numSets - 1; subset >= 0; subset--) {
      int mask = 0;
      for (int i = 0; i < n; i++) {
        if ((subset & (1 << i)) != 0) {
          mask |= 1 << columnIndices[i];
        }
      }
      masks.add(mask);
    }
    return masks;
  }

  /**
   * Cartesian product of the per-element mask lists, combining selected masks with bitwise OR. Each grouping
   * element of a {@code GROUP BY} list (a plain column, a {@code ROLLUP}, a {@code CUBE}, or a {@code GROUPING SETS})
   * contributes one list of masks; the resulting grouping sets are every combination, one mask per element, OR-ed
   * together. Duplicates are preserved (SQL semantics).
   *
   * <p>Example: {@code GROUP BY a, ROLLUP(b, c)} =&gt; elements {@code [[a], [bc, b, ()]]} =&gt;
   * {@code [abc, ab, a]}.
   */
  public static List<Integer> crossProduct(List<List<Integer>> perElementMasks) {
    List<Integer> result = new ArrayList<>();
    result.add(0);
    for (List<Integer> elementMasks : perElementMasks) {
      List<Integer> next = new ArrayList<>(result.size() * elementMasks.size());
      for (int accumulated : result) {
        for (int elementMask : elementMasks) {
          next.add(accumulated | elementMask);
        }
      }
      result = next;
    }
    return result;
  }
}
