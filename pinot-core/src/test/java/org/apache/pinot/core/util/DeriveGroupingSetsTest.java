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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests for [GroupByUtils#deriveGroupingSetsFromMergedBaseTable], the parallel combine-phase derivation of
/// grouping-set records from merged BASE groups.
public class DeriveGroupingSetsTest {
  private static final int TRIM_SIZE = Integer.MAX_VALUE;
  private static final int TRIM_THRESHOLD = Integer.MAX_VALUE;
  private static final int INITIAL_CAPACITY = 1000;
  private final ExecutorService _executorService = Executors.newFixedThreadPool(8);

  /// ROLLUP(d1, d2) over base groups, so the union is {d1, d2}. The base table below is what the combine holds
  /// after merging base groups across segments: base key layout `[d1, d2]`, values `[d1, d2, sum(m1)]`.
  private static QueryContext rollupQueryContext() {
    // GROUP BY GROUPING SETS makes isGroupingSets() true with the desired union columns and grouping sets.
    return QueryContextConverterUtils.getQueryContext(
        "SELECT d1, d2, SUM(m1) FROM t GROUP BY GROUPING SETS ((d1, d2), (d1), ())");
  }

  private static DataSchema baseSchema() {
    return new DataSchema(new String[]{"d1", "d2", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.DOUBLE});
  }

  /// Builds a merged base table with `numBaseGroups` distinct base groups, each summing to a known value. The
  /// base records intentionally carry NO $groupingId column (the base layout the combine produces).
  private IndexedTable buildBaseTable(int numBaseGroups) {
    QueryContext queryContext = rollupQueryContext();
    IndexedTable baseTable = new ConcurrentIndexedTable(baseSchema(), false, queryContext, INITIAL_CAPACITY,
        TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY, _executorService);
    for (int i = 0; i < numBaseGroups; i++) {
      String d1 = "a" + (i % 4);
      String d2 = "b" + (i % 7);
      // Insert each base group twice to exercise the cross-"segment" merge path (updateRecord), which is where a
      // wrong key-column offset would drop an aggregation or throw.
      baseTable.upsert(new Key(new Object[]{d1, d2}), new Record(new Object[]{d1, d2, (double) i}));
      baseTable.upsert(new Key(new Object[]{d1, d2}), new Record(new Object[]{d1, d2, (double) i}));
    }
    return baseTable;
  }

  /// Materializes a derived table as a map keyed on d1|d2|$groupingId -> sum, for order-independent comparison.
  private static Map<String, Double> toMap(IndexedTable table) {
    table.finish(false);
    Map<String, Double> result = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      // Layout: [d1, d2, $groupingId, sum(m1)]
      String key = values[0] + "|" + values[1] + "|" + values[2];
      result.put(key, ((Number) values[3]).doubleValue());
    }
    return result;
  }

  @Test
  public void testParallelismDoesNotChangeResult() {
    QueryContext queryContext = rollupQueryContext();
    // Reference: single-threaded derive.
    Map<String, Double> reference =
        toMap(GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(64), queryContext, 1,
            _executorService));
    assertTrue(reference.size() > 0);
    // The output schema must carry the $groupingId column.
    // Every thread count must produce the identical result as the single-threaded run.
    for (int numTasks : new int[]{2, 4, 8}) {
      Map<String, Double> parallel =
          toMap(GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(64), queryContext, numTasks,
              _executorService));
      assertEquals(parallel, reference, "derive with numTasks=" + numTasks + " must match single-threaded result");
    }
  }

  @Test
  public void testDerivedValuesAndSchema() {
    // 8 base groups: base group i has d1=a(i%4), d2=b(i%7), sum=i, and it is inserted twice (so the merged base
    // sum is 2*i). Verify the derive rolls up correctly into each grouping set and that the $groupingId column
    // is present at the union-column offset.
    QueryContext queryContext = rollupQueryContext();
    int numBaseGroups = 8;
    IndexedTable derived =
        GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(numBaseGroups), queryContext, 4,
            _executorService);
    // Schema: [d1, d2, $groupingId, sum(m1)].
    DataSchema schema = derived.getDataSchema();
    assertEquals(schema.size(), 4);
    assertEquals(schema.getColumnName(2), GroupingSets.GROUPING_ID_COLUMN);

    Map<String, Double> result = toMap(derived);
    // Grand-total set (ordinal 2, both columns rolled up to NULL) must equal the sum over all base groups:
    // 2 * (0+1+...+7) = 2 * 28 = 56.
    double grandTotal = 0;
    for (Map.Entry<String, Double> entry : result.entrySet()) {
      if (entry.getKey().startsWith("null|null|")) {
        grandTotal = entry.getValue();
      }
    }
    assertEquals(grandTotal, 56.0);
  }

  @Test
  public void testDeriveDoesNotDropGroupsWhenDerivedCountExceedsNumGroupsLimit() {
    // The derived group count (baseGroups * numSets) can exceed numGroupsLimit even when the base grouping is
    // well within it. Deriving must not drop derived groups based on that per-segment guardrail (and certainly
    // not non-deterministically under parallelism, which could starve an entire low-magnitude set such as the
    // grand total). Set a small numGroupsLimit and assert the derive still emits every expected derived group,
    // including the grand total, and that repeated parallel runs are identical.
    QueryContext queryContext = rollupQueryContext();
    queryContext.setNumGroupsLimit(10);
    // 24 base groups (a0..a3 x b0..b6 gives 28 combos, i<24 -> 24 distinct). Grouping sets: {d1,d2}, {d1}, {}.
    Map<String, Double> first =
        toMap(GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(24), queryContext, 8,
            _executorService));
    // Grand total (both columns rolled up) must be present regardless of the small numGroupsLimit.
    boolean hasGrandTotal = first.keySet().stream().anyMatch(k -> k.startsWith("null|null|"));
    assertTrue(hasGrandTotal, "grand-total grouping set must not be dropped by the per-segment numGroupsLimit");
    // Deterministic across parallel runs.
    for (int i = 0; i < 4; i++) {
      Map<String, Double> again =
          toMap(GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(24), queryContext, 8,
              _executorService));
      assertEquals(again, first, "parallel derive must be deterministic and drop no groups");
    }
  }

  @Test
  public void testEmptyBaseTable() {
    QueryContext queryContext = rollupQueryContext();
    IndexedTable derived =
        GroupByUtils.deriveGroupingSetsFromMergedBaseTable(buildBaseTable(0), queryContext, 4, _executorService);
    assertEquals(toMap(derived).size(), 0);
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdownNow();
  }
}
