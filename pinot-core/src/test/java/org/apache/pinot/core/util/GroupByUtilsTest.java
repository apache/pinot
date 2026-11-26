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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class GroupByUtilsTest {
  private static final DataSchema KEY_AND_SUM =
      new DataSchema(new String[]{"a", "sum(m)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});

  @Test
  public void testGetTableCapacity() {
    assertEquals(GroupByUtils.getTableCapacity(0), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1000), 5000);
    assertEquals(GroupByUtils.getTableCapacity(10000), 50000);
    assertEquals(GroupByUtils.getTableCapacity(100000), 500000);
    assertEquals(GroupByUtils.getTableCapacity(1000000), 5000000);
    assertEquals(GroupByUtils.getTableCapacity(10000000), 50000000);
    assertEquals(GroupByUtils.getTableCapacity(100000000), 500000000);
    assertEquals(GroupByUtils.getTableCapacity(1000000000), Integer.MAX_VALUE);
  }

  @Test
  public void getIndexedTableTrimThreshold() {
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, -1), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 0), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000), 100000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000), 1000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000000), 10000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000000), 100000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000000), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000001), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(Integer.MAX_VALUE, 10), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000000, 10), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000001, 10), Integer.MAX_VALUE);
  }

  @Test
  public void testGetIndexedTableInitialCapacity() {
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 128),
        HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 256), 256);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 1000, 256),
        HashUtil.getHashMapCapacity(1000));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 100, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 1000, 256), HashUtil.getHashMapCapacity(100));
  }

  @Test
  public void testGetTrimSize() {
    QueryContext byKeys = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY a LIMIT 10");
    QueryContext byAggregate = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY SUM(m) DESC LIMIT 10");
    assertEquals(GroupByUtils.getTrimSize(byKeys, 10, 5000), 10);
    assertEquals(GroupByUtils.getTrimSize(byKeys, 10, 0), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getTrimSize(byAggregate, 10, 5000), 5000);
    assertEquals(GroupByUtils.getTrimSize(byAggregate, 10_000, 5000), 50_000);
  }

  @Test
  public void testGetTrimThreshold() {
    QueryContext byKeys = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY a LIMIT 10000");
    QueryContext byAggregate = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY SUM(m) DESC LIMIT 10000");
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, 10_000, 1_000_000), 20_000);
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, 10_000, 100), 20_000);
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, 10_000, 0), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, 10_000, 1_000_000_001), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, Integer.MAX_VALUE, 1_000_000), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getTrimThreshold(byKeys, 0, 1_000_000), 1_000_000);
    assertEquals(GroupByUtils.getTrimThreshold(byAggregate, 50_000, 1_000_000), 1_000_000);
  }

  /// Ordered only by its group keys with no HAVING, a combine or reducer table trims down to `LIMIT` groups whenever it
  /// reaches twice that, so it never holds more than `2 * LIMIT`, and still returns the `LIMIT` smallest keys with both
  /// of their values merged: fewer than `LIMIT` keys are smaller than any of them, so no trim drops one. Ordered on an
  /// aggregate it trims at the configured threshold (1,000,000), so it holds all 1,000 groups here.
  @Test
  public void testTablesOrderedOnGroupKeysTrimAtTwiceTheLimit() {
    QueryContext byKeys = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY a LIMIT 10");
    QueryContext byAggregate = queryContext("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY SUM(m) DESC LIMIT 10");
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      for (IndexedTable table : List.of(combineTable(byKeys, executorService), reducerTable(byKeys, executorService))) {
        assertTrue(upsertEachKeyTwice(table) <= 20);
        table.finish(true, false);
        List<Integer> keys = new ArrayList<>();
        table.iterator().forEachRemaining(record -> {
          keys.add((Integer) record.getValues()[0]);
          assertEquals(record.getValues()[1], 2.0);
        });
        assertEquals(keys, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
      }
      assertEquals(upsertEachKeyTwice(combineTable(byAggregate, executorService)), 1000);
      assertEquals(upsertEachKeyTwice(reducerTable(byAggregate, executorService)), 1000);
    } finally {
      executorService.shutdownNow();
    }
  }

  /// Upserts a group for each key from 0 to 999 twice, in a shuffled order and each time with SUM 1, and returns the
  /// most groups the table held after an upsert.
  private static int upsertEachKeyTwice(IndexedTable table) {
    List<Integer> keys = new ArrayList<>();
    for (int key = 0; key < 1000; key++) {
      keys.add(key);
      keys.add(key);
    }
    Collections.shuffle(keys, new Random(42));
    int mostGroups = 0;
    for (int key : keys) {
      table.upsert(new Record(new Object[]{key, 1.0}));
      mostGroups = Math.max(mostGroups, table.size());
    }
    return mostGroups;
  }

  /// Ordered only by its group keys with no HAVING, a combine table returns the `LIMIT` groups with the smallest keys
  /// (the largest when descending), which is every group of the query's result that it has. Ordered on an aggregate,
  /// or with a HAVING, it keeps `max(5 * LIMIT, 5000)` groups, so all 20 groups here.
  @Test
  public void testCombineTableOrderedOnGroupKeysKeepsLimitGroups() {
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      assertEquals(combineTableKeys("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY a LIMIT 10", executorService),
          List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
      assertEquals(combineTableKeys("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY a DESC LIMIT 10", executorService),
          List.of(19, 18, 17, 16, 15, 14, 13, 12, 11, 10));
      assertEquals(
          combineTableKeys("SELECT a, SUM(m) FROM t GROUP BY a ORDER BY SUM(m) DESC LIMIT 10", executorService).size(),
          20);
      assertEquals(combineTableKeys("SELECT a, SUM(m) FROM t GROUP BY a HAVING SUM(m) > 0 ORDER BY a LIMIT 10",
          executorService).size(), 20);
    } finally {
      executorService.shutdownNow();
    }
  }

  /// The keys a combine table for `sql` returns, in order, after one group for each key from 0 to 19.
  private static List<Integer> combineTableKeys(String sql, ExecutorService executorService) {
    IndexedTable table = combineTable(queryContext(sql), executorService);
    for (int key = 0; key < 20; key++) {
      table.upsert(new Record(new Object[]{key, 1.0}));
    }
    table.finish(true, false);
    List<Integer> keys = new ArrayList<>();
    table.iterator().forEachRemaining(record -> keys.add((Integer) record.getValues()[0]));
    return keys;
  }

  /// A server combine table for `queryContext` over the groups `(a INT, sum(m) DOUBLE)`, with the default trim
  /// settings: minServerGroupTrimSize 5,000 and groupTrimThreshold 1,000,000.
  private static IndexedTable combineTable(QueryContext queryContext, ExecutorService executorService) {
    return GroupByUtils.createIndexedTableForCombineOperator(
        new GroupByResultsBlock(KEY_AND_SUM, List.of(), queryContext), queryContext, 2, executorService);
  }

  /// A broker reducer table for `queryContext` over the groups `(a INT, sum(m) DOUBLE)`, with the broker's default trim
  /// settings: minGroupTrimSize 5,000 and groupByTrimThreshold 1,000,000.
  private static IndexedTable reducerTable(QueryContext queryContext, ExecutorService executorService) {
    DataTableReducerContext reducerContext =
        new DataTableReducerContext(executorService, 2, 10_000L, 1_000_000, 5000, 128);
    return GroupByUtils.createIndexedTableForDataTableReducer(
        DataTableBuilderFactory.getDataTableBuilder(KEY_AND_SUM).build(), queryContext, reducerContext, 2,
        executorService);
  }

  private static QueryContext queryContext(String sql) {
    return QueryContextConverterUtils.getQueryContext(CalciteSqlParser.compileToPinotQuery(sql));
  }

  /// Grouping-set queries must keep all groups at the combine and reducer stages (no global ORDER BY trim), so
  /// a row that ranks low on a single segment/server is never dropped before its partial aggregates are merged.
  /// The IndexedTable must therefore be the trim-disabled [UnboundedConcurrentIndexedTable] even under
  /// aggressive trim settings, whereas a plain GROUP BY with the same settings stays trim-enabled.
  @Test
  public void testGroupingSetQueriesDisableCombineAndReducerTrim() {
    QueryContext groupingSets = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SELECT a, COUNT(*) FROM t GROUP BY ROLLUP(a) ORDER BY COUNT(*) DESC"));
    assertTrue(groupingSets.isGroupingSets());
    DataSchema schema = new DataSchema(new String[]{"a", GroupingSets.GROUPING_ID_COLUMN, "count"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG});
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      /// Aggressive trim settings (minGroupTrimSize=1, groupTrimThreshold=1) would enable trim for a plain query.
      DataTableReducerContext aggressiveTrim = new DataTableReducerContext(executorService, 2, 10_000L, 1, 1, 128);
      DataTable dataTable = DataTableBuilderFactory.getDataTableBuilder(schema).build();
      IndexedTable reducerTable =
          GroupByUtils.createIndexedTableForDataTableReducer(dataTable, groupingSets, aggressiveTrim, 2,
              executorService);
      assertTrue(reducerTable instanceof UnboundedConcurrentIndexedTable,
          "grouping-set reducer table must be trim-disabled, got " + reducerTable.getClass().getSimpleName());

      GroupByResultsBlock resultsBlock = new GroupByResultsBlock(schema, List.of(), groupingSets);
      IndexedTable combineTable =
          GroupByUtils.createIndexedTableForCombineOperator(resultsBlock, groupingSets, 2, executorService);
      assertTrue(combineTable instanceof UnboundedConcurrentIndexedTable,
          "grouping-set combine table must be trim-disabled, got " + combineTable.getClass().getSimpleName());

      /// Contrast: a plain GROUP BY with the same aggressive trim settings still uses the trim-enabled table.
      QueryContext plainGroupBy = QueryContextConverterUtils.getQueryContext(
          CalciteSqlParser.compileToPinotQuery("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY COUNT(*) DESC"));
      DataSchema plainSchema = new DataSchema(new String[]{"a", "count"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG});
      DataTableReducerContext trimEnabled = new DataTableReducerContext(executorService, 2, 10_000L, 100, 1, 128);
      DataTable plainDataTable = DataTableBuilderFactory.getDataTableBuilder(plainSchema).build();
      IndexedTable plainReducerTable =
          GroupByUtils.createIndexedTableForDataTableReducer(plainDataTable, plainGroupBy, trimEnabled, 2,
              executorService);
      assertEquals(plainReducerTable.getClass(), ConcurrentIndexedTable.class,
          "plain GROUP BY with aggressive trim must remain trim-enabled");
    } finally {
      executorService.shutdownNow();
    }
  }
}
