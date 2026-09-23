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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.DeterministicConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class GroupByUtilsTest {

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

  @DataProvider
  public Object[][] combineTableShapes() {
    return new Object[][]{
        {1, false, GroupByUtils.CombineTableKind.SIMPLE, SimpleIndexedTable.class},
        {1, true, GroupByUtils.CombineTableKind.SIMPLE, SimpleIndexedTable.class},
        {2, false, GroupByUtils.CombineTableKind.UNBOUNDED_CONCURRENT, UnboundedConcurrentIndexedTable.class},
        {2, true, GroupByUtils.CombineTableKind.CONCURRENT, ConcurrentIndexedTable.class}
    };
  }

  @Test(dataProvider = "combineTableShapes")
  public void testCombineFactoryPreservesTablePolicy(int numThreads, boolean trim,
      GroupByUtils.CombineTableKind expectedKind, Class<?> fallbackClass) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(CalciteSqlParser.compileToPinotQuery(
        "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY COUNT(*) DESC LIMIT 10"));
    queryContext.setMinServerGroupTrimSize(trim ? 100 : 0);
    queryContext.setGroupTrimThreshold(200);
    queryContext.setMinInitialIndexedTableCapacity(128);
    DataSchema schema = new DataSchema(new String[]{"a", "count"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG});
    GroupByResultsBlock block = new GroupByResultsBlock(schema, List.of(), queryContext);
    ExecutorService executor = mock(ExecutorService.class);
    IndexedTable customTable = mock(IndexedTable.class);
    AtomicInteger calls = new AtomicInteger();
    GroupByUtils.CombineTableFactory factory =
        (kind, dataSchema, hasFinalInput, context, resultSize, trimSize, threshold, capacity, executorService) -> {
          calls.incrementAndGet();
          assertEquals(kind, expectedKind);
          assertSame(dataSchema, schema);
          assertSame(context, queryContext);
          assertEquals(hasFinalInput, false);
          assertEquals(resultSize, trim ? 100 : Integer.MAX_VALUE);
          assertEquals(trimSize, trim ? 100 : Integer.MAX_VALUE);
          assertEquals(threshold, trim ? 200 : Integer.MAX_VALUE);
          assertEquals(capacity, 128);
          assertSame(executorService, executor);
          return calls.get() == 1 ? customTable : null;
        };

    assertSame(GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, numThreads, executor, factory),
        customTable);
    assertEquals(GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, numThreads, executor, factory)
        .getClass(), fallbackClass);
    assertEquals(calls.get(), 2);
  }

  @Test
  public void testDeterministicCombineBypassesCustomFactory() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SELECT a, COUNT(*) FROM t GROUP BY a"));
    queryContext.setAccurateGroupByWithoutOrderBy(true);
    DataSchema schema = new DataSchema(new String[]{"a", "count"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG});
    GroupByResultsBlock block = new GroupByResultsBlock(schema, List.of(), queryContext);
    IndexedTable table = GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, 2,
        mock(ExecutorService.class), (kind, dataSchema, hasFinalInput, context, resultSize, trimSize, threshold,
            capacity, executor) -> {
          throw new AssertionError("Deterministic combine must retain its built-in table");
        });
    assertEquals(table.getClass(), DeterministicConcurrentIndexedTable.class);
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
