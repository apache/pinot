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
package org.apache.pinot.core.data.table;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies unordered group insertion and atomic updates using independent concurrent writers.
public class UnboundedConcurrentIndexedTableTest {
  @DataProvider(name = "inputModes")
  public Object[][] inputModes() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @DataProvider(name = "singleAggregations")
  public Object[][] singleAggregations() {
    Supplier<Object[]> numericInputs = () -> new Object[]{3d, 5d};
    Supplier<Object[]> avgInputs = () -> new Object[]{new AvgPair(9, 3), new AvgPair(11, 1)};
    Supplier<Object[]> distinctInputs = () -> new Object[]{new IntOpenHashSet(new int[]{1, 2}),
        new IntOpenHashSet(new int[]{2, 3})};
    Supplier<Object[]> hllInputs = () -> {
      HyperLogLog first = new HyperLogLog(8);
      first.offer(1);
      first.offer(2);
      HyperLogLog second = new HyperLogLog(8);
      second.offer(2);
      second.offer(3);
      return new Object[]{first, second};
    };
    Supplier<Object[]> arrayInputs = () -> new Object[]{new IntArrayList(new int[]{1, 2}),
        new IntArrayList(new int[]{2, 3})};
    return new Object[][]{
        {"SUM(metric)", false, numericInputs, 8d, null},
        {"SUM(metric)", true, numericInputs, 8d, null},
        {"MIN(metric)", false, numericInputs, 3d, null},
        {"MIN(metric)", true, numericInputs, 3d, null},
        {"MAX(metric)", false, numericInputs, 5d, null},
        {"MAX(metric)", true, numericInputs, 5d, null},
        {"AVG(metric)", false, avgInputs, 5d, null},
        {"DISTINCTCOUNT(metric)", false, distinctInputs, 3, 0},
        {"DISTINCTCOUNTHLL(metric)", false, hllInputs, 3L, 0L},
        {"ARRAYAGG(metric, 'INT')", false, arrayInputs, new IntArrayList(new int[]{1, 2, 2, 3}), new IntArrayList()}
    };
  }

  @Test(dataProvider = "singleAggregations")
  public void testSingleAggregationParity(String expression, boolean hasFinalInput, Supplier<Object[]> inputSupplier,
      Object mergedResult, Object emptyResult) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Map<Key, Object> eagerResults =
          singleAggregationResults(expression, hasFinalInput, inputSupplier, false, executor);
      Map<Key, Object> scalarResults =
          singleAggregationResults(expression, hasFinalInput, inputSupplier, true, executor);
      assertEquals(eagerResults.size(), 3);
      assertEquals(scalarResults.keySet(), eagerResults.keySet());
      for (String group : List.of("null-first", "null-last", "all-null")) {
        Key key = new Key(new Object[]{group, 1d});
        assertTrue(eagerResults.containsKey(key));
        Object expectedResult = group.equals("all-null") ? emptyResult : mergedResult;
        assertTrue(Objects.deepEquals(eagerResults.get(key), expectedResult), expression + ": " + group);
        assertTrue(Objects.deepEquals(scalarResults.get(key), eagerResults.get(key)), expression + ": " + group);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "inputModes", timeOut = 30_000)
  public void testConcurrentCountsWithHashCollisions(boolean scalarInput, boolean hasFinalInput)
      throws InterruptedException, ExecutionException, TimeoutException {
    int numThreads = 4;
    int iterations = 1_000;
    assertEquals(new Key(new Object[]{"Aa", 1d}).hashCode(), new Key(new Object[]{"BB", 1d}).hashCode());
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      IndexedTable table = newTable(Integer.MAX_VALUE, hasFinalInput, executor);
      List<Future<?>> futures = new ArrayList<>();
      for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
        String distinctKey = "thread-" + threadIndex;
        futures.add(executor.submit(() -> {
          ready.countDown();
          start.await();
          for (int i = 0; i < iterations; i++) {
            upsert(table, new Record(new Object[]{"Aa", 1d, 1L}), scalarInput);
            upsert(table, new Record(new Object[]{"BB", 1d, 1L}), scalarInput);
            upsert(table, new Record(new Object[]{distinctKey, 1d, 1L}), scalarInput);
          }
          return null;
        }));
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS), "Writers did not become ready");
      start.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      Map<Key, Long> counts = counts(table, hasFinalInput);
      assertEquals(counts.size(), numThreads + 2);
      assertEquals(counts.get(new Key(new Object[]{"Aa", 1d})), Long.valueOf(numThreads * iterations));
      assertEquals(counts.get(new Key(new Object[]{"BB", 1d})), Long.valueOf(numThreads * iterations));
      for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
        assertEquals(counts.get(new Key(new Object[]{"thread-" + threadIndex, 1d})), Long.valueOf(iterations));
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "inputModes")
  public void testMaterializedKeyEquality(boolean scalarInput, boolean hasFinalInput) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      IndexedTable table = newTable(Integer.MAX_VALUE, hasFinalInput, executor);
      Object[][] keys = {{null, null}, {"", 0d}, {null, 0d}, {"", null}, {"zero", 0d}, {"zero", -0d},
          {"nan", Double.NaN}, {"nan", Double.longBitsToDouble(0x7ff8000000000001L)}};
      for (Object[] key : keys) {
        upsert(table, new Record(new Object[]{key[0], key[1], 1L}), scalarInput);
        upsert(table, new Record(new Object[]{key[0], key[1], 1L}), scalarInput);
      }
      assertEquals(counts(table, hasFinalInput), Map.of(new Key(new Object[]{null, null}), 2L,
          new Key(new Object[]{"", 0d}), 2L, new Key(new Object[]{null, 0d}), 2L,
          new Key(new Object[]{"", null}), 2L, new Key(new Object[]{"zero", 0d}), 2L,
          new Key(new Object[]{"zero", -0d}), 2L, new Key(new Object[]{"nan", Double.NaN}), 4L));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "inputModes")
  public void testGroupLimitAndRecordOwnership(boolean scalarInput, boolean hasFinalInput) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      IndexedTable table = newTable(1, hasFinalInput, executor);
      Record first = new Record(new Object[]{"Aa", 1d, 3L});
      Record rejected = new Record(new Object[]{"BB", 1d, 7L});
      Record duplicate = new Record(new Object[]{"Aa", 1d, 5L});
      upsert(table, first, scalarInput);
      Record retained = table._lookupMap.get(new Key(new Object[]{"Aa", 1d}));
      if (scalarInput) {
        assertNotSame(retained, first);
      } else {
        assertSame(retained, first);
      }
      assertEquals(retained.getValues(), first.getValues());
      upsert(table, rejected, scalarInput);
      upsert(table, duplicate, scalarInput);
      table.finish(false, hasFinalInput);
      assertEquals(table.size(), 1);
      assertSame(table.iterator().next(), retained);
      assertEquals(retained.getValues(), new Object[]{"Aa", 1d, 8L});
      if (scalarInput) {
        assertEquals(first.getValues(), new Object[]{"Aa", 1d, 3L});
      }
      assertEquals(duplicate.getValues(), new Object[]{"Aa", 1d, 5L});
      assertEquals(rejected.getValues(), new Object[]{"BB", 1d, 7L});
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "inputModes")
  public void testScalarFallbacksRespectTableLimits(boolean scalarInput, boolean hasFinalInput) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*) FROM testTable GROUP BY tv_id, weights");
      QueryContext orderedQueryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*) FROM testTable GROUP BY tv_id, weights ORDER BY COUNT(*) DESC");
      IndexedTable[] tables = {
          new SimpleIndexedTable(newDataSchema(), hasFinalInput, queryContext, 1, Integer.MAX_VALUE,
              Integer.MAX_VALUE, 1, executor),
          new DeterministicConcurrentIndexedTable(newDataSchema(), hasFinalInput, queryContext, 1,
              Integer.MAX_VALUE, Integer.MAX_VALUE, 1, executor),
          new ConcurrentIndexedTable(newDataSchema(), hasFinalInput, orderedQueryContext, 1, 1, 2, 1, executor)
      };
      for (int i = 0; i < tables.length; i++) {
        IndexedTable table = tables[i];
        upsert(table, new Record(new Object[]{"BB", 1d, 7L}), scalarInput);
        upsert(table, new Record(new Object[]{"Aa", 1d, 3L}), scalarInput);
        upsert(table, new Record(new Object[]{"BB", 1d, 5L}), scalarInput);
        Map<Key, Long> expected = i == 1
            ? Map.of(new Key(new Object[]{"Aa", 1d}), 3L)
            : Map.of(new Key(new Object[]{"BB", 1d}), 12L);
        assertEquals(counts(table, hasFinalInput), expected);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private IndexedTable newTable(int resultSize, boolean hasFinalInput, ExecutorService executor) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY tv_id, weights");
    return new UnboundedConcurrentIndexedTable(newDataSchema(), hasFinalInput, queryContext, resultSize, 1, executor);
  }

  private Map<Key, Object> singleAggregationResults(String expression, boolean hasFinalInput,
      Supplier<Object[]> inputSupplier, boolean scalarInput, ExecutorService executor) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SET enableNullHandling=true; SELECT " + expression + " FROM testTable GROUP BY tv_id, weights");
    ColumnDataType resultType = hasFinalInput
        ? queryContext.getAggregationFunctions()[0].getFinalResultColumnType()
        : queryContext.getAggregationFunctions()[0].getIntermediateResultColumnType();
    DataSchema dataSchema = new DataSchema(new String[]{"tv_id", "weights", expression},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, resultType});
    IndexedTable table = new UnboundedConcurrentIndexedTable(dataSchema, hasFinalInput, queryContext, 3, 1, executor);
    Object[] nullFirstInputs = inputSupplier.get();
    Object[] nullLastInputs = inputSupplier.get();
    upsert(table, new Record(new Object[]{"null-first", 1d, null}), scalarInput);
    upsert(table, new Record(new Object[]{"null-last", 1d, nullLastInputs[0]}), scalarInput);
    upsert(table, new Record(new Object[]{"all-null", 1d, null}), scalarInput);
    upsert(table, new Record(new Object[]{"rejected", 1d, inputSupplier.get()[0]}), scalarInput);
    upsert(table, new Record(new Object[]{"null-first", 1d, nullFirstInputs[0]}), scalarInput);
    upsert(table, new Record(new Object[]{"null-first", 1d, nullFirstInputs[1]}), scalarInput);
    upsert(table, new Record(new Object[]{"null-last", 1d, nullLastInputs[1]}), scalarInput);
    upsert(table, new Record(new Object[]{"null-last", 1d, null}), scalarInput);
    upsert(table, new Record(new Object[]{"all-null", 1d, null}), scalarInput);
    table.finish(false, true);
    Map<Key, Object> results = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      results.put(new Key(new Object[]{values[0], values[1]}), values[2]);
    }
    return results;
  }

  private DataSchema newDataSchema() {
    return new DataSchema(new String[]{"tv_id", "weights", "count(*)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.LONG});
  }

  private void upsert(IndexedTable table, Record record, boolean scalarInput) {
    if (scalarInput) {
      Object[] values = record.getValues();
      table.upsertSingleAggregationResult(new Key(new Object[]{values[0], values[1]}), values[2]);
    } else {
      table.upsert(record);
    }
  }

  private Map<Key, Long> counts(Table table, boolean hasFinalInput) {
    table.finish(false, hasFinalInput);
    Map<Key, Long> counts = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      counts.put(new Key(new Object[]{values[0], values[1]}), (Long) values[2]);
    }
    return counts;
  }
}
