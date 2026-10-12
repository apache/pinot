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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies unordered group insertion and atomic updates using independent concurrent writers.
public class UnboundedConcurrentIndexedTableTest {
  @DataProvider(name = "inputModes")
  public Object[][] inputModes() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @DataProvider(name = "concurrentInputModes")
  public Object[][] concurrentInputModes() {
    return new Object[][]{{false, false, false}, {true, false, false}, {false, true, false}, {true, true, false},
        {false, false, true}, {true, false, true}, {false, true, true}, {true, true, true}};
  }

  @DataProvider(name = "scalarInputs")
  public Object[][] scalarInputs() {
    return new Object[][]{{false}, {true}};
  }

  @DataProvider(name = "singleAggregations")
  public Object[][] singleAggregations() {
    Supplier<Object[]> numericInputs = () -> new Object[]{3d, 5d};
    Supplier<Object[]> avgInputs = () -> new Object[]{new AvgPair(9, 3), new AvgPair(11, 1)};
    Supplier<Object[]> distinctInputs = () -> new Object[]{new IntOpenHashSet(new int[]{1, 2}),
        new IntOpenHashSet(new int[]{2, 3})};
    Supplier<Object[]> finalDistinctInputs = () -> new Object[]{3, 5};
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
        {"DISTINCTCOUNT(metric)", true, finalDistinctInputs, 8, null},
        {"DISTINCTCOUNTHLL(metric)", false, hllInputs, 3L, 0L},
        {"ARRAYAGG(metric, 'INT')", false, arrayInputs, new IntArrayList(new int[]{1, 2, 2, 3}), new IntArrayList()}
    };
  }

  @Test(dataProvider = "singleAggregations")
  public void testSingleAggregationParity(String expression, boolean hasFinalInput, Supplier<Object[]> inputSupplier,
      Object mergedResult, Object emptyResult)
      throws IOException {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      IndexedTable eagerTable = singleAggregationTable(expression, hasFinalInput, inputSupplier, false, executor);
      IndexedTable scalarTable = singleAggregationTable(expression, hasFinalInput, inputSupplier, true, executor);
      assertEquals(scalarTable._lookupMap.keySet(), eagerTable._lookupMap.keySet());
      if (!hasFinalInput) {
        for (Map.Entry<Key, Record> entry : eagerTable._lookupMap.entrySet()) {
          assertEquivalentResult(scalarTable._lookupMap.get(entry.getKey()).getValues()[2],
              entry.getValue().getValues()[2], expression + ": intermediate " + entry.getKey());
        }
      }
      Map<Key, Object> eagerResults = finishSingleAggregation(eagerTable);
      Map<Key, Object> scalarResults = finishSingleAggregation(scalarTable);
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

  @Test(dataProvider = "concurrentInputModes", timeOut = 30_000)
  public void testConcurrentCountsWithHashCollisions(boolean scalarInput, boolean hasFinalInput,
      boolean deterministicTable)
      throws InterruptedException, ExecutionException, TimeoutException {
    int numThreads = 4;
    int iterations = 1_000;
    assertEquals(new Key(new Object[]{"Aa", 1d}).hashCode(), new Key(new Object[]{"BB", 1d}).hashCode());
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*) FROM testTable GROUP BY tv_id, weights");
      IndexedTable table = deterministicTable
          ? new DeterministicConcurrentIndexedTable(newDataSchema(), hasFinalInput, queryContext, Integer.MAX_VALUE,
              Integer.MAX_VALUE, Integer.MAX_VALUE, 1, executor)
          : newTable(Integer.MAX_VALUE, hasFinalInput, executor);
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
      Map<Key, Long> counts = finishAndCount(table, hasFinalInput);
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

  @Test(dataProvider = "scalarInputs", timeOut = 30_000)
  public void testConcurrentSaturatedMutableUpdates(boolean scalarInput)
      throws InterruptedException, ExecutionException, TimeoutException {
    int numThreads = 4;
    int iterations = 1_000;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT AVG(metric) FROM testTable GROUP BY tv_id, weights");
      DataSchema dataSchema = new DataSchema(new String[]{"tv_id", "weights", "avg(metric)"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.OBJECT});
      IndexedTable table = new UnboundedConcurrentIndexedTable(dataSchema, false, queryContext, 1, 1, executor);
      Key hotKey = new Key(new Object[]{"hot", 1d});
      upsert(table, new Record(new Object[]{"hot", 1d, new AvgPair(9, 3)}), scalarInput);
      List<Future<?>> futures = new ArrayList<>();
      for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
        String rejectedKey = "rejected-" + threadIndex;
        futures.add(executor.submit(() -> {
          ready.countDown();
          start.await();
          for (int i = 0; i < iterations; i++) {
            upsert(table, new Record(new Object[]{"hot", 1d, new AvgPair(3, 1)}), scalarInput);
            upsert(table, new Record(new Object[]{rejectedKey, 1d, new AvgPair(100, 1)}), scalarInput);
          }
          return null;
        }));
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS), "Writers did not become ready");
      start.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      assertEquals(table._lookupMap.keySet(), Set.of(hotKey));
      AvgPair result = (AvgPair) table._lookupMap.get(hotKey).getValues()[2];
      assertEquals(result.getCount(), 3L + numThreads * iterations);
      assertEquals(result.getSum(), 9d + 3d * numThreads * iterations);
      assertEquals(finishSingleAggregation(table), Map.of(hotKey, 3d));
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "scalarInputs", timeOut = 30_000)
  public void testPendingInsertionUpdateAfterGroupLimit(boolean scalarInput)
      throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch insertionStarted = new CountDownLatch(1);
    CountDownLatch releaseInsertion = new CountDownLatch(1);
    CountDownLatch updateReady = new CountDownLatch(1);
    AtomicReference<Thread> updateThread = new AtomicReference<>();
    Key hotKey = new Key(new Object[]{"hot", 1d});
    Key otherKey = new Key(new Object[]{"other-key", 1d});
    int hotHash = hotKey.hashCode();
    int otherHash = otherKey.hashCode();
    // Different low spread-hash bits put the keys in separate buckets at every map capacity above one.
    assertTrue(((hotHash ^ (hotHash >>> 16)) & 1) != ((otherHash ^ (otherHash >>> 16)) & 1));
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*) FROM testTable GROUP BY tv_id, weights");
      IndexedTable table = new UnboundedConcurrentIndexedTable(newDataSchema(), false, queryContext, 1, 64, executor) {
        @Override
        protected Record createSingleAggregationRecord(Key key, Object result) {
          if (key.equals(hotKey)) {
            insertionStarted.countDown();
            try {
              assertTrue(releaseInsertion.await(10, TimeUnit.SECONDS), "Pending insertion was not released");
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }
          return super.createSingleAggregationRecord(key, result);
        }
      };
      Future<?> insertion = executor.submit(() -> table.upsertSingleAggregationResult(hotKey, 1L));
      assertTrue(insertionStarted.await(10, TimeUnit.SECONDS), "Insertion did not reach the map computation");
      Future<?> otherInsertion = executor.submit(() -> table.upsertSingleAggregationResult(otherKey, 1L));
      otherInsertion.get(10, TimeUnit.SECONDS);
      assertEquals(table.size(), 1);
      Future<?> update = executor.submit(() -> {
        updateThread.set(Thread.currentThread());
        updateReady.countDown();
        upsert(table, new Record(new Object[]{"hot", 1d, 1L}), scalarInput);
      });
      assertTrue(updateReady.await(10, TimeUnit.SECONDS), "Update writer did not become ready");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!update.isDone() && updateThread.get().getState() != Thread.State.BLOCKED
          && System.nanoTime() < deadline) {
        Thread.onSpinWait();
      }
      assertTrue(update.isDone() || updateThread.get().getState() == Thread.State.BLOCKED,
          "Update must complete or wait for the pending insertion before it is released");
      releaseInsertion.countDown();
      insertion.get(10, TimeUnit.SECONDS);
      update.get(10, TimeUnit.SECONDS);
      assertEquals(finishAndCount(table, false), Map.of(hotKey, 2L, otherKey, 1L));
    } finally {
      releaseInsertion.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Writers did not terminate");
    }
  }

  @Test
  public void testScalarInputRequiresSingleAggregation() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*), SUM(metric) FROM testTable GROUP BY tv_id, weights");
      DataSchema dataSchema = new DataSchema(new String[]{"tv_id", "weights", "count(*)", "sum(metric)"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.LONG,
              ColumnDataType.DOUBLE});
      List<IndexedTable> tables = List.of(
          new SimpleIndexedTable(dataSchema, false, queryContext, 1, Integer.MAX_VALUE, Integer.MAX_VALUE, 1, executor),
          new ConcurrentIndexedTable(dataSchema, false, queryContext, 1, Integer.MAX_VALUE, Integer.MAX_VALUE, 1,
              executor));
      for (IndexedTable table : tables) {
        assertThrows(IllegalStateException.class,
            () -> table.upsertSingleAggregationResult(new Key(new Object[]{"hot", 1d}), 1L));
        assertEquals(table.size(), 0);
      }
    } finally {
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
      assertEquals(finishAndCount(table, hasFinalInput), Map.of(new Key(new Object[]{null, null}), 2L,
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
      Map<Key, Long> firstGroup = Map.of(new Key(new Object[]{"BB", 1d}), 12L);
      Map<IndexedTable, Map<Key, Long>> expectedByTable = Map.of(
          new SimpleIndexedTable(newDataSchema(), hasFinalInput, queryContext, 1, Integer.MAX_VALUE,
              Integer.MAX_VALUE, 1, executor), firstGroup,
          new DeterministicConcurrentIndexedTable(newDataSchema(), hasFinalInput, queryContext, 1,
              Integer.MAX_VALUE, Integer.MAX_VALUE, 1, executor), Map.of(new Key(new Object[]{"Aa", 1d}), 3L),
          new ConcurrentIndexedTable(newDataSchema(), hasFinalInput, orderedQueryContext, 1, 1, 2, 1, executor),
          firstGroup);
      for (Map.Entry<IndexedTable, Map<Key, Long>> entry : expectedByTable.entrySet()) {
        IndexedTable table = entry.getKey();
        upsert(table, new Record(new Object[]{"BB", 1d, 7L}), scalarInput);
        upsert(table, new Record(new Object[]{"Aa", 1d, 3L}), scalarInput);
        upsert(table, new Record(new Object[]{"BB", 1d, 5L}), scalarInput);
        assertEquals(finishAndCount(table, hasFinalInput), entry.getValue());
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

  private IndexedTable singleAggregationTable(String expression, boolean hasFinalInput,
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
    return table;
  }

  private Map<Key, Object> finishSingleAggregation(IndexedTable table) {
    table.finish(false, true);
    Map<Key, Object> results = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      results.put(new Key(new Object[]{values[0], values[1]}), values[2]);
    }
    return results;
  }

  private void assertEquivalentResult(Object actual, Object expected, String message)
      throws IOException {
    if (expected instanceof AvgPair expectedPair) {
      assertTrue(actual instanceof AvgPair, message);
      AvgPair actualPair = (AvgPair) actual;
      assertEquals(actualPair.getSum(), expectedPair.getSum(), message);
      assertEquals(actualPair.getCount(), expectedPair.getCount(), message);
    } else if (expected instanceof HyperLogLog expectedHll) {
      assertTrue(actual instanceof HyperLogLog, message);
      assertEquals(((HyperLogLog) actual).getBytes(), expectedHll.getBytes(), message);
    } else {
      assertTrue(Objects.deepEquals(actual, expected), message);
    }
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

  private Map<Key, Long> finishAndCount(Table table, boolean hasFinalInput) {
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
