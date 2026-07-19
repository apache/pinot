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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the {@link Table} operations
 */
@SuppressWarnings({"rawtypes"})
public class IndexedTableTest {
  private static final int TRIM_SIZE = 10;
  private static final int TRIM_THRESHOLD = 20;
  private static final int INITIAL_CAPACITY = Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY;

  @Test
  public void testConcurrentIndexedTable()
      throws InterruptedException, TimeoutException, ExecutionException {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2, d3 ORDER BY SUM(m1)");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    IndexedTable indexedTable =
        new ConcurrentIndexedTable(dataSchema, false, queryContext, 5, TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY,
            Executors.newCachedThreadPool());

    // 3 threads upsert together
    // a inserted 6 times (60), b inserted 5 times (50), d inserted 2 times (20)
    // inserting 14 unique records
    // c (10000) and f (20000) should be trimmed out no matter what
    // a (60) and i (500) trimmed out after size()

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    try {
      Callable<Void> c1 = () -> {
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"c", 3, 30d}),
            getRecord(new Object[]{"c", 3, 30d, 10000d, 300d})); // eviction candidate
        indexedTable.upsert(getKey(new Object[]{"d", 4, 40d}), getRecord(new Object[]{"d", 4, 40d, 10d, 400d}));
        indexedTable.upsert(getKey(new Object[]{"d", 4, 40d}), getRecord(new Object[]{"d", 4, 40d, 10d, 400d}));
        indexedTable.upsert(getKey(new Object[]{"e", 5, 50d}), getRecord(new Object[]{"e", 5, 50d, 10d, 500d}));
        return null;
      };

      Callable<Void> c2 = () -> {
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"f", 6, 60d}),
            getRecord(new Object[]{"f", 6, 60d, 20000d, 600d})); // eviction candidate
        indexedTable.upsert(getKey(new Object[]{"g", 7, 70d}), getRecord(new Object[]{"g", 7, 70d, 10d, 700d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"h", 8, 80d}), getRecord(new Object[]{"h", 8, 80d, 10d, 800d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"i", 9, 90d}), getRecord(new Object[]{"i", 9, 90d, 500d, 900d}));
        return null;
      };

      Callable<Void> c3 = () -> {
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"j", 10, 100d}), getRecord(new Object[]{"j", 10, 100d, 10d, 1000d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"k", 11, 110d}), getRecord(new Object[]{"k", 11, 110d, 10d, 1100d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"l", 12, 120d}), getRecord(new Object[]{"l", 12, 120d, 10d, 1200d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}),
            getRecord(new Object[]{"a", 1, 10d, 10d, 100d})); // trimming candidate
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"m", 13, 130d}), getRecord(new Object[]{"m", 13, 130d, 10d, 1300d}));
        indexedTable.upsert(getKey(new Object[]{"n", 14, 140d}), getRecord(new Object[]{"n", 14, 140d, 10d, 1400d}));
        return null;
      };

      List<Future<Void>> futures = executorService.invokeAll(Arrays.asList(c1, c2, c3));
      for (Future future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      indexedTable.finish(false);
      Assert.assertEquals(indexedTable.size(), 5);
      checkEvicted(indexedTable, "c", "f");
    } finally {
      executorService.shutdown();
    }
  }

  @DataProvider(name = "serializedPercentileTDigestResults")
  public static Object[][] serializedPercentileTDigestResults() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "serializedPercentileTDigestResults", timeOut = 30_000)
  public void testConcurrentPercentileTDigestUpdatesForSameKey(boolean serialized)
      throws InterruptedException, ExecutionException, TimeoutException {
    int numThreads = 8;
    int segmentsPerThread = 32;
    int valuesPerSegment = 64;
    long expectedSize = (long) numThreads * segmentsPerThread * valuesPerSegment;
    ExpressionContext metricExpression = ExpressionContext.forIdentifier("m1");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT PERCENTILETDIGEST(m1, 75) FROM testTable GROUP BY d1");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "percentileTDigest(m1, 75)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.OBJECT});
    ExecutorService tableExecutor = Executors.newCachedThreadPool();
    ExecutorService writers = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      IndexedTable indexedTable = new ConcurrentIndexedTable(dataSchema, false, queryContext, 1,
          Integer.MAX_VALUE, Integer.MAX_VALUE, INITIAL_CAPACITY, tableExecutor);
      List<Future<Void>> futures = new ArrayList<>(numThreads);
      for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
        int currentThread = threadIndex;
        futures.add(writers.submit(() -> {
          PercentileTDigestAggregationFunction function =
              new PercentileTDigestAggregationFunction(metricExpression, 75.0, false);
          ready.countDown();
          start.await();
          for (int segmentIndex = 0; segmentIndex < segmentsPerThread; segmentIndex++) {
            double[] values = new double[valuesPerSegment];
            long firstValue = ((long) currentThread * segmentsPerThread + segmentIndex) * valuesPerSegment;
            for (int valueIndex = 0; valueIndex < valuesPerSegment; valueIndex++) {
              values[valueIndex] = firstValue + valueIndex;
            }
            AggregationResultHolder resultHolder = function.createAggregationResultHolder();
            function.aggregate(valuesPerSegment, resultHolder,
                Map.of(metricExpression, SyntheticBlockValSets.Double.create(null, values)));
            TDigest digest = function.extractAggregationResult(resultHolder);
            if (serialized) {
              SerializedIntermediateResult serializedResult = function.serializeIntermediateResult(digest);
              digest = function.deserializeIntermediateResult(new CustomObject(serializedResult.getType(),
                  ByteBuffer.wrap(serializedResult.getBytes())));
            }
            indexedTable.upsert(new Key(new Object[]{"same-key"}),
                new Record(new Object[]{"same-key", digest}));
          }
          return null;
        }));
      }
      Assert.assertTrue(ready.await(10, TimeUnit.SECONDS), "Writer threads did not become ready");
      start.countDown();
      for (Future<Void> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      indexedTable.finish(false);
      Assert.assertEquals(indexedTable.size(), 1);
      TDigest result = (TDigest) indexedTable.iterator().next().getValues()[1];
      Assert.assertEquals(result.size(), expectedSize);
      Assert.assertEquals(result.getMin(), 0.0);
      Assert.assertEquals(result.getMax(), expectedSize - 1.0);

      long centroidWeight = 0L;
      double previousMean = Double.NEGATIVE_INFINITY;
      for (Centroid centroid : result.centroids()) {
        Assert.assertTrue(Double.isFinite(centroid.mean()));
        Assert.assertTrue(centroid.count() > 0);
        Assert.assertTrue(centroid.mean() + 1e-12 >= previousMean);
        centroidWeight += centroid.count();
        previousMean = centroid.mean();
      }
      Assert.assertEquals(centroidWeight, expectedSize);

      double previousQuantile = Double.NEGATIVE_INFINITY;
      for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
        double value = result.quantile(quantile);
        Assert.assertTrue(Double.isFinite(value));
        Assert.assertTrue(value >= previousQuantile);
        Assert.assertEquals(value, quantile * (expectedSize - 1), expectedSize * 0.02);
        previousQuantile = value;
      }
    } finally {
      start.countDown();
      writers.shutdownNow();
      tableExecutor.shutdownNow();
    }
  }

  @Test(dataProvider = "initDataProvider")
  public void testNonConcurrentIndexedTable(String orderBy, List<String> survivors) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2, d3, d4 ORDER BY " + orderBy);
    DataSchema dataSchema =
        new DataSchema(new String[]{"d1", "d2", "d3", "d4", "sum(m1)", "max(m2)"}, new ColumnDataType[]{
            ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.INT,
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        });

    // Test SimpleIndexedTable
    IndexedTable indexedTable =
        new SimpleIndexedTable(dataSchema, false, queryContext, 5, TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY,
            Executors.newCachedThreadPool());
    IndexedTable mergeTable =
        new SimpleIndexedTable(dataSchema, false, queryContext, 10, TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY,
            Executors.newCachedThreadPool());
    testNonConcurrent(indexedTable, mergeTable);
    indexedTable.finish(true);
    checkSurvivors(indexedTable, survivors);

    // Test ConcurrentIndexedTable
    indexedTable =
        new ConcurrentIndexedTable(dataSchema, false, queryContext, 5, TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY,
            Executors.newCachedThreadPool());
    mergeTable =
        new SimpleIndexedTable(dataSchema, false, queryContext, 10, TRIM_SIZE, TRIM_THRESHOLD, INITIAL_CAPACITY,
            Executors.newCachedThreadPool());
    testNonConcurrent(indexedTable, mergeTable);
    indexedTable.finish(true);
    checkSurvivors(indexedTable, survivors);
  }

  @DataProvider(name = "initDataProvider")
  public Object[][] initDataProvider() {
    List<Object[]> data = new ArrayList<>();

    // d1 desc
    data.add(new Object[]{"d1 DESC", Arrays.asList("m", "l", "k", "j", "i")});

    // d1 asc
    data.add(new Object[]{"d1", Arrays.asList("a", "b", "c", "d", "e")});

    // sum(m1) desc, d1 asc
    data.add(new Object[]{"SUM(m1) DESC, d1", Arrays.asList("m", "h", "i", "a", "b")});

    // d2 desc
    data.add(new Object[]{"d2 DESC", Arrays.asList("m", "l", "k", "j", "i")});

    // d4 asc, d1 asc
    data.add(new Object[]{"d4, d1 ASC", Arrays.asList("a", "b", "c", "d", "e")});

    return data.toArray(new Object[data.size()][]);
  }

  private void testNonConcurrent(IndexedTable indexedTable, IndexedTable mergeTable) {

    // 2 unique rows
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 1000, 10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 1);
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d, 1000, 10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 2);

    // repeat row a
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 1000, 10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 1000, 10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 2);

    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d, 1000, 10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d, 1000, 10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d, 1000, 10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d, 1000, 10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d, 1000, 10d, 500d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d, 1000, 10d, 500d}));
    Assert.assertEquals(indexedTable.size(), 5);

    // able to insert more, maxCapacity is very high
    indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d, 1000, 10d, 600d}));
    indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d, 1000, 10d, 700d}));
    indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d, 1000, 10d, 800d}));
    indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d, 1000, 10d, 900d}));
    indexedTable.upsert(getRecord(new Object[]{"j", 10, 100d, 1000, 10d, 1000d}));
    Assert.assertEquals(indexedTable.size(), 10);

    // repeat row b
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d, 1000, 10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 10);

    // create merge table, 2 new records for indexedTable, 2 repeat records
    mergeTable.upsert(getRecord(new Object[]{"j", 10, 100d, 1000, 10d, 1000d}));
    mergeTable.upsert(getRecord(new Object[]{"k", 11, 110d, 1000, 10d, 1100d}));
    mergeTable.upsert(getRecord(new Object[]{"b", 2, 20d, 1000, 10d, 200d}));
    mergeTable.upsert(getRecord(new Object[]{"l", 12, 120d, 1000, 10d, 1200d}));
    Assert.assertEquals(mergeTable.size(), 4);
    mergeTable.finish(false);

    // merge with indexed table
    indexedTable.merge(mergeTable);
    Assert.assertEquals(indexedTable.size(), 12);

    // insert more
    indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d, 1000, 100d, 800d}));
    indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d, 1000, 50d, 900d}));
    indexedTable.upsert(getRecord(new Object[]{"m", 13, 130d, 1000, 600d, 1300d}));
    Assert.assertEquals(indexedTable.size(), 13);
  }

  private void checkEvicted(Table indexedTable, String... evicted) {
    Iterator<Record> iterator = indexedTable.iterator();
    List<String> d1 = new ArrayList<>();
    while (iterator.hasNext()) {
      d1.add((String) iterator.next().getValues()[0]);
    }
    for (String s : evicted) {
      Assert.assertFalse(d1.contains(s), "Expected '" + s + "' to be evicted, but it was found in the "
          + "indexed table");
    }
  }

  private void checkSurvivors(Table indexedTable, List<String> survivors) {
    Assert.assertEquals(survivors.size(), indexedTable.size());
    Iterator<Record> iterator = indexedTable.iterator();
    for (String survivor : survivors) {
      Assert.assertEquals(survivor, iterator.next().getValues()[0]);
    }
  }

  private Key getKey(Object[] keys) {
    return new Key(keys);
  }

  private Record getRecord(Object[] columns) {
    return new Record(columns);
  }

  @Test
  public void testNoMoreNewRecords() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2, d3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });

    IndexedTable indexedTable =
        new SimpleIndexedTable(dataSchema, false, queryContext, 5, Integer.MAX_VALUE, Integer.MAX_VALUE,
            INITIAL_CAPACITY, Executors.newCachedThreadPool());
    testNoMoreNewRecordsInTable(indexedTable);

    indexedTable = new ConcurrentIndexedTable(dataSchema, false, queryContext, 5, Integer.MAX_VALUE, Integer.MAX_VALUE,
        INITIAL_CAPACITY, Executors.newCachedThreadPool());
    testNoMoreNewRecordsInTable(indexedTable);
  }

  private void testNoMoreNewRecordsInTable(IndexedTable indexedTable) {
    // Insert 7 records. Check that last 2 never made it.
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 2);

    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d, 10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d, 10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d, 10d, 500d}));
    Assert.assertEquals(indexedTable.size(), 5);

    // no resize. no more records allowed
    indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d, 10d, 600d}));
    indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d, 10d, 700d}));
    Assert.assertEquals(indexedTable.size(), 5);

    // existing row allowed
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d, 10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 5);

    indexedTable.finish(false);

    checkEvicted(indexedTable, "f", "g");
  }
}
