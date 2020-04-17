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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the {@link Table} operations
 */
public class IndexedTableTest {

  @Test
  public void testConcurrentIndexedTable() throws InterruptedException, TimeoutException, ExecutionException {

    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    List<String> args1 = new ArrayList<>(1);
    args1.add("m1");
    agg1.setAggregationFunctionArgs(args1);
    agg1.setAggregationType("sum");

    AggregationInfo agg2 = new AggregationInfo();
    List<String> args2 = new ArrayList<>(1);
    args2.add("m2");
    agg2.setAggregationFunctionArgs(args2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    SelectionSort sel = new SelectionSort();
    sel.setColumn("sum(m1)");
    sel.setIsAsc(true);
    List<SelectionSort> orderBy = Lists.newArrayList(sel);

    IndexedTable indexedTable = new ConcurrentIndexedTable(dataSchema, aggregationInfos, orderBy, 5);

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
        indexedTable.upsert(getKey(new Object[]{"c", 3, 30d}), getRecord(new Object[]{"c", 3, 30d, 10000d, 300d})); // eviction candidate
        indexedTable.upsert(getKey(new Object[]{"d", 4, 40d}), getRecord(new Object[]{"d", 4, 40d, 10d, 400d}));
        indexedTable.upsert(getKey(new Object[]{"d", 4, 40d}), getRecord(new Object[]{"d", 4, 40d, 10d, 400d}));
        indexedTable.upsert(getKey(new Object[]{"e", 5, 50d}), getRecord(new Object[]{"e", 5, 50d, 10d, 500d}));
        return null;
      };

      Callable<Void> c2 = () -> {
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d, 10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"f", 6, 60d}), getRecord(new Object[]{"f", 6, 60d,20000d, 600d})); // eviction candidate
        indexedTable.upsert(getKey(new Object[]{"g", 7, 70d}), getRecord(new Object[]{"g", 7, 70d,10d, 700d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d,10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d,10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"h", 8, 80d}), getRecord(new Object[]{"h", 8, 80d,10d, 800d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d,10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"i", 9, 90d}), getRecord(new Object[]{"i", 9, 90d,500d, 900d}));
        return null;
      };

      Callable<Void> c3 = () -> {
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d,10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"j", 10, 100d}), getRecord(new Object[]{"j", 10, 100d,10d, 1000d}));
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d,10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"k", 11, 110d}), getRecord(new Object[]{"k", 11, 110d,10d, 1100d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d,10d, 100d}));
        indexedTable.upsert(getKey(new Object[]{"l", 12, 120d}), getRecord(new Object[]{"l", 12, 120d,10d, 1200d}));
        indexedTable.upsert(getKey(new Object[]{"a", 1, 10d}), getRecord(new Object[]{"a", 1, 10d,10d, 100d})); // trimming candidate
        indexedTable.upsert(getKey(new Object[]{"b", 2, 20d}), getRecord(new Object[]{"b", 2, 20d,10d, 200d}));
        indexedTable.upsert(getKey(new Object[]{"m", 13, 130d}), getRecord(new Object[]{"m", 13, 130d,10d, 1300d}));
        indexedTable.upsert(getKey(new Object[]{"n", 14, 140d}), getRecord(new Object[]{"n", 14, 140d,10d, 1400d}));
        return null;
      };

      List<Future<Void>> futures = executorService.invokeAll(Lists.newArrayList(c1, c2, c3));
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

  @Test(dataProvider = "initDataProvider")
  public void testNonConcurrentIndexedTable(List<SelectionSort> orderBy, List<String> survivors) {

    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "d4", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    List<String> args1 = new ArrayList<>(1);
    args1.add("m1");
    agg1.setAggregationFunctionArgs(args1);
    agg1.setAggregationType("sum");

    AggregationInfo agg2 = new AggregationInfo();
    List<String> args2 = new ArrayList<>(1);
    args2.add("m2");
    agg2.setAggregationFunctionArgs(args2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    // Test SimpleIndexedTable
    IndexedTable simpleIndexedTable = new SimpleIndexedTable(dataSchema, aggregationInfos, orderBy, 5);
    // merge table
    IndexedTable mergeTable = new SimpleIndexedTable(dataSchema, aggregationInfos, orderBy, 10);
    testNonConcurrent(simpleIndexedTable, mergeTable);

    // finish
    simpleIndexedTable.finish(true);
    checkSurvivors(simpleIndexedTable, survivors);

    // Test ConcurrentIndexedTable
    IndexedTable concurrentIndexedTable = new ConcurrentIndexedTable(dataSchema, aggregationInfos, orderBy, 5);
    mergeTable = new SimpleIndexedTable(dataSchema, aggregationInfos, orderBy, 10);
    testNonConcurrent(concurrentIndexedTable, mergeTable);

    // finish
    concurrentIndexedTable.finish(true);
    checkSurvivors(concurrentIndexedTable, survivors);
  }

  @DataProvider(name = "initDataProvider")
  public Object[][] initDataProvider() {

    List<Object[]> data = new ArrayList<>();

    SelectionSort sel1;
    SelectionSort sel2;
    List<SelectionSort> orderBy;
    List<String> survivors;

    // d1 desc
    sel1 = new SelectionSort();
    sel1.setColumn("d1");
    sel1.setIsAsc(false);
    orderBy = Lists.newArrayList(sel1);
    survivors = Lists.newArrayList("m", "l", "k", "j", "i");
    data.add(new Object[]{orderBy, survivors});

    // d1 asc
    sel1 = new SelectionSort();
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    orderBy = Lists.newArrayList(sel1);
    survivors = Lists.newArrayList("a", "b", "c", "d", "e");
    data.add(new Object[]{orderBy, survivors});

    // sum(m1) desc, d1 asc
    sel1 = new SelectionSort();
    sel1.setColumn("sum(m1)");
    sel1.setIsAsc(false);
    sel2 = new SelectionSort();
    sel2.setColumn("d1");
    sel2.setIsAsc(true);
    orderBy = Lists.newArrayList(sel1, sel2);
    survivors = Lists.newArrayList("m", "h", "i", "a", "b");
    data.add(new Object[]{orderBy, survivors});

    // d2 desc
    sel1 = new SelectionSort();
    sel1.setColumn("d2");
    sel1.setIsAsc(false);
    orderBy = Lists.newArrayList(sel1);
    survivors = Lists.newArrayList("m", "l", "k", "j", "i");
    data.add(new Object[]{orderBy, survivors});

    // d4 asc, d1 asc
    sel1 = new SelectionSort();
    sel1.setColumn("d4");
    sel1.setIsAsc(true);
    sel2 = new SelectionSort();
    sel2.setColumn("d1");
    sel2.setIsAsc(true);
    orderBy = Lists.newArrayList(sel1, sel2);
    survivors = Lists.newArrayList("a", "b", "c", "d", "e");
    data.add(new Object[]{orderBy, survivors});

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
      Assert.assertFalse(d1.contains(s));
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
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    List<String> args1 = new ArrayList<>(1);
    args1.add("m1");
    agg1.setAggregationFunctionArgs(args1);
    agg1.setAggregationType("sum");

    AggregationInfo agg2 = new AggregationInfo();
    List<String> args2 = new ArrayList<>(1);
    args2.add("m2");
    agg2.setAggregationFunctionArgs(args2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    IndexedTable indexedTable = new SimpleIndexedTable(dataSchema, aggregationInfos, null, 5);
    testNoMoreNewRecordsInTable(indexedTable);

    indexedTable = new ConcurrentIndexedTable(dataSchema, aggregationInfos, null, 5);
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
