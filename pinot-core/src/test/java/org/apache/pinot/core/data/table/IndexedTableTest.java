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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.testng.annotations.Test;


/**
 * Tests the {@link Table} operations
 */
public class IndexedTableTest {

  @Test
  public void testConcurrentIndexedTable() throws InterruptedException, TimeoutException, ExecutionException {
    Table indexedTable = new ConcurrentIndexedTable();

    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    Map<String, String> params1 = new HashMap<>();
    params1.put("column", "m1");
    agg1.setAggregationParams(params1);
    agg1.setAggregationType("sum");
    AggregationInfo agg2 = new AggregationInfo();
    Map<String, String> params2 = new HashMap<>();
    params2.put("column", "m2");
    agg2.setAggregationParams(params2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    SelectionSort sel = new SelectionSort();
    sel.setColumn("sum(m1)");
    sel.setIsAsc(true);
    List<SelectionSort> orderBy = Lists.newArrayList(sel);

    // max capacity 5, buffered capacity at 10
    indexedTable.init(dataSchema, aggregationInfos, orderBy, 5, false);

    // 3 threads upsert together
    // a inserted 6 times (60), b inserted 5 times (50), d inserted 2 times (20)
    // inserting 14 unique records
    // c (10000) and f (20000) should be trimmed out no matter what
    // a (60) and i (500) trimmed out after size()

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    try {
      Callable<Void> c1 = () -> {
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d}, new Object[]{10000d, 300d})); // eviction candidate
        indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
        indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
        indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
        return null;
      };

      Callable<Void> c2 = () -> {
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d}, new Object[]{20000d, 600d})); // eviction candidate
        indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
        indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d}, new Object[]{10d, 800d}));
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d}, new Object[]{500d, 900d}));
        return null;
      };

      Callable<Void> c3 = () -> {
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(getRecord(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));
        indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(getRecord(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(getRecord(new Object[]{"l", 12, 120d}, new Object[]{10d, 1200d}));
        indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d})); // trimming candidate
        indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(getRecord(new Object[]{"m", 13, 130d}, new Object[]{10d, 1300d}));
        indexedTable.upsert(getRecord(new Object[]{"n", 14, 140d}, new Object[]{10d, 1400d}));
        return null;
      };

      List<Future<Void>> futures = executorService.invokeAll(Lists.newArrayList(c1, c2, c3));
      for (Future future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      indexedTable.finish();
      Assert.assertEquals(indexedTable.size(), 5);
      checkEvicted(indexedTable, "c", "f");

    } finally {
      executorService.shutdown();
    }
  }


  @Test
  public void testNonConcurrentIndexedTable() {

    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    Map<String, String> params1 = new HashMap<>();
    params1.put("column", "m1");
    agg1.setAggregationParams(params1);
    agg1.setAggregationType("sum");
    AggregationInfo agg2 = new AggregationInfo();
    Map<String, String> params2 = new HashMap<>();
    params2.put("column", "m2");
    agg2.setAggregationParams(params2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    SelectionSort sel = new SelectionSort();
    sel.setColumn("sum(m1)");
    sel.setIsAsc(true);
    List<SelectionSort> orderBy = Lists.newArrayList(sel);

    IndexedTable simpleIndexedTable = new SimpleIndexedTable();
    // max capacity 5, buffered capacity 10
    simpleIndexedTable.init(dataSchema, aggregationInfos, orderBy, 5, false);
    // merge table
    IndexedTable mergeTable = new SimpleIndexedTable();
    mergeTable.init(dataSchema, aggregationInfos, orderBy, 10, false);
    testNonConcurrent(simpleIndexedTable, mergeTable);

    IndexedTable concurrentIndexedTable = new ConcurrentIndexedTable();
    // max capacity 5, buffered capacity 10
    concurrentIndexedTable.init(dataSchema, aggregationInfos, orderBy, 5, false);
    mergeTable = new SimpleIndexedTable();
    mergeTable.init(dataSchema, aggregationInfos, orderBy, 10, false);
    testNonConcurrent(concurrentIndexedTable, mergeTable);

  }

  private void testNonConcurrent(IndexedTable indexedTable, IndexedTable mergeTable) {

    // 2 unique rows
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 1);
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 2);

    // repeat row a
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 2);

    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d}, new Object[]{10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d}, new Object[]{10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
    indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d}, new Object[]{10d, 600d}));
    indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
    indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d}, new Object[]{10d, 800d}));
    indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d}, new Object[]{10d, 900d}));

    // reached max capacity
    Assert.assertEquals(indexedTable.size(), 9);

    // repeat row b
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 9);

    // insert 1 more rows to reach buffer limit
    indexedTable.upsert(getRecord(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));

    // resized to 5
    Assert.assertEquals(indexedTable.size(), 5);
    checkEvicted(indexedTable, "a", "b", "c", "d", "e");
    checkAggregations(indexedTable, 30d, 20d);

    // filling up again
    indexedTable.upsert(getRecord(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
    indexedTable.upsert(getRecord(new Object[]{"l", 12, 120d}, new Object[]{10d, 1200d}));
    indexedTable.upsert(getRecord(new Object[]{"m", 13, 130d}, new Object[]{10d, 1300d}));
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    // repeat f
    indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d}, new Object[]{10d, 600d}));
    // repeat g
    indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
    Assert.assertEquals(indexedTable.size(), 9);


    // repeat record j
    mergeTable.upsert(getRecord(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));
    // repeat record k
    mergeTable.upsert(getRecord(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
    // repeat record b
    mergeTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    // insert new record n
    mergeTable.upsert(getRecord(new Object[]{"n", 14, 140d}, new Object[]{10d, 1400d}));
    Assert.assertEquals(mergeTable.size(), 4);

    // merge with table
    indexedTable.merge(mergeTable);
    Assert.assertEquals(indexedTable.size(), 5);
    checkEvicted(indexedTable, "b", "j", "k", "f", "g");

    indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d}, new Object[]{100d, 800d}));
    indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d}, new Object[]{50d, 900d}));
    mergeTable.upsert(getRecord(new Object[]{"n", 14, 140d}, new Object[]{600d, 1400d}));

    // finish
    indexedTable.finish();
    Assert.assertEquals(indexedTable.size(), 5);
  }

  private void checkAggregations(Table indexedTable, double... evicted) {
    Iterator<Record> iterator = indexedTable.iterator();
    Set<Double> actualAgg = new HashSet<>();
    while (iterator.hasNext()) {
      actualAgg.add((double) iterator.next().getValues()[0]);
    }
    for (double d : evicted) {
      Assert.assertFalse(actualAgg.contains(d));
    }
  }

  private void checkEvicted(Table indexedTable, String... evicted) {
    Iterator<Record> iterator = indexedTable.iterator();
    List<String> d1 = new ArrayList<>();
    while (iterator.hasNext()) {
      d1.add((String) iterator.next().getKey().getColumns()[0]);
    }
    for (String s : evicted) {
      Assert.assertFalse(d1.contains(s));
    }
  }

  private Record getRecord(Object[] keys, Object[] values) {
    return new Record(new Key(keys), values);
  }

  @Test
  public void testNoMoreNewRecords() {
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    Map<String, String> params1 = new HashMap<>();
    params1.put("column", "m1");
    agg1.setAggregationParams(params1);
    agg1.setAggregationType("sum");
    AggregationInfo agg2 = new AggregationInfo();
    Map<String, String> params2 = new HashMap<>();
    params2.put("column", "m2");
    agg2.setAggregationParams(params2);
    agg2.setAggregationType("max");
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(agg1, agg2);

    IndexedTable indexedTable = new SimpleIndexedTable();
    indexedTable.init(dataSchema, aggregationInfos, null, 5, false);
    testNoMoreNewRecordsInTable(indexedTable);

    indexedTable = new ConcurrentIndexedTable();
    indexedTable.init(dataSchema, aggregationInfos, null, 5, false);
    testNoMoreNewRecordsInTable(indexedTable);
  }

  private void testNoMoreNewRecordsInTable(IndexedTable indexedTable) {
    // Insert 14 records. Check that last 2 never made it.
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    indexedTable.upsert(getRecord(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 2);

    indexedTable.upsert(getRecord(new Object[]{"c", 3, 30d}, new Object[]{10d, 300d}));
    indexedTable.upsert(getRecord(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
    indexedTable.upsert(getRecord(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
    indexedTable.upsert(getRecord(new Object[]{"f", 6, 60d}, new Object[]{10d, 600d}));
    indexedTable.upsert(getRecord(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
    indexedTable.upsert(getRecord(new Object[]{"h", 8, 80d}, new Object[]{10d, 800d}));
    indexedTable.upsert(getRecord(new Object[]{"i", 9, 90d}, new Object[]{10d, 900d}));
    Assert.assertEquals(indexedTable.size(), 9);

    indexedTable.upsert(getRecord(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));
    // no resize. no more records allowed
    indexedTable.upsert(getRecord(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
    indexedTable.upsert(getRecord(new Object[]{"l", 12, 120d}, new Object[]{10d, 1200d}));
    Assert.assertEquals(indexedTable.size(), 10);
    checkEvicted(indexedTable, "k", "l");

    // existing row allowed
    indexedTable.upsert(getRecord(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 10);


  }
}
