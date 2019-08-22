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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    // max capacity 10, evict at 12, evict until 11
    indexedTable.init(dataSchema, aggregationInfos, orderBy, 10);

    // 3 threads upsert together
    // a inserted 6 times (60), b inserted 5 times (50), d inserted 2 times (20)
    // buffered capacity 12.
    // inserting 14 unique records
    // c (10000) and f (20000) should be trimmed out before size()
    // a (60) and i (500) trimmed out after size()

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    try {
      Callable<Void> c1 = () -> {
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(new Record(new Object[]{"c", 3, 30d}, new Object[]{10000d, 300d})); // eviction candidate
        indexedTable.upsert(new Record(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
        indexedTable.upsert(new Record(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
        indexedTable.upsert(new Record(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
        return null;
      };

      Callable<Void> c2 = () -> {
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(new Record(new Object[]{"f", 6, 60d}, new Object[]{20000d, 600d})); // eviction candidate
        indexedTable.upsert(new Record(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
        indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(new Record(new Object[]{"h", 8, 80d}, new Object[]{10d, 800d}));
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(new Record(new Object[]{"i", 9, 90d}, new Object[]{500d, 900d}));
        return null;
      };

      Callable<Void> c3 = () -> {
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(new Record(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));
        indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(new Record(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
        indexedTable.upsert(new Record(new Object[]{"l", 12, 120d}, new Object[]{10d, 1200d}));
        indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d})); // trimming candidate
        indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
        indexedTable.upsert(new Record(new Object[]{"m", 13, 130d}, new Object[]{10d, 1300d}));
        indexedTable.upsert(new Record(new Object[]{"n", 14, 140d}, new Object[]{10d, 1400d}));
        return null;
      };

      List<Future<Void>> futures = executorService.invokeAll(Lists.newArrayList(c1, c2, c3));
      for (Future future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      Assert.assertEquals(indexedTable.size(), 12);

      indexedTable.finish();
      Assert.assertEquals(indexedTable.size(), 10);
      checkSurvivors(indexedTable, "a", "i", "c", "f");
      checkAggregations(indexedTable, Lists.newArrayList(10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 20d, 50d));

    } finally {
      executorService.shutdown();
    }
  }


  @Test
  public void testSimpleIndexedTable() {
    Table indexedTable = new SimpleIndexedTable();

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

    // max capacity 10, evict at 12, evict until 11
    indexedTable.init(dataSchema, aggregationInfos, orderBy, 10);

    // 2 unique rows
    indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 1);
    indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 2);

    // repeat row a
    indexedTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    Assert.assertEquals(indexedTable.size(), 2);

    // check aggregations
    checkAggregations(indexedTable, Lists.newArrayList(20d, 10d));

    indexedTable.upsert(new Record(new Object[]{"c", 3, 30d}, new Object[]{10d, 300d}));
    indexedTable.upsert(new Record(new Object[]{"d", 4, 40d}, new Object[]{10d, 400d}));
    indexedTable.upsert(new Record(new Object[]{"e", 5, 50d}, new Object[]{10d, 500d}));
    indexedTable.upsert(new Record(new Object[]{"f", 6, 60d}, new Object[]{10d, 600d}));
    indexedTable.upsert(new Record(new Object[]{"g", 7, 70d}, new Object[]{10d, 700d}));
    indexedTable.upsert(new Record(new Object[]{"h", 8, 80d}, new Object[]{10d, 800d}));
    indexedTable.upsert(new Record(new Object[]{"i", 9, 90d}, new Object[]{10d, 900d}));
    indexedTable.upsert(new Record(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));

    // reached max capacity
    Assert.assertEquals(indexedTable.size(), 10);

    // repeat row b
    indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 10);

    // insert 2 more rows to reach buffer limit
    indexedTable.upsert(new Record(new Object[]{"k", 11, 110d}, new Object[]{10d, 1100d}));
    indexedTable.upsert(new Record(new Object[]{"l", 12, 120d}, new Object[]{10d, 1200d}));
    Assert.assertEquals(indexedTable.size(), 12);

    // repeat row b
    indexedTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    Assert.assertEquals(indexedTable.size(), 12);

    // check aggregations
    checkAggregations(indexedTable, Lists.newArrayList(20d, 30d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d));

    // new row, reorder and evict lowest 1 record
    indexedTable.upsert(new Record(new Object[]{"m", 13, 130d}, new Object[]{10d, 1300d}));
    Assert.assertEquals(indexedTable.size(), 12);

    // check the survivors, b should have been evicted
    checkSurvivors(indexedTable, "b");

    // check aggregations
    checkAggregations(indexedTable, Lists.newArrayList(10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 20d, 10d));

    // merge table
    Table mergeTable = new ConcurrentIndexedTable();
    mergeTable.init(dataSchema, aggregationInfos, orderBy, 10);
    // repeat record j
    mergeTable.upsert(new Record(new Object[]{"j", 10, 100d}, new Object[]{10d, 1000d}));
    // repeat record a
    mergeTable.upsert(new Record(new Object[]{"a", 1, 10d}, new Object[]{10d, 100d}));
    // insert evicted record b
    mergeTable.upsert(new Record(new Object[]{"b", 2, 20d}, new Object[]{10d, 200d}));
    // insert new record
    mergeTable.upsert(new Record(new Object[]{"n", 14, 140d}, new Object[]{10d, 1400d}));
    Assert.assertEquals(mergeTable.size(), 4);

    // merge with table
    indexedTable.merge(mergeTable);
    Assert.assertEquals(indexedTable.size(), 12);

    // check survivors, a and j should be evicted
    checkSurvivors(indexedTable, "a", "j");

    // check aggregations
    checkAggregations(indexedTable, Lists.newArrayList(10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d));

    // finish
    indexedTable.finish();
    Assert.assertEquals(indexedTable.size(), 10);
  }

  private void checkAggregations(Table indexedTable, List<Double> expectedAgg) {
    Iterator<Record> iterator = indexedTable.iterator();
    List<Double> actualAgg = new ArrayList<>();
    while (iterator.hasNext()) {
      actualAgg.add((Double) iterator.next().getValues()[0]);
    }
    Assert.assertEquals(actualAgg, expectedAgg);
  }

  private void checkSurvivors(Table indexedTable, String... evicted) {
    Iterator<Record> iterator = indexedTable.iterator();
    List<String> d1 = new ArrayList<>();
    while (iterator.hasNext()) {
      d1.add((String) iterator.next().getKeys()[0]);
    }
    for (String s : evicted) {
      Assert.assertFalse(d1.contains(s));
    }
  }
}
