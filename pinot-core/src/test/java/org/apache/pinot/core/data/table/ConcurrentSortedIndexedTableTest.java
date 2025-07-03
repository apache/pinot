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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConcurrentSortedIndexedTableTest {

  @Test
  public void testSingleValueAsc() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 ORDER BY d1 LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    ConcurrentSortedIndexedTable table =
        new ConcurrentSortedIndexedTable(dataSchema, false, queryContext, 10, executor, comparator);

    // Insert out-of-order keys
    upsert(table, new Object[]{"zebra", 1L});
    upsert(table, new Object[]{"apple", 1L});
    upsert(table, new Object[]{"applee", 1L});
    upsert(table, new Object[]{"banana", 1L});
    upsert(table, new Object[]{"cherry", 1L});
    upsert(table, new Object[]{"yak", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[] expected = {"apple", "applee", "banana"};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testMultiThreadedWritersTop5000Asc()
      throws Exception {
    for (int i = 0; i < 5; i++) {
      testMultiThreadedWritersTop5000AscInternal();
    }
  }

  public void testMultiThreadedWritersTop5000AscInternal()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 ORDER BY d1 LIMIT 5000");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(
        queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(),
        queryContext.isNullHandlingEnabled()
    );

    int numThreads = 5;
    int max = 10000;

    ConcurrentSortedIndexedTable table = new ConcurrentSortedIndexedTable(
        dataSchema, false, queryContext, 5000, executor, comparator);

    Runnable writerTask = () -> {
      for (int i = max; i >= 1; i--) {
        Object[] row = new Object[]{i, 1L};
        table.upsert(new Record(row));
      }
    };

    // Launch writer threads
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < numThreads; t++) {
      Thread thread = new Thread(writerTask);
      thread.start();
      threads.add(thread);
    }

    // Wait for all threads to finish
    for (Thread thread : threads) {
      thread.join();
    }

    // Finalize table
    table.finish(false);

    Iterator<Record> it = table.iterator();
    int curVal = 1;
    long count = 5;
    while (it.hasNext()) {
      Record record = it.next();
      Assert.assertEquals(record.getValues()[0], curVal++);
      Assert.assertEquals(record.getValues()[1], count);
    }
  }

  @Test
  public void testMultiValueAscDescOrderFlipped()
      throws Exception {
    for (int i = 0; i < 5; i++) {
      testMultiValueAscDescOrderFlippedInternal();
    }
  }

  public void testMultiValueAscDescOrderFlippedInternal()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d2 DESC, d1 LIMIT 5000");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    ConcurrentSortedIndexedTable table =
        new ConcurrentSortedIndexedTable(dataSchema, false, queryContext, 5000, executor, comparator);

    int numThreads = 5;
    int max = 100;

    Runnable writerTask = () -> {
      for (int i = max; i >= 1; i--) {
        for (int j = 1; j <= max; j++) {
          Object[] row = new Object[]{i, j, 1L};
          table.upsert(new Record(row));
        }
      }
    };

    // Launch writer threads
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < numThreads; t++) {
      Thread thread = new Thread(writerTask);
      thread.start();
      threads.add(thread);
    }

    // Wait for all threads to finish
    for (Thread thread : threads) {
      thread.join();
    }

    table.finish(false);

    Iterator<Record> it = table.iterator();
    int i = 1;
    int j = max;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], i);
      Assert.assertEquals(r.getValues()[1], j);
      Assert.assertEquals(r.getValues()[2], 5L);
      if (i++ == max) {
        i = 1;
        j--;
      }
    }
  }

  private void upsert(ConcurrentSortedIndexedTable table, Object[] row) {
    upsert(table, row, 1);
  }

  private void upsert2cols(ConcurrentSortedIndexedTable table, Object[] row) {
    upsert(table, row, 2);
  }

  private void upsert(ConcurrentSortedIndexedTable table, Object[] row, int numKeyCols) {
    Object[] key = new Object[numKeyCols];
    for (int i = 0; i < numKeyCols; i++) {
      key[i] = row[i];
    }
    table.upsert(new Key(key), new Record(row));
  }
}
