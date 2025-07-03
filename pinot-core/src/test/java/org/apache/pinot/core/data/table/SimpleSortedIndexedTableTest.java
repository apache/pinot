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

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SimpleSortedIndexedTableTest {

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
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

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
  public void testSingleValueDesc() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 ORDER BY d1 DESC LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert(table, new Object[]{"zebra", 1L});
    upsert(table, new Object[]{"apple", 1L});
    upsert(table, new Object[]{"applee", 1L});
    upsert(table, new Object[]{"banana", 1L});
    upsert(table, new Object[]{"cherry", 1L});
    upsert(table, new Object[]{"yak", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[] expected = {"zebra", "yak", "cherry"};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testSingleValueAscNullEnabled() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 ORDER BY d1 LIMIT 3");
    queryContext.setNullHandlingEnabled(true);
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert(table, new Object[]{"zebra", 1L});
    upsert(table, new Object[]{"apple", 1L});
    upsert(table, new Object[]{"applee", 1L});
    upsert(table, new Object[]{"null", 1L});
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
  public void testMultiValueAscDesc() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d1, d2 DESC LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"zebra", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"applee", "z", 1L});
    upsert2cols(table, new Object[]{"banana", "b", 1L});
    upsert2cols(table, new Object[]{"cherry", "c", 1L});
    upsert2cols(table, new Object[]{"yak", "y", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[][] expected = {{"apple", "z"}, {"apple", "a"}, {"applee", "z"}};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i][0]);
      Assert.assertEquals(r.getValues()[1], expected[i][1]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testMultiValueAscDescOrderFlipped() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d2 DESC, d1 LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"zebra", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"applee", "z", 1L});
    upsert2cols(table, new Object[]{"banana", "b", 1L});
    upsert2cols(table, new Object[]{"cherry", "c", 1L});
    upsert2cols(table, new Object[]{"yak", "y", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[][] expected = {{"apple", "z"}, {"applee", "z"}, {"zebra", "z"}};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i][0]);
      Assert.assertEquals(r.getValues()[1], expected[i][1]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testMultiValueGroupKeyOrderFlipped() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d2, d1 ORDER BY d1, d2 DESC LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"z", "zebra", 1L});
    upsert2cols(table, new Object[]{"z", "apple", 1L});
    upsert2cols(table, new Object[]{"a", "apple", 1L});
    upsert2cols(table, new Object[]{"z", "applee", 1L});
    upsert2cols(table, new Object[]{"b", "banana", 1L});
    upsert2cols(table, new Object[]{"c", "cherry", 1L});
    upsert2cols(table, new Object[]{"y", "yak", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[][] expected = {{"z", "apple"}, {"a", "apple"}, {"z", "applee"}};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i][0]);
      Assert.assertEquals(r.getValues()[1], expected[i][1]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testMultiValueAscDescNullEnabled() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d1, d2 DESC LIMIT 3");
    queryContext.setNullHandlingEnabled(true);
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"zebra", "z", 1L});
    upsert2cols(table, new Object[]{"apple", null, 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"applee", "z", 1L});
    upsert2cols(table, new Object[]{"banana", "b", 1L});
    upsert2cols(table, new Object[]{"cherry", "c", 1L});
    upsert2cols(table, new Object[]{"yak", "y", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[][] expected = {{"apple", null}, {"apple", "a"}, {"applee", "z"}};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i][0]);
      Assert.assertEquals(r.getValues()[1], expected[i][1]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  @Test
  public void testMultiValueLimit0() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d1, d2 DESC LIMIT 0");
    queryContext.setNullHandlingEnabled(true);
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"zebra", "z", 1L});
    upsert2cols(table, new Object[]{"apple", null, 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"applee", "z", 1L});
    upsert2cols(table, new Object[]{"banana", "b", 1L});
    upsert2cols(table, new Object[]{"cherry", "c", 1L});
    upsert2cols(table, new Object[]{"yak", "y", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testMultiValueAscDescMerge() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1, d2 ORDER BY d1, d2 DESC LIMIT 3");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    Comparator<Key> comparator = OrderByComparatorFactory.getGroupKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    SimpleSortedIndexedTable table =
        new SimpleSortedIndexedTable(dataSchema, false, queryContext, 3, executor, comparator);

    // Insert out-of-order keys
    upsert2cols(table, new Object[]{"zebra", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "z", 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"applee", "z", 1L});
    upsert2cols(table, new Object[]{"banana", "b", 1L});
    upsert2cols(table, new Object[]{"cherry", "c", 1L});
    upsert2cols(table, new Object[]{"apple", "a", 1L});
    upsert2cols(table, new Object[]{"yak", "y", 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[][] expected = {{"apple", "z"}, {"apple", "a"}, {"applee", "z"}};
    long[] expectedCount = {1, 2, 1};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i][0]);
      Assert.assertEquals(r.getValues()[1], expected[i][1]);
      Assert.assertEquals(r.getValues()[2], expectedCount[i]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }

  private void upsert(SimpleSortedIndexedTable table, Object[] row) {
    upsert(table, row, 1);
  }

  private void upsert2cols(SimpleSortedIndexedTable table, Object[] row) {
    upsert(table, row, 2);
  }

  private void upsert(SimpleSortedIndexedTable table, Object[] row, int numKeyCols) {
    Object[] key = new Object[numKeyCols];
    for (int i = 0; i < numKeyCols; i++) {
      key[i] = row[i];
    }
    table.upsert(new Key(key), new Record(row));
  }
}
