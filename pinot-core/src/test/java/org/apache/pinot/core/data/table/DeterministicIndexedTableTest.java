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


import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeterministicIndexedTableTest {

  @Test
  public void testLexicographicEviction() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 LIMIT 3 OPTION(accurateGroupByWithoutOrderBy=true)");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    DeterministicConcurrentIndexedTable table = new DeterministicConcurrentIndexedTable(
        dataSchema, false, queryContext, 3, Integer.MAX_VALUE, Integer.MAX_VALUE, 16, executor);

    // Insert out-of-order keys
    upsert(table, new Object[]{"zebra", 1L});
    upsert(table, new Object[]{"apple", 1L});
    upsert(table, new Object[]{"applee", 1L});
    upsert(table, new Object[]{"banana", 1L});
    upsert(table, new Object[]{"cherry", 1L});
    upsert(table, new Object[]{"yak", 1L});
    upsert(table, new Object[]{null, 1L});

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
  public void testLexicographicEvictionNull() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 LIMIT 3 OPTION(accurateGroupByWithoutOrderBy=true)");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    DeterministicConcurrentIndexedTable table = new DeterministicConcurrentIndexedTable(
        dataSchema, false, queryContext, 3, Integer.MAX_VALUE, Integer.MAX_VALUE, 16, executor);

    // Insert out-of-order keys
    upsert(table, new Object[]{"zebra", 1L});
    upsert(table, new Object[]{null, 1L});

    table.finish(false);

    Iterator<Record> it = table.iterator();
    String[] expected = {"zebra", null};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i]);
      i++;
    }
    Assert.assertEquals(i, 2);
  }

  @Test
  public void testLexicographicEvictionNumbers() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY d1 LIMIT 3 OPTION(accurateGroupByWithoutOrderBy=true)");
    DataSchema dataSchema = new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.LONG
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    DeterministicConcurrentIndexedTable table = new DeterministicConcurrentIndexedTable(
        dataSchema, false, queryContext, 3, Integer.MAX_VALUE, Integer.MAX_VALUE, 16, executor);

    // Insert numeric keys (auto-boxed to Integer)
    upsert(table, new Object[]{1, 1L});
    upsert(table, new Object[]{2, 1L});
    upsert(table, new Object[]{3, 1L});
    upsert(table, new Object[]{4, 1L});  // Should be evicted (largest)

    table.finish(false);

    Iterator<Record> it = table.iterator();
    Object[] expected = {1, 2, 3};
    int i = 0;
    while (it.hasNext()) {
      Record r = it.next();
      Assert.assertEquals(r.getValues()[0], expected[i]);
      i++;
    }
    Assert.assertEquals(i, 3);
  }
  private void upsert(DeterministicConcurrentIndexedTable table, Object[] row) {
    Object[] key = new Object[]{row[0]};
    table.upsert(new Key(key), new Record(row));
  }
}
