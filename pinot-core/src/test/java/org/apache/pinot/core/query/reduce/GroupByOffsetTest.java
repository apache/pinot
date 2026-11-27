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
package org.apache.pinot.core.query.reduce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests pagination of single-stage GROUP BY results from both intermediate and final server responses.
public class GroupByOffsetTest {
  @DataProvider
  public Object[][] pages() {
    return new Object[][]{
        {"ORDER BY c DESC LIMIT 2 OFFSET 1", new Object[][]{{2, 3L}, {3, 2L}}},
        {"HAVING COUNT(*) < 4 ORDER BY c DESC LIMIT 1 OFFSET 1", new Object[][]{{3, 2L}}},
        {"ORDER BY c DESC LIMIT 2 OFFSET 4", new Object[0][]},
        {"ORDER BY c DESC LIMIT 0 OFFSET 1", new Object[0][]}
    };
  }

  @Test(dataProvider = "pages")
  public void testGroupByOffset(String page, Object[][] expected)
      throws Exception {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest();
        ExecutorService executor = Executors.newSingleThreadExecutor()) {
      QueryContext query = QueryContextConverterUtils.getQueryContext(
          "SELECT k, COUNT(*) AS c FROM test GROUP BY k " + page);
      DataSchema schema = new DataSchema(new String[]{"k", query.getAggregationFunctions()[0].getResultColumnName()},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
      ServerRoutingInstance firstServer = new ServerRoutingInstance("localhost", 1, TableType.OFFLINE);
      ServerRoutingInstance secondServer = new ServerRoutingInstance("localhost", 2, TableType.OFFLINE);
      Map<ServerRoutingInstance, DataTable> intermediateResults =
          Map.of(firstServer, table(schema, new int[][]{{3, 1}, {1, 1}, {2, 1}, {4, 1}}),
              secondServer, table(schema, new int[][]{{1, 3}, {2, 2}, {3, 1}}));
      Map<ServerRoutingInstance, DataTable> finalResults =
          Map.of(firstServer, table(schema, new int[][]{{1, 4}, {2, 3}, {3, 2}, {4, 1}}));

      assertPage(query, schema, intermediateResults, executor, expected);
      query.setServerReturnFinalResult(true);
      assertPage(query, schema, finalResults, executor, expected);
    }
  }

  private static void assertPage(QueryContext query, DataSchema schema,
      Map<ServerRoutingInstance, DataTable> results, ExecutorService executor, Object[][] expected) {
    BrokerResponseNative response = new BrokerResponseNative();
    new GroupByDataTableReducer(query).reduceAndSetResults("test", schema, results, response,
        new DataTableReducerContext(executor, 1, 10000, 0, 0, 16), null);
    List<Object[]> rows = response.getResultTable().getRows();
    assertEquals(rows.size(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(rows.get(i), expected[i]);
    }
  }

  private static DataTable table(DataSchema schema, int[][] rows)
      throws Exception {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    for (int[] row : rows) {
      builder.startRow();
      builder.setColumn(0, row[0]);
      builder.setColumn(1, (long) row[1]);
      builder.finishRow();
    }
    return DataTableFactory.getDataTable(builder.build().toBytes());
  }
}
