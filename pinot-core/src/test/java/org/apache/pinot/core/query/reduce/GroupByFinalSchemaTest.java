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

import java.util.LinkedHashMap;
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
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


/// Final grouped result types must not depend on which server response supplied the broker schema.
public class GroupByFinalSchemaTest {
  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
        {"SELECT k, AVG(v) AS mean FROM test GROUP BY k ORDER BY mean", 90.0},
        {"SELECT k, AVG(v) + 1 AS mean FROM test GROUP BY k HAVING AVG(v) > 80 ORDER BY mean", 91.0}
    };
  }

  @Test(dataProvider = "queries")
  public void testSchemaIsIndependentOfServerIterationOrder(String sql, double expected) throws Exception {
    for (boolean reverse : List.of(false, true)) {
      try (QueryThreadContext ignored = QueryThreadContext.openForSseTest();
          ExecutorService executor = Executors.newSingleThreadExecutor()) {
        QueryContext query = QueryContextConverterUtils.getQueryContext(sql);
        DataTable first = table(query, new AvgPair(0, 1));
        DataTable second = table(query, new AvgPair(900, 9));
        Map<ServerRoutingInstance, DataTable> tables = new LinkedHashMap<>();
        tables.put(mock(ServerRoutingInstance.class), reverse ? second : first);
        tables.put(mock(ServerRoutingInstance.class), reverse ? first : second);
        BrokerResponseNative response = new BrokerResponseNative();
        new GroupByDataTableReducer(query).reduceAndSetResults("test", first.getDataSchema(), tables, response,
            new DataTableReducerContext(executor, 1, 10000, 0, 0, 16), null);
        assertEquals(response.getResultTable().getRows().size(), 1);
        assertEquals(response.getResultTable().getRows().getFirst(), new Object[]{1, expected});
        assertEquals(response.getResultTable().getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});
        String resultName = query.getSelectExpressions().get(1).toString();
        assertEquals(response.getResultTable().getDataSchema().getColumnNames(), new String[]{"k", resultName});
      }
    }
  }

  private static DataTable table(QueryContext query, AvgPair value) throws Exception {
    var function = query.getAggregationFunctions()[0];
    DataSchema schema = new DataSchema(new String[]{"k", function.getResultColumnName()},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.OBJECT});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    builder.startRow();
    builder.setColumn(0, 1);
    builder.setColumn(1, function.serializeIntermediateResult(value));
    builder.finishRow();
    return DataTableFactory.getDataTable(builder.build().toBytes());
  }
}
