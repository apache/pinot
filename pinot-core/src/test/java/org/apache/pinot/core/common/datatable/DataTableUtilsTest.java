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
package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.util.Collections;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DataTableUtilsTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  @Test
  public void testBuildEmptyDataTable()
      throws IOException {
    // Selection
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest("SELECT * FROM table WHERE foo = 'bar'");
    DataTable dataTable = DataTableUtils.buildEmptyDataTable(brokerRequest);
    DataSchema dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"*"});
    assertEquals(dataSchema.getColumnDataTypes(), new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    assertEquals(dataTable.getNumberOfRows(), 0);

    // Aggregation
    brokerRequest = COMPILER.compileToBrokerRequest("SELECT COUNT(*), SUM(a), MAX(b) FROM table WHERE foo = 'bar'");
    dataTable = DataTableUtils.buildEmptyDataTable(brokerRequest);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"count_star", "sum_a", "max_b"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    assertEquals(dataTable.getNumberOfRows(), 1);
    assertEquals(dataTable.getLong(0, 0), 0L);
    assertEquals(dataTable.getDouble(0, 1), 0.0);
    assertEquals(dataTable.getDouble(0, 2), Double.NEGATIVE_INFINITY);

    // PQL group-by
    brokerRequest =
        COMPILER.compileToBrokerRequest("SELECT COUNT(*), SUM(a), MAX(b) FROM table WHERE foo = 'bar' GROUP BY c, d");
    dataTable = DataTableUtils.buildEmptyDataTable(brokerRequest);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"functionName", "GroupByResultMap"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.OBJECT});
    assertEquals(dataTable.getNumberOfRows(), 3);
    assertEquals(dataTable.getString(0, 0), "count_star");
    assertEquals(dataTable.getObject(0, 1), Collections.emptyMap());
    assertEquals(dataTable.getString(1, 0), "sum_a");
    assertEquals(dataTable.getObject(1, 1), Collections.emptyMap());
    assertEquals(dataTable.getString(2, 0), "max_b");
    assertEquals(dataTable.getObject(2, 1), Collections.emptyMap());

    // SQL group-by
    brokerRequest = COMPILER
        .compileToBrokerRequest("SELECT c, d, COUNT(*), SUM(a), MAX(b) FROM table WHERE foo = 'bar' GROUP BY c, d");
    brokerRequest.setQueryOptions(Collections.singletonMap(Request.QueryOptionKey.GROUP_BY_MODE, Request.SQL));
    dataTable = DataTableUtils.buildEmptyDataTable(brokerRequest);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"c", "d", "count(*)", "sum(a)", "max(b)"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    assertEquals(dataTable.getNumberOfRows(), 0);
  }
}
