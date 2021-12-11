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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DataTableUtilsTest {

  @Test
  public void testBuildEmptyDataTable()
      throws IOException {
    // Selection
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable WHERE foo = 'bar'");
    DataTable dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
    DataSchema dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"*"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING});
    assertEquals(dataTable.getNumberOfRows(), 0);

    // Aggregation
    queryContext = QueryContextConverterUtils
        .getQueryContextFromSQL("SELECT COUNT(*), SUM(a), MAX(b) FROM testTable WHERE foo = 'bar'");
    dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"count_star", "sum_a", "max_b"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    assertEquals(dataTable.getNumberOfRows(), 1);
    assertEquals(dataTable.getLong(0, 0), 0L);
    assertEquals(dataTable.getDouble(0, 1), 0.0);
    assertEquals(dataTable.getDouble(0, 2), Double.NEGATIVE_INFINITY);

    // PQL group-by
    queryContext = QueryContextConverterUtils
        .getQueryContextFromPQL("SELECT COUNT(*), SUM(a), MAX(b) FROM testTable WHERE foo = 'bar' GROUP BY c, d");
    dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"functionName", "GroupByResultMap"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.OBJECT});
    assertEquals(dataTable.getNumberOfRows(), 3);
    assertEquals(dataTable.getString(0, 0), "count_star");
    assertEquals(dataTable.getObject(0, 1), Collections.emptyMap());
    assertEquals(dataTable.getString(1, 0), "sum_a");
    assertEquals(dataTable.getObject(1, 1), Collections.emptyMap());
    assertEquals(dataTable.getString(2, 0), "max_b");
    assertEquals(dataTable.getObject(2, 1), Collections.emptyMap());

    // SQL group-by
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT c, d, COUNT(*), SUM(a), MAX(b) FROM testTable WHERE foo = 'bar' GROUP BY c, d OPTION(groupByMode=sql)");
    dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"c", "d", "count(*)", "sum(a)", "max(b)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(dataTable.getNumberOfRows(), 0);

    // Distinct
    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT DISTINCT a, b FROM testTable WHERE foo = 'bar'");
    dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
    dataSchema = dataTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"distinct_a:b"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.OBJECT});
    assertEquals(dataTable.getNumberOfRows(), 1);
    Object firstObject = dataTable.getObject(0, 0);
    assertTrue(firstObject instanceof DistinctTable);
    DistinctTable distinctTable = (DistinctTable) firstObject;
    assertEquals(distinctTable.size(), 0);
    assertEquals(distinctTable.getDataSchema().getColumnNames(), new String[]{"a", "b"});
    assertEquals(distinctTable.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
  }
}
