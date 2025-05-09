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
package org.apache.pinot.broker.requesthandler;

import java.util.List;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class EmptyResponseUtilsTest {

  @Test
  public void testBuildEmptyResultTable() {
    // Selection
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, b FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"a", "b"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertTrue(resultTable.getRows().isEmpty());

    // Distinct
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT DISTINCT a, b FROM testTable WHERE foo = 'bar'");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"a", "b"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertTrue(resultTable.getRows().isEmpty());

    // Aggregation
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*), SUM(a), MAX(b) FROM testTable WHERE foo = 'bar'");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"count(*)", "sum(a)", "max(b)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertEquals(row[0], 0L);
    assertEquals(row[1], 0.0);
    assertEquals(row[2], Double.NEGATIVE_INFINITY);

    // Group-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT c, d, COUNT(*), SUM(a), MAX(b) FROM testTable WHERE foo = 'bar' GROUP BY c, d");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"c", "d", "count(*)", "sum(a)", "max(b)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertTrue(resultTable.getRows().isEmpty());
  }
}
