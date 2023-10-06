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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PostAggregationHandlerTest {

  @Test
  public void testPostAggregation() {
    // Regular aggregation only
    {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT SUM(m1) FROM testTable");
      DataSchema dataSchema = new DataSchema(new String[]{"sum(m1)"}, new ColumnDataType[]{ColumnDataType.DOUBLE});
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      assertEquals(handler.getResultDataSchema(), dataSchema);
      assertEquals(handler.getResult(new Object[]{1.0}), new Object[]{1.0});
      assertEquals(handler.getResult(new Object[]{2.0}), new Object[]{2.0});
    }

    // Regular aggregation group-by
    {
      QueryContext queryContext =
          QueryContextConverterUtils.getQueryContext("SELECT d1, SUM(m1) FROM testTable GROUP BY d1");
      DataSchema dataSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      assertEquals(handler.getResultDataSchema(), dataSchema);
      assertEquals(handler.getResult(new Object[]{1, 2.0}), new Object[]{1, 2.0});
      assertEquals(handler.getResult(new Object[]{3, 4.0}), new Object[]{3, 4.0});
    }

    // Aggregation group-by with partial columns selected
    {
      QueryContext queryContext =
          QueryContextConverterUtils.getQueryContext("SELECT SUM(m1), d2 FROM testTable GROUP BY d1, d2");
      DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE});
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      DataSchema resultDataSchema = handler.getResultDataSchema();
      assertEquals(resultDataSchema.size(), 2);
      assertEquals(resultDataSchema.getColumnNames(), new String[]{"sum(m1)", "d2"});
      assertEquals(resultDataSchema.getColumnDataTypes(),
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG});
      assertEquals(handler.getResult(new Object[]{1, 2L, 3.0}), new Object[]{3.0, 2L});
      assertEquals(handler.getResult(new Object[]{4, 5L, 6.0}), new Object[]{6.0, 5L});
    }

    // Aggregation group-by with order-by
    {
      QueryContext queryContext = QueryContextConverterUtils
          .getQueryContext("SELECT SUM(m1), d2 FROM testTable GROUP BY d1, d2 ORDER BY MAX(m1)");
      DataSchema dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m1)"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      DataSchema resultDataSchema = handler.getResultDataSchema();
      assertEquals(resultDataSchema.size(), 2);
      assertEquals(resultDataSchema.getColumnNames(), new String[]{"sum(m1)", "d2"});
      assertEquals(resultDataSchema.getColumnDataTypes(),
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG});
      assertEquals(handler.getResult(new Object[]{1, 2L, 3.0, 4.0}), new Object[]{3.0, 2L});
      assertEquals(handler.getResult(new Object[]{5, 6L, 7.0, 8.0}), new Object[]{7.0, 6L});
    }

    // Post aggregation
    {
      QueryContext queryContext =
          QueryContextConverterUtils.getQueryContext("SELECT SUM(m1) + MAX(m2) FROM testTable");
      DataSchema dataSchema = new DataSchema(new String[]{"sum(m1)", "max(m2)"},
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      DataSchema resultDataSchema = handler.getResultDataSchema();
      assertEquals(resultDataSchema.size(), 1);
      assertEquals(resultDataSchema.getColumnName(0), "plus(sum(m1),max(m2))");
      assertEquals(resultDataSchema.getColumnDataType(0), ColumnDataType.DOUBLE);
      assertEquals(handler.getResult(new Object[]{1.0, 2.0}), new Object[]{3.0});
      assertEquals(handler.getResult(new Object[]{3.0, 4.0}), new Object[]{7.0});
    }

    // Post aggregation with group-by and order-by
    {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT (SUM(m1) + MAX(m2) - d1) / 2, d2 FROM testTable GROUP BY d1, d2 ORDER BY MAX(m1)");
      DataSchema dataSchema =
          new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)", "max(m1)"}, new ColumnDataType[]{
              ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
              ColumnDataType.DOUBLE
          });
      PostAggregationHandler handler = new PostAggregationHandler(queryContext, dataSchema);
      DataSchema resultDataSchema = handler.getResultDataSchema();
      assertEquals(resultDataSchema.size(), 2);
      assertEquals(resultDataSchema.getColumnNames(),
          new String[]{"divide(minus(plus(sum(m1),max(m2)),d1),2)", "d2"});
      assertEquals(resultDataSchema.getColumnDataTypes(),
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG});
      assertEquals(handler.getResult(new Object[]{1, 2L, 3.0, 4.0, 5.0}), new Object[]{3.0, 2L});
      assertEquals(handler.getResult(new Object[]{6, 7L, 8.0, 9.0, 10.0}), new Object[]{5.5, 7L});
    }
  }
}
