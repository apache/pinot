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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class HavingFilterHandlerTest {

  @Test
  public void testHavingFilter() {
    // Simple having
    {
      QueryContext queryContext = QueryContextConverterUtils
          .getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY d1 HAVING COUNT(*) > 5");
      DataSchema dataSchema =
          new DataSchema(new String[]{"d1", "count(*)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
      PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, false);
      assertFalse(havingFilterHandler.isMatch(new Object[]{1, 5L}));
      assertTrue(havingFilterHandler.isMatch(new Object[]{2, 10L}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{3, 3L}));
    }

    // Nested having
    {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT MAX(m1), MIN(m1) FROM testTable GROUP BY d1 HAVING MAX(m1) IN (15, 20, 25) AND (MIN(m1) > 10 OR MIN"
              + "(m1) <= 3)");
      DataSchema dataSchema = new DataSchema(new String[]{"d1", "max(m1)", "min(m1)"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
      PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, false);
      assertFalse(havingFilterHandler.isMatch(new Object[]{1, 15.5, 13.0}));
      assertTrue(havingFilterHandler.isMatch(new Object[]{2, 15.0, 3.0}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{3, 20.0, 7.5}));
    }

    // Having with post-aggregation
    {
      QueryContext queryContext = QueryContextConverterUtils
          .getQueryContext("SELECT MAX(m1), MIN(m2) FROM testTable GROUP BY d1 HAVING MAX(m1) > MIN(m2) * 2");
      DataSchema dataSchema = new DataSchema(new String[]{"d1", "max(m1)", "min(m2)"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
      PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, false);
      assertFalse(havingFilterHandler.isMatch(new Object[]{1, 15.5, 13.0}));
      assertTrue(havingFilterHandler.isMatch(new Object[]{2, 15.0, 3.0}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{3, 20.0, 10.0}));
    }

    // Having with all data types
    {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT COUNT(*) FROM testTable GROUP BY d1, d2, d3, d4, d5, d6 HAVING d1 > 10 AND d2 > 10 AND d3 > 10 AND "
              + "d4 > 10 AND d5 > 10 AND d6 > 10");
      DataSchema dataSchema =
          new DataSchema(new String[]{"d1", "d2", "d3", "d4", "d5", "d6", "count(*)"}, new ColumnDataType[]{
              ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
              ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.LONG
          });
      PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, false);
      assertTrue(havingFilterHandler.isMatch(new Object[]{11, 11L, 10.5f, 10.5, "11", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{10, 11L, 10.5f, 10.5, "11", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{11, 10L, 10.5f, 10.5, "11", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{11, 11L, 10.0f, 10.5, "11", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{11, 11L, 10.5f, 10.0, "11", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{11, 11L, 10.5f, 10.5, "10", new byte[]{17}, 5}));
      assertFalse(havingFilterHandler.isMatch(new Object[]{11, 11L, 10.5f, 10.5, "11", new byte[]{16}, 5}));
    }
  }

  @Test
  public void testIsNullWhenNullHandlingEnabled() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, COUNT(col2) FROM testTable GROUP BY col1 HAVING col1 IS NULL OPTION(enableNullHandling=true)");
    DataSchema dataSchema = new DataSchema(new String[]{"col1", "count(col2)"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT
    });
    PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
    HavingFilterHandler havingFilterHandler =
        new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, true);
    assertTrue(havingFilterHandler.isMatch(new Object[]{null, 1}));
    assertFalse(havingFilterHandler.isMatch(new Object[]{1, 1}));
    assertFalse(havingFilterHandler.isMatch(new Object[]{Integer.MIN_VALUE, 1}));
  }

  @Test
  public void testIsNotNullWhenNullHandlingEnabled() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, COUNT(col2) FROM testTable GROUP BY col1 HAVING col1 IS NOT NULL OPTION"
            + "(enableNullHandling=true)");
    DataSchema dataSchema = new DataSchema(new String[]{"col1", "count(col2)"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT
    });
    PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, dataSchema);
    HavingFilterHandler havingFilterHandler =
        new HavingFilterHandler(queryContext.getHavingFilter(), postAggregationHandler, true);
    assertFalse(havingFilterHandler.isMatch(new Object[]{null, 1}));
    assertTrue(havingFilterHandler.isMatch(new Object[]{1, 1}));
    assertTrue(havingFilterHandler.isMatch(new Object[]{Integer.MIN_VALUE, 1}));
  }
}
