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


public class ReducerDataSchemaUtilsTest {

  @Test
  public void testCanonicalizeDataSchemaForAggregation() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT SUM(col1 + col2) FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    DataSchema dataSchema = new DataSchema(new String[]{"sum(col1+col2)"}, new ColumnDataType[]{ColumnDataType.DOUBLE});
    DataSchema canonicalDataSchema =
        ReducerDataSchemaUtils.canonicalizeDataSchemaForAggregation(queryContext, dataSchema);
    assertEquals(canonicalDataSchema,
        new DataSchema(new String[]{"sum(plus(col1,col2))"}, new ColumnDataType[]{ColumnDataType.DOUBLE}));

    queryContext = QueryContextConverterUtils.getQueryContext("SELECT SUM(col1 + 1), MIN(col2 + 2) FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"sum(col1+1)", "min(col2+2)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForAggregation(queryContext, dataSchema);
    assertEquals(canonicalDataSchema, new DataSchema(new String[]{"sum(plus(col1,'1'))", "min(plus(col2,'2'))"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE}));

    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT MAX(col1 + 1) FILTER(WHERE col3 > 0) - MIN(col2 + 2) FILTER(WHERE col4 > 0) FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"max(col1+1)", "min(col2+2)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForAggregation(queryContext, dataSchema);
    assertEquals(canonicalDataSchema, new DataSchema(
        new String[]{"max(plus(col1,'1')) FILTER(WHERE col3 > '0')", "min(plus(col2,'2')) FILTER(WHERE col4 > '0')"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE}));
  }

  @Test
  public void testCanonicalizeDataSchemaForGroupBy() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(col1 + col2) FROM testTable GROUP BY col3 + col4 ORDER BY col3 + col4");
    // Intentionally make data schema not matching the string representation of the expression
    DataSchema dataSchema = new DataSchema(new String[]{"add(col3+col4)", "sum(col1+col2)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    DataSchema canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForGroupBy(queryContext, dataSchema);
    assertEquals(canonicalDataSchema, new DataSchema(new String[]{"plus(col3,col4)", "sum(plus(col1,col2))"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE}));

    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(col1 + 1), MIN(col2 + 2), col4 FROM testTable GROUP BY col3, col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"col3", "col4", "sum(col1+1)", "min(col2+2)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForGroupBy(queryContext, dataSchema);
    assertEquals(canonicalDataSchema,
        new DataSchema(new String[]{"col3", "col4", "sum(plus(col1,'1'))", "min(plus(col2,'2'))"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        }));

    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col3 + col4, MAX(col1 + 1) FILTER(WHERE col3 > 0) - MIN(col2 + 2) FILTER(WHERE col4 > 0) FROM "
            + "testTable GROUP BY col3 + col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col3+col4)", "max(col1+1)", "min(col2+2)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForGroupBy(queryContext, dataSchema);
    assertEquals(canonicalDataSchema, new DataSchema(new String[]{
        "plus(col3,col4)", "max(plus(col1,'1')) FILTER(WHERE col3 > '0')",
        "min(plus(col2,'2')) FILTER" + "(WHERE col4 > '0')"
    }, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE}));
  }

  @Test
  public void testCanonicalizeDataSchemaForDistinct() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT DISTINCT col1, col2 + col3 FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    DataSchema dataSchema = new DataSchema(new String[]{"col1", "add(col2+col3)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});
    DataSchema canonicalDataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForDistinct(queryContext, dataSchema);
    assertEquals(canonicalDataSchema, new DataSchema(new String[]{"col1", "plus(col2,col3)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE}));
  }
}
