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

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.AggregationFunctionBinder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.ExprMinMaxRewriter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class EmptyResponseUtilsTest {

  @Test
  public void testBoundAggregationSchemaWithoutServers() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("name", DataType.STRING)
        .addSingleValueDimension("ts", DataType.TIMESTAMP).addSingleValueDimension("flag", DataType.BOOLEAN)
        .addSingleValueDimension("id", DataType.LONG).build();
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SET enableNullHandling=true; "
        + "SELECT mode(ts) AS eventTime, firstWithTime(name,ts) AS firstName, "
        + "lastWithTime(flag,ts) AS lastFlag, anyValue(flag) AS anyFlag, anyValue(ts) AS anyTime, "
        + "anyValue(id) AS anyId, arrayAgg(ts) AS eventTimes, arrayAgg(flag) AS flags "
        + "FROM testTable WHERE 1=0");
    AggregationFunctionBinder.bind(query, schema);
    ResultTable result = EmptyResponseUtils.buildEmptyResultTable(QueryContextConverterUtils.getQueryContext(query));
    assertEquals(result.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.TIMESTAMP, ColumnDataType.STRING, ColumnDataType.BOOLEAN,
            ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.LONG,
            ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY});
    assertEquals(result.getDataSchema().getColumnNames(),
        new String[]{"eventTime", "firstName", "lastFlag", "anyFlag", "anyTime", "anyId", "eventTimes", "flags"});
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), new Object[]{null, null, null, null, null, null,
        new String[0], new boolean[0]});

    PinotQuery grouped = CalciteSqlParser.compileToPinotQuery(
        "SELECT mode(ts), firstWithTime(name,ts), anyValue(ts), anyValue(flag), anyValue(id), "
            + "arrayAgg(ts), arrayAgg(flag) FROM testTable WHERE 1=0 GROUP BY flag");
    AggregationFunctionBinder.bind(grouped, schema);
    ResultTable groupedResult =
        EmptyResponseUtils.buildEmptyResultTable(QueryContextConverterUtils.getQueryContext(grouped));
    assertTrue(groupedResult.getRows().isEmpty());
    assertEquals(groupedResult.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.TIMESTAMP, ColumnDataType.STRING, ColumnDataType.TIMESTAMP,
            ColumnDataType.BOOLEAN, ColumnDataType.LONG, ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY});
  }

  @Test
  public void testBoundExprMinMaxSchemaWithoutServers() {
    String previousRewriters = ResultRewriterFactory.getResultRewriter().stream()
        .map(rewriter -> rewriter.getClass().getName()).collect(Collectors.joining(","));
    ResultRewriterFactory.init(ParentAggregationResultRewriter.class.getName());
    try {
      Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("id", DataType.LONG)
          .addSingleValueDimension("ts", DataType.TIMESTAMP).addSingleValueDimension("flag", DataType.BOOLEAN)
          .addSingleValueDimension("payload", DataType.JSON).build();
      for (String suffix : List.of("", " GROUP BY id", " LIMIT 0")) {
        PinotQuery query = CalciteSqlParser.compileToPinotQuery("SET enableNullHandling=true; "
            + "SELECT exprMin(flag,id), exprMax(ts,id), exprMin(payload,id) FROM testTable WHERE 1=0" + suffix);
        new ExprMinMaxRewriter().rewrite(query);
        AggregationFunctionBinder.bind(query, schema);
        ResultTable result =
            EmptyResponseUtils.buildEmptyResultTable(QueryContextConverterUtils.getQueryContext(query));
        assertEquals(result.getDataSchema().getColumnNames(),
            new String[]{"exprmin(flag,id)", "exprmax(ts,id)", "exprmin(payload,id)"});
        assertEquals(result.getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING});
        if (suffix.isEmpty()) {
          assertEquals(result.getRows().size(), 1);
          assertEquals(result.getRows().get(0), new Object[]{null, null, null});
        } else {
          assertTrue(result.getRows().isEmpty());
        }
      }
    } finally {
      ResultRewriterFactory.init(previousRewriters.isEmpty() ? null : previousRewriters);
    }
  }

  @Test
  public void testEmptyPostAggregationFormattingWithAlias() {
    QueryContext query = QueryContextConverterUtils.getQueryContext(
        "SELECT toTimestamp(COUNT(*)) AS epoch FROM testTable WHERE 1=0");
    ResultTable result = EmptyResponseUtils.buildEmptyResultTable(query);
    assertEquals(result.getDataSchema().getColumnNames(), new String[]{"epoch"});
    assertEquals(result.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.TIMESTAMP});
    assertEquals(result.getRows().get(0), new Object[]{new Timestamp(0).toString()});
  }

  /// An aggregation whose answer over no input is `NULL` has to survive the whole empty-response path.
  ///
  /// This is the path that produced the NPE this contract exists to prevent: the result table is built by calling
  /// `extractFinalResult(extractAggregationResult(createAggregationResultHolder()))` on an untouched holder, and the
  /// value it returns is then passed to [ColumnDataType#convert]. A function that wraps the `null` in a serializer
  /// rather than resolving it fails at one of those two steps, not at extraction.
  @Test
  public void testBuildEmptyResultTableWithNullFinalResults() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT PERCENTILERAWKLL(a, 50), MAXSTRING(b), MINSTRING(b) FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
    });
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertNull(row[0]);
    assertNull(row[1]);
    assertNull(row[2]);
  }

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

  @Test
  public void testBuildEmptyResultTableWithAliases() {
    // Selection
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a AS col_a, b AS col_B, c FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"col_a", "col_B", "c"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertTrue(resultTable.getRows().isEmpty());

    // Distinct
    queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT DISTINCT a, b AS col_B FROM testTable WHERE foo = 'bar'");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"a", "col_B"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertTrue(resultTable.getRows().isEmpty());

    // Aggregation
    queryContext =
        QueryContextConverterUtils.getQueryContext(
            "SELECT COUNT(*) AS num_test, SUM(a) AS Total, MAX(b) AS largest, MIN(b) FROM testTable WHERE foo = 'bar'");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"num_test", "Total", "largest", "min(b)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertEquals(row[0], 0L);
    assertEquals(row[1], 0.0);
    assertEquals(row[2], Double.NEGATIVE_INFINITY);

    // Group-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT c, d AS col_d, COUNT(*) AS num_test, SUM(a) AS Total, MAX(b) AS largest, MIN(b) "
            + "FROM testTable WHERE foo = 'bar' GROUP BY c, d");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"c", "col_d", "num_test", "Total", "largest", "min(b)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG,
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertTrue(resultTable.getRows().isEmpty());
  }

  @Test
  public void testBuildEmptyResultTableWithPostAggregation() {
    // Aggregation with post-aggregation expression and aliases
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(a) * 1.0 / COUNT(*) AS rate, SUM(a) AS total FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"rate", "total"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);

    // Aggregation with post-aggregation expression without alias
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT SUM(a) + MAX(b) FROM testTable WHERE foo = 'bar'");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.size(), 1);
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.DOUBLE});
    rows = resultTable.getRows();
    assertEquals(rows.size(), 1);

    // Group-by with post-aggregation expression
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT c, SUM(a) * 1.0 / COUNT(*) AS rate FROM testTable WHERE foo = 'bar' GROUP BY c");
    resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"c", "rate"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    assertTrue(resultTable.getRows().isEmpty());
  }

  @Test
  public void testBuildEmptyResultTableWithDistinctCountRawHLL() {
    // Test DISTINCTCOUNTRAWHLL aggregation with empty results
    // This should not throw a Jackson serialization error
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCTCOUNTRAWHLL(a) FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();

    // Verify schema is correct
    assertEquals(dataSchema.getColumnNames(), new String[]{"distinctcountrawhll(a)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING});

    // Verify we have one row with a result
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertNotNull(row[0], "Result should not be null");

    // The critical test: verify the result is a String (or can be serialized)
    // This will fail before the fix because row[0] is a SerializedHLL object
    assertTrue(row[0] instanceof String,
        "Result should be a String, but got: " + row[0].getClass().getName());

    // Verify it can be serialized to JSON (this is where the original bug manifests)
    BrokerResponseNative response = new BrokerResponseNative();
    response.setResultTable(resultTable);
    try {
      String jsonString = response.toJsonString();
      assertNotNull(jsonString, "Should be able to serialize to JSON");
    } catch (Exception e) {
      throw new AssertionError("Failed to serialize BrokerResponseNative to JSON: " + e.getMessage(), e);
    }
  }

  @Test
  public void testBuildEmptyResultTableWithDistinctCountRawHLLPlus() {
    // Test DISTINCTCOUNTRAWHLLPLUS aggregation with empty results
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCTCOUNTRAWHLLPLUS(a) FROM testTable WHERE foo = 'bar'");
    ResultTable resultTable = EmptyResponseUtils.buildEmptyResultTable(queryContext);
    DataSchema dataSchema = resultTable.getDataSchema();

    // Verify schema is correct
    assertEquals(dataSchema.getColumnNames(), new String[]{"distinctcountrawhllplus(a)"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING});

    // Verify we have one row with a result
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertNotNull(row[0], "Result should not be null");

    // Verify the result is a String
    assertTrue(row[0] instanceof String,
        "Result should be a String, but got: " + row[0].getClass().getName());

    // Verify it can be serialized to JSON
    BrokerResponseNative response = new BrokerResponseNative();
    response.setResultTable(resultTable);
    try {
      String jsonString = response.toJsonString();
      assertNotNull(jsonString, "Should be able to serialize to JSON");
    } catch (Exception e) {
      throw new AssertionError("Failed to serialize BrokerResponseNative to JSON: " + e.getMessage(), e);
    }
  }
}
