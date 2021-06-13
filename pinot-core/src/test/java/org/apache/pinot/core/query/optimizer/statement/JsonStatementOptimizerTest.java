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
package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases to verify that {@link JsonStatementOptimizer} is properly rewriting queries that use JSON path expressions
 * into equivalent queries that use JSON_MATCH and JSON_EXTRACT_SCALAR functions.
 */
public class JsonStatementOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn", FieldSpec.DataType.INT)
          .addSingleValueDimension("longColumn", FieldSpec.DataType.LONG)
          .addSingleValueDimension("stringColumn", FieldSpec.DataType.STRING)
          .addSingleValueDimension("jsonColumn", FieldSpec.DataType.JSON).build();

  /** Test that a json path expression in SELECT list is properly converted to a JSON_EXTRACT_SCALAR function within an AS function. */
  @Test
  public void testJsonSelect() {
    // SELECT using a simple json path expression.
    assertEqualsQuery("SELECT jsonColumn.x FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.x', 'STRING', 'null') AS \"jsonColumn.x\" FROM testTable");

    // SELECT using json path expressions with array addressing.
    assertEqualsQuery("SELECT jsonColumn.data[0][1].a.b[0] FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.data[0][1].a.b[0]', 'STRING', 'null') AS \"jsonColumn.data[0][1].a.b[0]\" FROM testTable");

    // SELECT using json path expressions within double quotes.
    assertEqualsQuery("SELECT \"jsonColumn.a.b.c[0][1][2][3].d.e.f[0].g\" FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.a.b.c[0][1][2][3].d.e.f[0].g', 'STRING', 'null') AS \"jsonColumn.a.b.c[0][1][2][3].d.e.f[0].g\" FROM testTable");
  }

  /** Test that a predicate comparing a json path expression with literal is properly converted into a JSON_MATCH function. */
  @Test
  public void testJsonFilter() {
    // Comparing json path expression with a string value.
    assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy'",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.name.first\" = ''daffy''')");

    // Comparing json path expression with a  numerical value.
    assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101')");

    // Comparing json path expression with a  numerical value and checking for null value.
    assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id IS NOT NULL AND jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" IS NOT NULL') AND JSON_MATCH(jsonColumn, '\"$.id\" = 101')");
  }

  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupBy() {
    assertEqualsQuery("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null') AS \"jsonColumn.id\", count(*) FROM testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null')");
  }

  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupByHaving() {
    assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name.last = 'mouse'",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count(*) FROM testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') HAVING JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') = 'mouse'");
  }

  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test
  public void testJsonSelectFilterGroupBy() {
    assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count(*) FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101') GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null')");
  }

  /** Test an aggregation function over json path expression in SELECT clause. */
  @Test
  public void testTransformFunctionOverJsonPathSelectExpression() {
    // Apply string transform function on json path expression.
    assertEqualsQuery("SELECT UPPER(jsonColumn.name.first) FROM testTable",
        "SELECT UPPER(JSON_EXTRACT_SCALAR(jsonColumn, '$.name.first', 'STRING', 'null')) AS \"upper(jsonColumn.name.first)\" FROM testTable");

    // Apply date transform function on json path expression and check for IS NULL
    assertEqualsQuery("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NULL",
        "SELECT FROMEPOCHDAYS(JSON_EXTRACT_SCALAR(jsonColumn, '$.days', 'LONG', " + Long.MIN_VALUE + ")) AS \"fromepochdays(jsonColumn.days)\" FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.days\" IS NULL')");

    // Apply date transform function on json path expression and check for IS NOT NULL
    assertEqualsQuery("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NOT NULL",
        "SELECT FROMEPOCHDAYS(JSON_EXTRACT_SCALAR(jsonColumn, '$.days', 'LONG', " + Long.MIN_VALUE + ")) AS \"fromepochdays(jsonColumn.days)\" FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.days\" IS NOT NULL')");
  }

  /** Test a numerical function over json path expression in SELECT clause. */
  @Test
  public void testNumericalFunctionOverJsonPathSelectExpression() {

    // Test without user-specified alias.
    assertEqualsQuery("SELECT MAX(jsonColumn.id) FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY + "')) AS \"max(jsonColumn.id)\" FROM testTable");

    // Test with user-specified alias.
    assertEqualsQuery("SELECT MAX(jsonColumn.id) AS x FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY + "')) AS x FROM testTable");

    // Test with nested function calls (minus function being used within max function).
    assertEqualsQuery("SELECT MAX(jsonColumn.id - 5) FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY + "') - 5) AS \"max(minus(jsonColumn.id,'5'))\" FROM testTable");
  }

  /**
   * Given two queries, this function will validate that the query obtained after rewriting the first query is the
   * same as the second query.
   */
  private static void assertEqualsQuery(String queryOriginal, String queryAfterRewrite) {
    BrokerRequest userBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryOriginal);
    PinotQuery userQuery = userBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(userQuery, SCHEMA);

    BrokerRequest rewrittenBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryAfterRewrite);
    PinotQuery rewrittenQuery = rewrittenBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(rewrittenQuery, SCHEMA);

    // Currently there is no way to specify Double.NEGATIVE_INFINITY in SQL, so in the test cases we specify string '-Infinity' as
    // default null value, but change "stringValue:-Infinity" to "doubleValue:-Infinity" to adjust for internal rewrite.
    Assert.assertEquals(userQuery.toString(),
        rewrittenQuery.toString().replace("stringValue:-Infinity", "doubleValue:-Infinity"));
  }
}
