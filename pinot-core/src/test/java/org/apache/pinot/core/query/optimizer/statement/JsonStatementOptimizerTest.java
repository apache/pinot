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

import java.util.Arrays;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
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
  private static final TableConfig TABLE_CONFIG_WITH_INDEX =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
          .setJsonIndexColumns(Arrays.asList("jsonColumn")).build();
  private static final TableConfig TABLE_CONFIG_WITHOUT_INDEX = null;

  /** Test that a json path expression in SELECT list is properly converted to a JSON_EXTRACT_SCALAR function within
   * an AS function. */
  @Test
  public void testJsonSelect() {
    // SELECT using json column.
    TestHelper.assertEqualsQuery("SELECT jsonColumn FROM testTable", "SELECT jsonColumn FROM testTable",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // SELECT using a simple json path expression.
    TestHelper.assertEqualsQuery("SELECT jsonColumn.x FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.x', 'STRING', 'null') AS \"jsonColumn.x\" FROM testTable",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // SELECT using json path expressions with array addressing.
    TestHelper.assertEqualsQuery("SELECT jsonColumn.data[0][1].a.b[0] FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.data[0][1].a.b[0]', 'STRING', 'null') AS \"jsonColumn.data[0][1].a"
            + ".b[0]\" FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // SELECT using json path expressions within double quotes.
    TestHelper.assertEqualsQuery("SELECT \"jsonColumn.a.b.c[0][1][2][3].d.e.f[0].g\" FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.a.b.c[0][1][2][3].d.e.f[0].g', 'STRING', 'null') AS \"jsonColumn.a"
            + ".b.c[0][1][2][3].d.e.f[0].g\" FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);
  }

  /** Test that a predicate comparing a json path expression with literal is properly converted into a JSON_MATCH
   * function. */
  @Test
  public void testJsonFilter() {
    // Comparing json path expression with a string value.
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy'",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.name.first\" = ''daffy''')", TABLE_CONFIG_WITH_INDEX,
        SCHEMA);

    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy'",
        "SELECT * FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.name.first', 'STRING', 'null') = 'daffy'",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // Comparing json path expression with a  numerical value.
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101')", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'LONG', -9223372036854775808) = 101",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // Comparing json path expression with a  numerical value and checking for null value.
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id IS NOT NULL AND jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" IS NOT NULL') AND JSON_MATCH(jsonColumn, '\"$"
            + ".id\" = 101')", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn.id IS NOT NULL AND jsonColumn.id = 101",
        "SELECT * FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'JSON', 'null') IS NOT NULL AND "
            + "JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'LONG', -9223372036854775808) = 101", TABLE_CONFIG_WITHOUT_INDEX,
        SCHEMA);
  }

  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupBy() {
    TestHelper.assertEqualsQuery("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null') AS \"jsonColumn.id\", count(*) FROM "
            + "testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null')", TABLE_CONFIG_WITH_INDEX,
        SCHEMA);

    TestHelper.assertEqualsQuery("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null') AS \"jsonColumn.id\", count(*) FROM "
            + "testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null')",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);
  }

  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupByHaving() {
    TestHelper.assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name"
            + ".last = 'mouse'",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count"
            + "(*) FROM testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') HAVING "
            + "JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') = 'mouse'", TABLE_CONFIG_WITH_INDEX,
        SCHEMA);

    TestHelper.assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name"
            + ".last = 'mouse'",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count"
            + "(*) FROM testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') HAVING "
            + "JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') = 'mouse'", TABLE_CONFIG_WITHOUT_INDEX,
        SCHEMA);
  }

  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test
  public void testJsonSelectFilterGroupBy() {
    TestHelper.assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count"
            + "(*) FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101') GROUP BY JSON_EXTRACT_SCALAR"
            + "(jsonColumn, '$.name.last', 'STRING', 'null')", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null') AS \"jsonColumn.name.last\", count"
            + "(*) FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'LONG', -9223372036854775808) = 101 "
            + "GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.name.last', 'STRING', 'null')", TABLE_CONFIG_WITHOUT_INDEX,
        SCHEMA);
  }

  /** Test an aggregation function over json path expression in SELECT clause. */
  @Test
  public void testTransformFunctionOverJsonPathSelectExpression() {
    // Apply string transform function on json path expression.
    TestHelper.assertEqualsQuery("SELECT UPPER(jsonColumn.name.first) FROM testTable",
        "SELECT UPPER(JSON_EXTRACT_SCALAR(jsonColumn, '$.name.first', 'STRING', 'null')) AS \"upper(jsonColumn.name"
            + ".first)\" FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Apply date transform function on json path expression and check for IS NULL
    TestHelper.assertEqualsQuery("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NULL",
        "SELECT FROMEPOCHDAYS(JSON_EXTRACT_SCALAR(jsonColumn, '$.days', 'LONG', -9223372036854775808)) AS "
            + "\"fromepochdays(jsonColumn.days)\" FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.days\" IS NULL')",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Apply date transform function on json path expression and check for IS NOT NULL
    TestHelper
        .assertEqualsQuery("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NOT NULL",
            "SELECT FROMEPOCHDAYS(JSON_EXTRACT_SCALAR(jsonColumn, '$.days', 'LONG', -9223372036854775808)) AS "
                + "\"fromepochdays(jsonColumn.days)\" FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.days\" IS NOT "
                + "NULL')", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper
        .assertEqualsQuery("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NOT NULL",
            "SELECT FROMEPOCHDAYS(JSON_EXTRACT_SCALAR(jsonColumn, '$.days', 'LONG', -9223372036854775808)) AS "
                + "\"fromepochdays(jsonColumn.days)\" FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.days', "
                + "'JSON', 'null') IS NOT NULL", TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);
  }

  /** Test a numerical function over json path expression in SELECT clause. */
  @Test
  public void testNumericalFunctionOverJsonPathSelectExpression() {

    // Test without user-specified alias.
    TestHelper.assertEqualsQuery("SELECT MAX(jsonColumn.id) FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY
            + "')) AS \"max(jsonColumn.id)\" FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Test with user-specified alias.
    TestHelper.assertEqualsQuery("SELECT MAX(jsonColumn.id) AS x FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY
            + "')) AS x FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Test with nested function calls (minus function being used within max function).
    TestHelper.assertEqualsQuery("SELECT MAX(jsonColumn.id - 5) FROM testTable",
        "SELECT MAX(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '" + Double.NEGATIVE_INFINITY
            + "') - 5) AS \"max(minus(jsonColumn.id,'5'))\" FROM testTable", TABLE_CONFIG_WITH_INDEX, SCHEMA);
  }

  @Test
  public void testTopLevelArrayPathExpressions() {
    // SELECT using json path expression with top-level array addressing.
    TestHelper.assertEqualsQuery("SELECT jsonColumn[0] FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.[0]', 'STRING', 'null') AS \"jsonColumn[0]\" FROM testTable",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery("SELECT jsonColumn[0].a FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.[0].a', 'STRING', 'null') AS \"jsonColumn[0].a\" FROM testTable",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery("SELECT jsonColumn.a[0] FROM testTable",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.a[0]', 'STRING', 'null') AS \"jsonColumn.a[0]\" FROM testTable",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    TestHelper.assertEqualsQuery("SELECT jsonColumn[1].i2 FROM testTable WHERE jsonColumn[1].i2 IS NOT NULL",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.[1].i2', 'STRING', 'null') AS \"jsonColumn[1].i2\" FROM testTable "
            + "WHERE JSON_MATCH(jsonColumn, '\"$.[1].i2\" IS NOT NULL')",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Use top-level array addressing in json path expression in JSON_EXTRACT_SCALAR filter.
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn[2] IS NOT NULL and jsonColumn[2] = 'test'",
        "SELECT * FROM testTable WHERE JSON_EXTRACT_SCALAR(jsonColumn, '$.[2]', 'JSON', 'null') IS NOT NULL AND "
            + "JSON_EXTRACT_SCALAR(jsonColumn, '$.[2]', 'STRING', 'null') = 'test'",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // Use top-level array addressing in json path expression in JSON_MATCH filter
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE jsonColumn[2] IS NOT NULL and jsonColumn[2] = 'test'",
        "SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.[2]\" IS NOT NULL') AND JSON_MATCH(jsonColumn, "
            + "'\"$.[2]\" = ''test''')",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);

    // Use top-level array addressing in json path expression in GROUP BY clause.
    TestHelper.assertEqualsQuery("SELECT jsonColumn[0], count(*) FROM testTable GROUP BY jsonColumn[0]",
        "SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.[0]', 'STRING', 'null') AS \"jsonColumn[0]\", count(*) FROM "
            + "testTable GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.[0]', 'STRING', 'null')",
        TABLE_CONFIG_WITH_INDEX, SCHEMA);
  }
}
