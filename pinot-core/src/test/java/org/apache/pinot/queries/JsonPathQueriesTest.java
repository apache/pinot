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
package org.apache.pinot.queries;

import java.util.List;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/* NOTE: These test cases are inactive since {@link JsonStatementOptimizer} is currently disabled. */
@Test(enabled = false)
public class JsonPathQueriesTest extends BaseJsonQueryTest {

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_WITHOUT_INDEX, FieldSpec.DataType.JSON).build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  GenericRow createRecord(int intValue, long longValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(LONG_COLUMN, longValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);
    record.putValue(JSON_COLUMN_WITHOUT_INDEX, jsonValue);

    return record;
  }

  @Override
  TableConfig tableConfig() {
    return TABLE_CONFIG;
  }

  @Override
  Schema schema() {
    return SCHEMA;
  }

  /** Test that a json path expression in SELECT list is properly converted to a JSON_EXTRACT_SCALAR function within
   * an AS function. */
  @Test(enabled = false)
  public void testJsonSelect() {
    // SELECT using a simple json path expression.
    Object[][] expecteds1 = {{"duck"}, {"mouse"}, {"duck"}};
    checkResult("SELECT jsonColumn.name.last FROM testTable LIMIT 3", expecteds1);

    Object[][] expecteds2 =
        {{"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"1"}};
    checkResult("SELECT jsonColumn.data[0].e[2].z[0].i1 FROM testTable", expecteds2);
  }

  /** Test that a predicate comparing a json path expression with literal is properly converted into a JSON_MATCH
   * function. */
  @Test(enabled = false)
  public void testJsonFilter() {
    // Comparing json path expression with a string value.
    Object[][] expecteds1 =
        {{1, "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}",
            "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}", 1L,
            "daffy duck"}};
    checkResult("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy' LIMIT 1", expecteds1);
    checkResult("SELECT * FROM testTable WHERE jsonColumnWithoutIndex.name.first = 'daffy' LIMIT 1", expecteds1);

    // Comparing json path expression with a numerical value.
    Object[][] expecteds2 =
        {{1, "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}",
            "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}", 1L,
            "daffy duck"}};
    checkResult("SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101') LIMIT 1", expecteds2);
    try {
      checkResult("SELECT * FROM testTable WHERE JSON_MATCH(jsonColumnWithoutIndex, '\"$.id\" = 101') LIMIT 1",
          expecteds2);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert
          .assertEquals(e.getMessage(), "Cannot apply JSON_MATCH on column: jsonColumnWithoutIndex without json index");
    }

    // Comparing json path expression with a string value.
    Object[][] expecteds3 = {{4L}};
    checkResult("SELECT count(*) FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" IS NOT NULL') AND JSON_MATCH"
        + "(jsonColumn, '\"$.id\" = 101')", expecteds3);
  }

  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test(enabled = false)
  public void testJsonGroupBy() {
    Object[][] expecteds1 =
        {{"111", 20L}, {"101", 4L}, {"null", 8L}, {"181", 4L}, {"161.5", 4L}, {"171", 4L}, {"161", 4L}, {"141", 4L},
            {"131", 4L}, {"121", 4L}};
    checkResult("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id", expecteds1);
    checkResult("SELECT jsonColumnWithoutIndex.id, count(*) FROM testTable GROUP BY jsonColumnWithoutIndex.id",
        expecteds1);
  }

  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test(enabled = false)
  public void testJsonGroupByHaving() {
    Object[][] expecteds1 = {{"mouse", 8L}};
    checkResult(
        "SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name"
            + ".last = 'mouse'", expecteds1);
    checkResult(
        "SELECT jsonColumnWithoutIndex.name.last, count(*) FROM testTable GROUP BY jsonColumnWithoutIndex.name.last "
            + "HAVING jsonColumnWithoutIndex.name.last = 'mouse'", expecteds1);
  }

  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test(enabled = false)
  public void testJsonSelectFilterGroupBy() {
    Object[][] expecteds1 = {{"duck", 4L}};
    checkResult(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last",
        expecteds1);
    checkResult(
        "SELECT jsonColumnWithoutIndex.name.last, count(*) FROM testTable WHERE jsonColumnWithoutIndex.id = 101 GROUP"
            + " BY jsonColumnWithoutIndex.name.last", expecteds1);
  }

  /** Test an aggregation function over json path expression in SELECT clause. */
  @Test(enabled = false)
  public void testTransformFunctionOverJsonPathSelectExpression() {
    // Apply string transform function on json path expression.
    Object[][] expecteds1 = {{"DAFFY"}};
    checkResult("SELECT UPPER(jsonColumn.name.first) FROM testTable LIMIT 1", expecteds1);
    checkResult("SELECT UPPER(jsonColumnWithoutIndex.name.first) FROM testTable LIMIT 1", expecteds1);

    // Apply date transform function on json path expression and check for IS NULL
    Object[][] expecteds2 = {{Long.MIN_VALUE}};
    checkResult("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NULL LIMIT 1",
        expecteds2);
    try {
      checkResult(
          "SELECT FROMEPOCHDAYS(jsonColumnWithoutIndex.days) FROM testTable WHERE jsonColumnWithoutIndex.days IS NULL"
              + " LIMIT 1", expecteds2);
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert
          .assertEquals(e.getMessage(), "java.lang.UnsupportedOperationException: Unsupported predicate type: IS_NULL");
    }

    // Apply date transform function on json path expression and check for IS NOT NULL
    Object[][] expecteds3 = {{9590400000L}};
    checkResult("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NOT NULL LIMIT 1",
        expecteds3);
    try {
      checkResult(
          "SELECT FROMEPOCHDAYS(jsonColumnWithoutIndex.days) FROM testTable WHERE jsonColumnWithoutIndex.days IS NOT "
              + "NULL LIMIT 1", expecteds3);
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getMessage(),
          "java.lang.UnsupportedOperationException: Unsupported predicate type: IS_NOT_NULL");
    }
  }

  /** Test a numerical function over json path expression in SELECT clause. */
  @Test(enabled = false)
  public void testNumericalFunctionOverJsonPathSelectExpression() {

    // Test without user-specified alias.
    Object[][] expecteds1 = {{181.0}};
    checkResult("SELECT MAX(jsonColumn.id) FROM testTable", expecteds1);
    checkResult("SELECT MAX(jsonColumnWithoutIndex.id) FROM testTable", expecteds1);

    // Test with user-specified alias.
    Object[][] expecteds2 = {{181.0}};
    checkResult("SELECT MAX(jsonColumn.id) AS x FROM testTable", expecteds2);
    checkResult("SELECT MAX(jsonColumnWithoutIndex.id) AS x FROM testTable", expecteds2);

    // Test with nested function calls (minus function being used within max function).
    Object[][] expecteds3 = {{176.0}};
    checkResult("SELECT MAX(jsonColumn.id - 5) FROM testTable", expecteds3);
    checkResult("SELECT MAX(jsonColumnWithoutIndex.id - 5) FROM testTable", expecteds3);
  }

  @Test(enabled = false)
  public void testTopLevelArrayPathExpressions() {
    // SELECT using json path expressions that refers to second element of a top-level array.
    Object[][] expecteds1 = {{"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,"
        + "\"i2\":4}"}};
    checkResult("SELECT jsonColumn[1] FROM testTable WHERE intColumn=14", expecteds1);

    // SELECT using json path expressions that refers to item within second element of a top-level array.
    Object[][] expecteds2 = {{"4"}, {"4"}, {"4"}, {"4"}};
    checkResult("SELECT jsonColumn[1].i2 FROM testTable WHERE intColumn=14", expecteds2);

    // SELECT using json path expression and check path expression for IS NULL.
    checkResult("SELECT jsonColumn[1].i2 FROM testTable WHERE jsonColumn[1].i2 IS NOT NULL", expecteds2);

    // GROUP BY using a json path expression that refers to a top-level array element.
    Object[][] expecteds3 = {{"{\"i1\":3,\"i2\":4}", 4L}, {"null", 56L}};
    checkResult("SELECT jsonColumn[1], count(*) FROM testTable GROUP BY jsonColumn[1]", expecteds3);
  }
}
