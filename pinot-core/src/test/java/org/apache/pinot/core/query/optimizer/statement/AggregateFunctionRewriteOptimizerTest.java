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

import java.util.Map;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Verifies which aggregation calls are rewritten from the schema, with and without the `autoRewriteAggregationType`
/// query option. Each test compiles its own queries, so the class has no shared mutable state.
public class AggregateFunctionRewriteOptimizerTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("intCol", DataType.INT)
      .addSingleValueDimension("longCol", DataType.LONG)
      .addSingleValueDimension("floatCol", DataType.FLOAT)
      .addSingleValueDimension("doubleCol", DataType.DOUBLE)
      .addSingleValueDimension("bigDecimalCol", DataType.BIG_DECIMAL)
      .addSingleValueDimension("stringCol", DataType.STRING)
      .addSingleValueDimension("boolCol", DataType.BOOLEAN)
      .addSingleValueDimension("timestampCol", DataType.TIMESTAMP)
      .addSingleValueDimension("jsonCol", DataType.JSON)
      .addSingleValueDimension("bytesCol", DataType.BYTES)
      .addSingleValueDimension("uuidCol", DataType.UUID)
      .addMultiValueDimension("mvCol", DataType.INT)
      .addMultiValueDimension("mvStringCol", DataType.STRING)
      .addSingleValueDimension("timeCol", DataType.LONG)
      .addComplex("mapCol", DataType.MAP, Map.of(
          "key", new DimensionFieldSpec("key", DataType.STRING, true),
          "value", new DimensionFieldSpec("value", DataType.INT, true)))
      .build();
  private static final String AUTO_REWRITE = "SET autoRewriteAggregationType=true; ";
  private static final String INFER_ANY_VALUE_TYPE = "SET inferAnyValueType=true; ";

  @DataProvider
  public Object[][] dataTypes() {
    return new Object[][]{
        {"intCol", "INT"}, {"longCol", "LONG"}, {"floatCol", "FLOAT"}, {"doubleCol", "DOUBLE"},
        {"bigDecimalCol", "BIG_DECIMAL"}, {"stringCol", "STRING"}, {"jsonCol", "STRING"}, {"boolCol", "BOOLEAN"},
        {"timestampCol", "TIMESTAMP"}, {"bytesCol", "BYTES"}, {"uuidCol", "UUID"}
    };
  }

  @Test(dataProvider = "dataTypes")
  public void testValueWithTimeInfersDataType(String column, String dataType) {
    for (String function : new String[]{"FIRST_WITH_TIME", "LASTWITHTIME"}) {
      assertRewrite("SELECT " + function + "(" + column + ", timeCol) FROM testTable",
          "SELECT " + function + "(" + column + ", timeCol, '" + dataType + "') FROM testTable");
    }
  }

  @Test
  public void testValueWithTimeRejectsMultiValueDataColumn() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT FIRST_WITH_TIME(mvCol, timeCol) FROM testTable");
    BadQueryRequestException exception = expectThrows(BadQueryRequestException.class,
        () -> new AggregateFunctionRewriteOptimizer().optimize(query, SCHEMA));
    assertEquals(exception.getMessage(),
        "Unsupported data type for FIRSTWITHTIME(dataColumn, timeColumn): multi-value INT");
  }

  @Test
  public void testValueWithTimeKeepsDataColumnsWithoutSchemaType() {
    // These keep the two-argument form, which fails with an error asking for the explicit data type.
    for (String column : new String[]{"missingCol", "trim(stringCol)"}) {
      assertUnchanged("SELECT FIRST_WITH_TIME(" + column + ", timeCol) FROM testTable");
    }
    assertUnchanged("SELECT FIRST_WITH_TIME(stringCol, timeCol, 'STRING'), LAST_WITH_TIME(intCol, timeCol, 'LONG') "
        + "FROM testTable");
  }

  @DataProvider
  public Object[][] arrayAggDataTypes() {
    return new Object[][]{
        {"intCol", "INT"}, {"longCol", "LONG"}, {"floatCol", "FLOAT"}, {"doubleCol", "DOUBLE"},
        {"bigDecimalCol", "BIG_DECIMAL"}, {"stringCol", "STRING"}, {"jsonCol", "STRING"}, {"boolCol", "BOOLEAN"},
        {"timestampCol", "TIMESTAMP"}, {"bytesCol", "BYTES"}, {"uuidCol", "UUID"}, {"mvCol", "INT"}
    };
  }

  @Test(dataProvider = "arrayAggDataTypes")
  public void testArrayAggInfersDataType(String column, String dataType) {
    assertRewrite("SELECT ARRAY_AGG(" + column + "), arrayAgg(" + column + ", true) FROM testTable",
        "SELECT ARRAY_AGG(" + column + ", '" + dataType + "'), arrayAgg(" + column + ", '" + dataType + "', true) "
            + "FROM testTable");
  }

  @Test
  public void testArrayAggRejectsUnsupportedColumn() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT ARRAY_AGG(mapCol) FROM testTable");
    BadQueryRequestException exception = expectThrows(BadQueryRequestException.class,
        () -> new AggregateFunctionRewriteOptimizer().optimize(query, SCHEMA));
    assertEquals(exception.getMessage(), "Unsupported data type for ARRAYAGG(expression): MAP");
  }

  @Test
  public void testArrayAggKeepsExplicitFormsAndColumnsWithoutSchemaType() {
    assertUnchanged("SELECT ARRAY_AGG(intCol, 'LONG'), ARRAY_AGG(intCol, 'LONG', true), ARRAY_AGG(intCol, 1), "
        + "ARRAY_AGG(missingCol), ARRAY_AGG(trim(stringCol), true) FROM testTable");
  }

  @Test
  public void testRewritesNestedAggregationsConsistently() {
    // Aliases, transforms, FILTER, HAVING and ORDER BY wrap the aggregation in another function.
    assertRewrite("SELECT intCol, FIRST_WITH_TIME(stringCol, timeCol) AS v1, "
            + "trim(LAST_WITH_TIME(stringCol, timeCol)) AS v2, "
            + "FIRST_WITH_TIME(doubleCol, timeCol) FILTER (WHERE boolCol) "
            + "FROM testTable GROUP BY intCol HAVING FIRST_WITH_TIME(longCol, timeCol) > 1 "
            + "ORDER BY LAST_WITH_TIME(timestampCol, timeCol)",
        "SELECT intCol, FIRST_WITH_TIME(stringCol, timeCol, 'STRING') AS v1, "
            + "trim(LAST_WITH_TIME(stringCol, timeCol, 'STRING')) AS v2, "
            + "FIRST_WITH_TIME(doubleCol, timeCol, 'DOUBLE') FILTER (WHERE boolCol) "
            + "FROM testTable GROUP BY intCol HAVING FIRST_WITH_TIME(longCol, timeCol, 'LONG') > 1 "
            + "ORDER BY LAST_WITH_TIME(timestampCol, timeCol, 'TIMESTAMP')");
  }

  @Test(dataProvider = "dataTypes")
  public void testAnyValueInfersDataType(String column, String dataType) {
    for (String function : new String[]{"ANY_VALUE", "anyValue"}) {
      assertRewrite(INFER_ANY_VALUE_TYPE + "SELECT " + function + "(" + column + ") FROM testTable",
          INFER_ANY_VALUE_TYPE + "SELECT " + function + "(" + column + ", '" + dataType + "') FROM testTable");
    }
  }

  @Test
  public void testAnyValueRejectsMultiValueColumn() {
    PinotQuery query =
        CalciteSqlParser.compileToPinotQuery(INFER_ANY_VALUE_TYPE + "SELECT ANY_VALUE(mvCol) FROM testTable");
    BadQueryRequestException exception = expectThrows(BadQueryRequestException.class,
        () -> new AggregateFunctionRewriteOptimizer().optimize(query, SCHEMA));
    assertEquals(exception.getMessage(), "Unsupported data type for ANYVALUE(expr): multi-value INT");
    assertUnchanged("SELECT ANY_VALUE(mvCol) FROM testTable");
  }

  @Test
  public void testAnyValueKeepsArgumentsWithoutSchemaType() {
    for (String column : new String[]{"missingCol", "trim(stringCol)"}) {
      assertUnchanged(INFER_ANY_VALUE_TYPE + "SELECT ANY_VALUE(" + column + ") FROM testTable");
    }
    assertUnchanged(INFER_ANY_VALUE_TYPE + "SELECT ANY_VALUE(intCol, 'LONG') FROM testTable");
  }

  @Test
  public void testRewritesNestedAnyValueConsistently() {
    assertRewrite(INFER_ANY_VALUE_TYPE + "SELECT intCol, ANY_VALUE(stringCol) AS v1, ANY_VALUE(doubleCol) FILTER "
            + "(WHERE boolCol) FROM testTable GROUP BY intCol HAVING ANY_VALUE(longCol) > 1 "
            + "ORDER BY ANY_VALUE(timestampCol)",
        INFER_ANY_VALUE_TYPE + "SELECT intCol, ANY_VALUE(stringCol, 'STRING') AS v1, ANY_VALUE(doubleCol, 'DOUBLE') "
            + "FILTER (WHERE boolCol) FROM testTable GROUP BY intCol HAVING ANY_VALUE(longCol, 'LONG') > 1 "
            + "ORDER BY ANY_VALUE(timestampCol, 'TIMESTAMP')");
  }

  @Test
  public void testAnyValueDataTypeRequiresQueryOption() {
    String query = "SELECT ANY_VALUE(intCol), FIRST_WITH_TIME(intCol, timeCol) FROM testTable";
    String expectedQuery = "SELECT ANY_VALUE(intCol, 'INT'), FIRST_WITH_TIME(intCol, timeCol, 'INT') FROM testTable";
    assertRewrite(INFER_ANY_VALUE_TYPE + query, INFER_ANY_VALUE_TYPE + expectedQuery);
    // The option only applies to ANY_VALUE.
    String unchangedAnyValue = "SELECT ANY_VALUE(intCol), FIRST_WITH_TIME(intCol, timeCol, 'INT') FROM testTable";
    assertRewrite(query, unchangedAnyValue);
    assertRewrite("SET inferAnyValueType=false; " + query, "SET inferAnyValueType=false; " + unchangedAnyValue);
  }

  @Test
  public void testModeInfersNonNumericDataType() {
    assertRewrite("SELECT MODE(stringCol), MODE(jsonCol), MODE(boolCol, 'MAX'), MODE(timestampCol, 'AVG') "
            + "FROM testTable",
        "SELECT MODE(stringCol, 'MIN', 'STRING'), MODE(jsonCol, 'MIN', 'STRING'), MODE(boolCol, 'MAX'), "
            + "MODE(timestampCol, 'AVG') FROM testTable");
    assertRewrite("SELECT intCol, MODE(stringCol) FILTER (WHERE boolCol) AS m FROM testTable GROUP BY intCol "
            + "HAVING MODE(boolCol) = true ORDER BY MODE(timestampCol, 'MAX')",
        "SELECT intCol, MODE(stringCol, 'MIN', 'STRING') FILTER (WHERE boolCol) AS m FROM testTable GROUP BY intCol "
            + "HAVING MODE(boolCol) = true ORDER BY MODE(timestampCol, 'MAX')");
  }

  @Test
  public void testModeKeepsOtherInputs() {
    // Numeric, BOOLEAN and TIMESTAMP MODE keep their DOUBLE results
    assertUnchanged("SELECT MODE(intCol), MODE(longCol, 'AVG'), MODE(floatCol, 'MAX'), MODE(doubleCol) FROM testTable");
    for (String column : new String[]{
        "boolCol", "timestampCol", "bytesCol", "uuidCol", "mvStringCol", "missingCol", "trim(stringCol)"
    }) {
      assertUnchanged("SELECT MODE(" + column + ") FROM testTable");
    }
    assertUnchanged("SELECT MODE(stringCol, 'MAX', 'STRING') FROM testTable");
  }

  @Test
  public void testTypedVariantsRequireQueryOption() {
    String query = "SELECT MIN(stringCol) AS m, MAX(longCol), SUM(intCol), SUM(longCol), MIN(doubleCol) "
        + "FROM testTable ORDER BY MIN(stringCol)";
    assertUnchanged(query);
    assertRewrite(AUTO_REWRITE + query, AUTO_REWRITE
        + "SELECT MINSTRING(stringCol) AS m, MAXLONG(longCol), SUMINT(intCol), SUMLONG(longCol), MIN(doubleCol) "
        + "FROM testTable ORDER BY MINSTRING(stringCol)");
  }

  @Test
  public void testTypedVariantsRewriteNestedAggregations() {
    // Aggregations under an alias, a transform and HAVING are rewritten like top-level ones, so the select list and
    // HAVING compute the same aggregation.
    String query = "SELECT intCol, MIN(longCol) AS m, MAX(longCol) + 1 FROM testTable GROUP BY intCol "
        + "HAVING MIN(longCol) > 5";
    assertUnchanged(query);
    assertRewrite(AUTO_REWRITE + query, AUTO_REWRITE
        + "SELECT intCol, MINLONG(longCol) AS m, MAXLONG(longCol) + 1 FROM testTable GROUP BY intCol "
        + "HAVING MINLONG(longCol) > 5");
  }

  @Test
  public void testNoSchema() {
    PinotQuery query =
        CalciteSqlParser.compileToPinotQuery("SELECT FIRST_WITH_TIME(stringCol, timeCol) FROM testTable");
    PinotQuery original = query.deepCopy();
    new AggregateFunctionRewriteOptimizer().optimize(query, null);
    assertEquals(query, original);
  }

  private static void assertRewrite(String query, String expectedQuery) {
    PinotQuery actual = CalciteSqlParser.compileToPinotQuery(query);
    new AggregateFunctionRewriteOptimizer().optimize(actual, SCHEMA);
    PinotQuery expected = CalciteSqlParser.compileToPinotQuery(expectedQuery);
    assertEquals(actual.getSelectList(), expected.getSelectList());
    assertEquals(actual.getHavingExpression(), expected.getHavingExpression());
    assertEquals(actual.getOrderByList(), expected.getOrderByList());
  }

  private static void assertUnchanged(String query) {
    assertRewrite(query, query);
  }
}
