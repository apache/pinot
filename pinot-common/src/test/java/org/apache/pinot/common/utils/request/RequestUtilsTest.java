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
package org.apache.pinot.common.utils.request;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RequestUtilsTest {

  @Test
  public void testNullLiteralParsing() {
    SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    Expression nullExpr = RequestUtils.getLiteralExpression(nullLiteral);
    assertEquals(nullExpr.getType(), ExpressionType.LITERAL);
    assertTrue(nullExpr.getLiteral().getNullValue());
  }

  @Test
  public void testGetLiteralExpressionForPrimitiveLong() {
    Expression literalExpression = RequestUtils.getLiteralExpression(4500L);
    assertTrue(literalExpression.getLiteral().isSetLongValue());
    assertEquals(literalExpression.getLiteral().getLongValue(), 4500L);
  }

  @Test
  public void testParseQuery() {
    SqlNodeAndOptions result = RequestUtils.parseQuery("select foo from countries where bar > 1");
    assertTrue(result.getParseTimeNs() > 0);
    assertEquals(result.getSqlType(), PinotSqlType.DQL);
    assertEquals(result.getSqlNode().toSqlString((SqlDialect) null).toString(),
        "SELECT `foo`\n" + "FROM `countries`\n" + "WHERE `bar` > 1");
  }

  @DataProvider(name = "queryProvider")
  public Object[][] queryProvider() {
    return new Object[][] {
      {"select foo from countries where bar > 1", Set.of("countries")},
      {"select 1", null},
      {"SET useMultiStageEngine=true; SELECT table1.foo, table2.bar FROM "
              + "table1 JOIN table2 ON table1.id = table2.id LIMIT 10;", Set.of("table1", "table2")}
    };
  }

  @Test(dataProvider = "queryProvider")
  public void testResolveTableNames(String query, Set<String> expectedSet) {
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    Set<String> tableNames =
        RequestUtils.getTableNames(CalciteSqlParser.compileSqlNodeToPinotQuery(sqlNodeAndOptions.getSqlNode()));

    if (expectedSet == null) {
      assertNull(tableNames);
    } else {
      assertEquals(tableNames, expectedSet);
    }
  }

  private static Schema buildTestSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svLong", FieldSpec.DataType.LONG)
        .addSingleValueDimension("svFloat", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("svDouble", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("svBigDecimal", FieldSpec.DataType.BIG_DECIMAL)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svBoolean", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("svTimestamp", FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension("svBytes", FieldSpec.DataType.BYTES)
        .addMultiValueDimension("mvInt", FieldSpec.DataType.INT)
        .addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
        .addMultiValueDimension("mvFloat", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("mvDouble", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("mvString", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvBoolean", FieldSpec.DataType.BOOLEAN)
        .addMultiValueDimension("mvTimestamp", FieldSpec.DataType.TIMESTAMP)
        .addMultiValueDimension("mvBytes", FieldSpec.DataType.BYTES)
        .build();
  }

  @Test
  public void testInferExpressionTypeIdentifiers() {
    Schema schema = buildTestSchema();
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svInt"), schema),
        ColumnDataType.INT);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svLong"), schema),
        ColumnDataType.LONG);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svFloat"), schema),
        ColumnDataType.FLOAT);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svDouble"), schema),
        ColumnDataType.DOUBLE);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svBigDecimal"), schema),
        ColumnDataType.BIG_DECIMAL);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svString"), schema),
        ColumnDataType.STRING);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svBoolean"), schema),
        ColumnDataType.BOOLEAN);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svTimestamp"), schema),
        ColumnDataType.TIMESTAMP);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("svBytes"), schema),
        ColumnDataType.BYTES);

    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvInt"), schema),
        ColumnDataType.INT_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvLong"), schema),
        ColumnDataType.LONG_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvFloat"), schema),
        ColumnDataType.FLOAT_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvDouble"), schema),
        ColumnDataType.DOUBLE_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvString"), schema),
        ColumnDataType.STRING_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvBoolean"), schema),
        ColumnDataType.BOOLEAN_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvTimestamp"), schema),
        ColumnDataType.TIMESTAMP_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("mvBytes"), schema),
        ColumnDataType.BYTES_ARRAY);

    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getIdentifierExpression("unknownCol"), schema),
        ColumnDataType.UNKNOWN);
  }

  @Test
  public void testInferExpressionTypeLiterals() {
    Schema schema = buildTestSchema();
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(true), schema),
        ColumnDataType.BOOLEAN);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(1), schema),
        ColumnDataType.INT);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(1L), schema),
        ColumnDataType.LONG);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(1.0f), schema),
        ColumnDataType.FLOAT);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(1.0d), schema),
        ColumnDataType.DOUBLE);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new BigDecimal("123.45")), schema),
        ColumnDataType.BIG_DECIMAL);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression("abc"), schema),
        ColumnDataType.STRING);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new byte[]{1, 2}), schema),
        ColumnDataType.BYTES);

    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new int[]{1, 2}), schema),
        ColumnDataType.INT_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new long[]{1L, 2L}), schema),
        ColumnDataType.LONG_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new float[]{1.0f, 2.0f}), schema),
        ColumnDataType.FLOAT_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new double[]{1.0d, 2.0d}), schema),
        ColumnDataType.DOUBLE_ARRAY);
    assertEquals(RequestUtils.inferExpressionType(RequestUtils.getLiteralExpression(new String[]{"a", "b"}), schema),
        ColumnDataType.STRING_ARRAY);
  }

  @Test
  public void testInferExpressionTypeScalarFunction() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // abs(x) -> DOUBLE
    Expression absOnInt = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("abs"), RequestUtils.getIdentifierExpression("svInt"));
    assertEquals(RequestUtils.inferExpressionType(absOnInt, schema), ColumnDataType.DOUBLE);

    // intDiv(10.0, 3.0) -> LONG
    Expression intDiv = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("intDiv"),
        RequestUtils.getLiteralExpression(10.0d),
        RequestUtils.getLiteralExpression(3.0d));
    assertEquals(RequestUtils.inferExpressionType(intDiv, schema), ColumnDataType.LONG);
  }

  @Test
  public void testInferExpressionTypeScalarFunctionPolymorphic() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // plus(long, long) -> LONG
    Expression plusLongLong = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("plus"),
        RequestUtils.getIdentifierExpression("svLong"),
        RequestUtils.getIdentifierExpression("svLong"));
    assertEquals(RequestUtils.inferExpressionType(plusLongLong, schema), ColumnDataType.LONG);

    // plus(long, double) -> DOUBLE
    Expression plusLongDouble = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("plus"),
        RequestUtils.getIdentifierExpression("svLong"),
        RequestUtils.getIdentifierExpression("svDouble"));
    assertEquals(RequestUtils.inferExpressionType(plusLongDouble, schema), ColumnDataType.DOUBLE);
  }

  @Test
  public void testInferExpressionTypeNestedFunctions() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // ln(abs(3.0)) -> DOUBLE
    Expression absExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("abs"), RequestUtils.getLiteralExpression(3.0d));
    Expression lnAbs = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("ln"), absExpression);
    assertEquals(RequestUtils.inferExpressionType(lnAbs, schema), ColumnDataType.DOUBLE);
  }

  @Test
  public void testInferExpressionTypeUnknownAndAggregationFunctions() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // Unknown function -> UNKNOWN
    Expression unknownFn = RequestUtils.getFunctionExpression("notafunction",
        List.of(RequestUtils.getIdentifierExpression("svInt")));
    assertEquals(RequestUtils.inferExpressionType(unknownFn, schema), ColumnDataType.UNKNOWN);

    // Aggregation function or wrong arg count for scalar min -> UNKNOWN
    Expression minOneArg = RequestUtils.getFunctionExpression(FunctionRegistry.canonicalize("min"),
        List.of(RequestUtils.getIdentifierExpression("svInt")));
    assertEquals(RequestUtils.inferExpressionType(minOneArg, schema), ColumnDataType.UNKNOWN);
  }
}
