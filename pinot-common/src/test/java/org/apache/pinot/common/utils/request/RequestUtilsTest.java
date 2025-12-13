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
import java.util.Set;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
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

  /**
   * Verifies that non-integer decimal SQL literals (e.g. {@code 3.14}) are encoded as
   * {@code DOUBLE_VALUE} in the broker-to-server thrift {@link org.apache.pinot.common.request.Literal}.
   *
   * <p>Background: {@link org.apache.pinot.common.request.Literal} is a Thrift <em>union</em>, so
   * only one field can be set at a time.  Sending {@code BIG_DECIMAL_VALUE} would break older
   * servers that do not yet recognise that field.  Once all servers have been upgraded and the
   * {@code BIG_DECIMAL_VALUE} case is handled everywhere, the broker can be changed to use
   * {@code setBigDecimalValue()} instead (see the TODO in
   * {@link RequestUtils#getLiteral(org.apache.calcite.sql.SqlLiteral)}).
   *
   * <p>The server side ({@link org.apache.pinot.common.request.context.LiteralContext}) already
   * handles {@code BIG_DECIMAL_VALUE} correctly, so this test also verifies that round-trip
   * behaviour.
   */
  @Test
  public void testDecimalSqlLiteralUsesDoubleValueForBackwardCompatibility() {
    // Exact decimal literal (e.g. "3.14" in SQL) – isExact=true, isInteger=false
    SqlLiteral exactDecimal = SqlLiteral.createExactNumeric("3.14", SqlParserPos.ZERO);
    Expression expr = RequestUtils.getLiteralExpression(exactDecimal);
    assertEquals(expr.getType(), ExpressionType.LITERAL);
    // Must use DOUBLE_VALUE for backward compatibility with older servers that do not support
    // BIG_DECIMAL_VALUE.  Change to BIG_DECIMAL_VALUE in the next release.
    assertTrue(expr.getLiteral().isSetDoubleValue(),
        "Non-integer decimal literals must be encoded as DOUBLE_VALUE for backward compatibility");
    assertEquals(expr.getLiteral().getDoubleValue(), 3.14, 1e-9);
  }

  /**
   * Verifies that approximate (floating-point) SQL literals (e.g. {@code 3.14E0}) are also
   * encoded as {@code DOUBLE_VALUE}.
   */
  @Test
  public void testApproxDecimalSqlLiteralUsesDoubleValue() {
    // Approximate numeric literal (e.g. "3.14E0" in SQL) – isExact=false
    SqlLiteral approxDecimal = SqlLiteral.createApproxNumeric("3.14E0", SqlParserPos.ZERO);
    Expression expr = RequestUtils.getLiteralExpression(approxDecimal);
    assertEquals(expr.getType(), ExpressionType.LITERAL);
    assertTrue(expr.getLiteral().isSetDoubleValue(),
        "Approximate decimal literals must be encoded as DOUBLE_VALUE");
    assertEquals(expr.getLiteral().getDoubleValue(), 3.14, 1e-9);
  }

  /**
   * Verifies that integer SQL literals within the {@code int} range are encoded as
   * {@code INT_VALUE}, and those outside the range are encoded as {@code LONG_VALUE}.
   */
  @Test
  public void testIntegerSqlLiteralEncoding() {
    // Small integer → INT_VALUE
    SqlLiteral smallInt = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);
    Expression smallExpr = RequestUtils.getLiteralExpression(smallInt);
    assertTrue(smallExpr.getLiteral().isSetIntValue(), "Small integer must be INT_VALUE");
    assertEquals(smallExpr.getLiteral().getIntValue(), 42);

    // Large integer (> Integer.MAX_VALUE) → LONG_VALUE
    SqlLiteral largeInt = SqlLiteral.createExactNumeric("9999999999", SqlParserPos.ZERO);
    Expression largeExpr = RequestUtils.getLiteralExpression(largeInt);
    assertTrue(largeExpr.getLiteral().isSetLongValue(), "Large integer must be LONG_VALUE");
    assertEquals(largeExpr.getLiteral().getLongValue(), 9999999999L);
  }

  /**
   * Verifies that the programmatic {@link RequestUtils#getLiteral(BigDecimal)} helper (used when
   * constructing literals from Java objects, not from SQL text) correctly uses
   * {@code BIG_DECIMAL_VALUE} to preserve full precision.
   */
  @Test
  public void testGetLiteralFromBigDecimalObjectUsesBigDecimalValue() {
    BigDecimal value = new BigDecimal("123456789.123456789");
    Expression expr = RequestUtils.getLiteralExpression(value);
    assertEquals(expr.getType(), ExpressionType.LITERAL);
    assertTrue(expr.getLiteral().isSetBigDecimalValue(),
        "getLiteral(BigDecimal) must use BIG_DECIMAL_VALUE to preserve full precision");
  }
}
