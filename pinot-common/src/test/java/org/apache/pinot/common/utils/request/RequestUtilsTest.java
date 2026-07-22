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

import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class RequestUtilsTest {

  @Test
  public void testNullLiteralParsing() {
    SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    Expression nullExpr = RequestUtils.getLiteralExpression(nullLiteral);
    assertEquals(nullExpr.getType(), ExpressionType.LITERAL);
    assertTrue(nullExpr.getLiteral().getNullValue());
  }

  /// Pins the factory contract that `RequestUtils.getFunction` always returns a function whose
  /// operand list is mutable, even when the caller passes an immutable list. Downstream query
  /// rewriters and filter optimizers mutate operands in place; a regression here would resurface as
  /// an `UnsupportedOperationException` far from this factory (e.g. the broker RLS +
  /// expression-override path).
  @Test
  public void testGetFunctionReturnsMutableOperandList() {
    Expression a = RequestUtils.getLiteralExpression(1L);
    Expression b = RequestUtils.getLiteralExpression(2L);

    // List overload with a genuinely immutable input list must be defensively copied.
    Function fromList = RequestUtils.getFunction("and", List.of(a, b));
    fromList.getOperands().replaceAll(o -> o);
    fromList.getOperands().set(0, b);
    fromList.getOperands().add(a);
    assertEquals(fromList.getOperands().size(), 3);

    // Varargs and single-operand overloads delegate to the List overload and share the guarantee.
    Function fromVarargs = RequestUtils.getFunction("and", a, b);
    fromVarargs.getOperands().replaceAll(o -> o);
    fromVarargs.getOperands().add(b);
    assertEquals(fromVarargs.getOperands().size(), 3);

    Function fromSingle = RequestUtils.getFunction("not", a);
    fromSingle.getOperands().replaceAll(o -> o);
    fromSingle.getOperands().add(b);
    assertEquals(fromSingle.getOperands().size(), 2);
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
              + "table1 JOIN table2 ON table1.id = table2.id LIMIT 10;", Set.of("table1", "table2")},
      {"SET useMultiStageEngine=true; SELECT foo, occurredAt, "
              + "LAG(occurredAt) OVER (PARTITION BY foo ORDER BY occurredAt) AS laggedAt FROM events",
          Set.of("events")}
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

  // ---------------------------------------------------------------------------------------------
  // Alias helpers: isAliased / unwrapAlias / extractAliasOrIdentifierName
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testIsAliasedAliasedFunctionReturnsTrue() {
    Expression expr = aliased(sumOf("amount"), "total");
    assertTrue(RequestUtils.isAliased(expr));
  }

  @Test
  public void testIsAliasedBareIdentifierReturnsFalse() {
    Expression expr = RequestUtils.getIdentifierExpression("country");
    assertFalse(RequestUtils.isAliased(expr));
  }

  @Test
  public void testIsAliasedAggregationWithoutAsReturnsFalse() {
    // SUM(x) without AS — the operator is "sum", not "as".
    Expression expr = sumOf("amount");
    assertFalse(RequestUtils.isAliased(expr));
  }

  @Test
  public void testIsAliasedNullReturnsFalse() {
    assertFalse(RequestUtils.isAliased(null));
  }

  @Test
  public void testIsAliasedLiteralReturnsFalse() {
    Expression expr = RequestUtils.getLiteralExpression(42L);
    assertFalse(RequestUtils.isAliased(expr));
  }

  @Test
  public void testUnwrapAliasAliasedReturnsUnderlyingExpression() {
    Expression sum = sumOf("amount");
    Expression aliased = aliased(sum, "total");
    Expression unwrapped = RequestUtils.unwrapAlias(aliased);
    // Same Expression instance returned (we store it in operands, no copy).
    assertSame(unwrapped, sum);
  }

  @Test
  public void testUnwrapAliasBareIdentifierReturnsExpressionUnchanged() {
    Expression expr = RequestUtils.getIdentifierExpression("country");
    Expression unwrapped = RequestUtils.unwrapAlias(expr);
    assertSame(unwrapped, expr);
  }

  @Test
  public void testUnwrapAliasAggregationWithoutAsReturnsExpressionUnchanged() {
    Expression expr = sumOf("amount");
    Expression unwrapped = RequestUtils.unwrapAlias(expr);
    assertSame(unwrapped, expr);
  }

  @Test
  public void testExtractAliasOrIdentifierNameAliasedFunctionReturnsAliasName() {
    Expression expr = aliased(sumOf("amount"), "total");
    assertEquals(RequestUtils.extractAliasOrIdentifierName(expr), "total");
  }

  @Test
  public void testExtractAliasOrIdentifierNameBareIdentifierReturnsIdentifierName() {
    Expression expr = RequestUtils.getIdentifierExpression("country");
    assertEquals(RequestUtils.extractAliasOrIdentifierName(expr), "country");
  }

  @Test
  public void testExtractAliasOrIdentifierNameAggregationWithoutAsThrows() {
    Expression expr = sumOf("amount");
    assertThrows(IllegalStateException.class, () -> RequestUtils.extractAliasOrIdentifierName(expr));
  }

  @Test
  public void testExtractAliasOrIdentifierNameLiteralThrows() {
    Expression expr = RequestUtils.getLiteralExpression(42L);
    assertThrows(IllegalStateException.class, () -> RequestUtils.extractAliasOrIdentifierName(expr));
  }

  @Test
  public void testExtractAliasOrIdentifierNameAliasOperandNotIdentifierThrows() {
    // Construct a malformed `as` call whose alias operand is a literal — should throw.
    Function asFunc = new Function(SqlKind.AS.lowerName);
    asFunc.addToOperands(RequestUtils.getIdentifierExpression("amount"));
    asFunc.addToOperands(RequestUtils.getLiteralExpression(99L));
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(asFunc);
    assertThrows(IllegalStateException.class, () -> RequestUtils.extractAliasOrIdentifierName(expr));
  }

  // ---------- helpers ----------

  private static Expression sumOf(String column) {
    Function func = new Function("sum");
    func.setOperands(List.of(RequestUtils.getIdentifierExpression(column)));
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(func);
    return expr;
  }

  private static Expression aliased(Expression expression, String alias) {
    Function asFunc = new Function(SqlKind.AS.lowerName);
    asFunc.addToOperands(expression);
    Expression aliasExpr = new Expression(ExpressionType.IDENTIFIER);
    aliasExpr.setIdentifier(new Identifier(alias));
    asFunc.addToOperands(aliasExpr);
    Expression result = new Expression(ExpressionType.FUNCTION);
    result.setFunctionCall(asFunc);
    return result;
  }
}
