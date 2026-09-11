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
package org.apache.pinot.sql.parsers;

import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.sql.parsers.CalciteSqlParser.CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for CalciteSqlParser.
///
/// Important note about SQL string literal escaping:
/// - In standard SQL, to include a single quote within a string literal, you escape it as ''
/// - Calcite parser handles this escaping: 'It''s' in SQL becomes "It's" after parsing
/// - The legacy SSE behavior (CONFIG_OF_SSE_LEGACY_LITERAL_UNESCAPING) performs an ADDITIONAL
///   replacement of '' to ' on the already-parsed string, which can cause double-unescaping issues.
///
/// Example with 4 single quotes in SQL: 'test''''value'
/// - This represents a string with TWO single quotes: test''value
/// - After Calcite parsing: "test''value" (2 quotes)
/// - With legacy enabled (additional '' -> '): "test'value" (WRONG - lost a quote)
/// - Without legacy: "test''value" (CORRECT)
public class CalciteSqlParserTest {
  private static final String SINGLE_CHAR = "a";
  private static final String QUERY_TEMPLATE = "SELECT %s FROM %s";

  @AfterMethod
  public void resetLegacyUnescaping() {
    // Reset to default (false) after each test
    RequestUtils.setUseLegacyLiteralUnescaping(true);
  }

  @Test
  public void testPostgreSqlByteaHexLiterals() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT '\\x0102'::bytea");
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getBinaryValue(), new byte[]{1, 2});

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST('\\xDe Ad Be Ef' AS BYTEA)");
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getBinaryValue(),
        new byte[]{(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT '\\x'::ByTeA");
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getBinaryValue(), new byte[0]);

    Expression expression = CalciteSqlParser.compileToExpression("'\\x0102'::bytea");
    assertEquals(expression.getLiteral().getBinaryValue(), new byte[]{1, 2});
  }

  /// `::` must bind only to the literal on its left, not to the whole expression accumulated so far, so that a bytea
  /// constant can be used as the right operand of a comparison.
  @Test(dataProvider = "binaryOperatorsTakingByteaLiterals")
  public void testPostgreSqlByteaLiteralBindsTighterThanBinaryOperators(String predicate, String expectedOperator) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT id FROM myTable WHERE bytesColumn " + predicate + " '\\x0102'::bytea");
    Function filter = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filter.getOperator(), expectedOperator);
    assertEquals(filter.getOperands().get(0).getIdentifier().getName(), "bytesColumn");
    assertEquals(filter.getOperands().get(1).getLiteral().getBinaryValue(), new byte[]{1, 2});
  }

  @DataProvider
  public static Object[][] binaryOperatorsTakingByteaLiterals() {
    return new Object[][]{
        {"=", "EQUALS"},
        {"<>", "NOT_EQUALS"},
        {">", "GREATER_THAN"},
        {"<=", "LESS_THAN_OR_EQUAL"}
    };
  }

  @Test
  public void testPostgreSqlByteaLiteralInCompoundPredicate() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT id FROM myTable WHERE id = 1 AND bytesColumn = '\\x0102'::bytea");
    Function and = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(and.getOperator(), "AND");
    Function byteaEquals = and.getOperands().get(1).getFunctionCall();
    assertEquals(byteaEquals.getOperands().get(0).getIdentifier().getName(), "bytesColumn");
    assertEquals(byteaEquals.getOperands().get(1).getLiteral().getBinaryValue(), new byte[]{1, 2});
  }

  /// The infix and standard cast spellings must normalize to the same binary literal wherever they appear.
  @Test
  public void testPostgreSqlByteaLiteralMatchesStandardCastSpelling() {
    for (String bytea : List.of("'\\x0102'::bytea", "'\\x0102' :: ByTeA", "CAST('\\x0102' AS BYTEA)",
        "cast('\\x0102' as bytea)", "X'0102'")) {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("SELECT id FROM myTable WHERE bytesColumn = " + bytea);
      assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral()
          .getBinaryValue(), new byte[]{1, 2}, bytea);
      assertEquals(CalciteSqlParser.compileToExpression(bytea).getLiteral().getBinaryValue(), new byte[]{1, 2},
          bytea);
    }
  }

  @Test(dataProvider = "invalidPostgreSqlByteaLiterals")
  public void testInvalidPostgreSqlByteaHexLiterals(String sql, String expectedMessageFragment) {
    SqlCompilationException e =
        expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(sql));
    assertTrue(ExceptionUtils.getStackTrace(e).contains(expectedMessageFragment),
        "Expected <" + expectedMessageFragment + "> for " + sql + " but got: " + e.getMessage());
  }

  private static final String INVALID_CONSTANT = "Invalid PostgreSQL BYTEA hex constant";
  private static final String NOT_A_CONSTANT = "BYTEA casts are supported only for quoted hex constants";

  @DataProvider
  public static Object[][] invalidPostgreSqlByteaLiterals() {
    return new Object[][]{
        // Malformed hex constants.
        {"SELECT '\\x0'::bytea", INVALID_CONSTANT},
        {"SELECT '\\x0g'::bytea", INVALID_CONSTANT},
        {"SELECT '\\x0 1'::bytea", INVALID_CONSTANT},
        {"SELECT '0102'::bytea", INVALID_CONSTANT},
        // Full-width and non-Latin digits are hex digits to Character.digit but not to PostgreSQL.
        {"SELECT '\\x\uFF21\uFF22'::bytea", INVALID_CONSTANT},
        {"SELECT '\\x\u0660\u0661'::bytea", INVALID_CONSTANT},
        // Non-ASCII whitespace is whitespace to Character.isWhitespace but not to PostgreSQL.
        {"SELECT '\\x01\u205F02'::bytea", INVALID_CONSTANT},

        // BYTEA casts of something that is not a constant.
        {"SELECT bytesColumn::bytea FROM myTable", NOT_A_CONSTANT},
        {"SELECT CAST(bytesColumn AS BYTEA) FROM myTable", NOT_A_CONSTANT},
        {"SELECT id FROM myTable WHERE id = 1 AND bytesColumn::bytea = X'01'", NOT_A_CONSTANT},

        // `::` to a target type other than BYTEA.
        {"SELECT 1::int", "not for target type 'INTEGER'"},
        {"SELECT bytesColumn::varchar FROM myTable", "not for target type 'VARCHAR'"},

        // The item accessor binds tighter than `::`, so the target type does not survive as a type spec.
        {"SELECT bytesColumn::bytea[1] FROM myTable", "Unsupported PostgreSQL :: cast target"}
    };
  }

  @Test
  public void testIdentifierLength() {
    String tableName = extendIdentifierToMaxLength("exampleTable");
    String columnName = extendIdentifierToMaxLength("exampleColumn");

    String validQuery = createQuery(tableName, columnName);
    CalciteSqlParser.compileToPinotQuery(validQuery);

    String invalidTableNameQuery = createQuery(columnName, tableName + SINGLE_CHAR);
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidTableNameQuery));
    String invalidColumnNameQuery = createQuery(columnName + SINGLE_CHAR, tableName);
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidColumnNameQuery));
  }

  private String extendIdentifierToMaxLength(String identifier) {
    return identifier + SINGLE_CHAR.repeat(CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH - identifier.length());
  }

  private String createQuery(String columnName, String tableName) {
    return String.format(QUERY_TEMPLATE, columnName, tableName);
  }

  @Test(dataProvider = "nonReservedKeywords")
  public void testNonReservedKeywords(String keyword) {
    CalciteSqlParser.compileToPinotQuery(createQuery(keyword, "testTable"));
    CalciteSqlParser.compileToPinotQuery(createQuery(keyword.toUpperCase(), "testTable"));
  }

  @DataProvider
  public static Object[][] nonReservedKeywords() {
    return new Object[][]{
        new Object[]{"int"},
        new Object[]{"integer"},
        new Object[]{"long"},
        new Object[]{"bigint"},
        new Object[]{"float"},
        new Object[]{"double"},
        new Object[]{"big_decimal"},
        new Object[]{"decimal"},
        new Object[]{"boolean"},
        // TODO: Revisit if we should make "timestamp" non reserved
//        new Object[]{"timestamp"},
        new Object[]{"string"},
        new Object[]{"varchar"},
        new Object[]{"bytes"},
        new Object[]{"bytea"},
        new Object[]{"binary"},
        new Object[]{"varbinary"},
        new Object[]{"variant"},
        new Object[]{"uuid"},
        // UNSIGNED became reserved in Calcite 1.41 (CALCITE-1466); kept non-reserved for backward compatibility.
        new Object[]{"unsigned"}
    };
  }

  // ==================== Tests for SQL string literal escaping ====================
  //
  // These tests verify correct escaping behavior with both legacy and default modes.

  @Test
  public void testSimpleEscapedQuoteBothBehaviorsMatch() {
    // SQL: 'It''s' represents the string "It's" ('' = escaped single quote)
    // Calcite unescapes this to "It's"
    // Both legacy and non-legacy should return "It's" for this simple case
    String query = "SELECT 'It''s working' FROM testTable";

    // Without legacy (default)
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "It's working");

    // With legacy - same result for simple case
    RequestUtils.setUseLegacyLiteralUnescaping(true);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "It's working");
  }

  @Test
  public void testFourQuotesShowsDoubleUnescapingIssue() {
    // SQL: 'test''''value' - 4 quotes in SQL = 2 escaped quotes = 2 actual quotes in the string
    // After Calcite parsing: "test''value" (contains two single quotes)
    // With legacy (additional '' -> '): "test'value" (WRONG - double unescaping)
    // Without legacy: "test''value" (CORRECT)
    String query = "SELECT 'test''''value' FROM testTable";

    // With legacy: incorrectly reduces to one quote (double unescaping)
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test'value");

    // Without legacy: preserves the two single quotes
    RequestUtils.setUseLegacyLiteralUnescaping(false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test''value");
  }

  @Test
  public void testSixQuotesShowsDoubleUnescapingIssue() {
    // SQL: 'test''''''value' - 6 quotes in SQL = 3 escaped quotes = 3 actual quotes
    // After Calcite parsing: "test'''value" (3 single quotes)
    // With legacy (additional '' -> '): "test''value" (WRONG - lost a quote)
    // Without legacy: "test'''value" (CORRECT)
    String query = "SELECT 'test''''''value' FROM testTable";

    // With legacy: reduces from 3 to 2 quotes
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test''value");

    // Without legacy: preserves all three single quotes
    RequestUtils.setUseLegacyLiteralUnescaping(false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test'''value");
  }

  @Test
  public void testWhereClauseWithFourQuotes() {
    // Same test but in WHERE clause
    String query = "SELECT col FROM testTable WHERE name = 'O''''Brien'";

    // With legacy: "O'Brien" (double unescaping)
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Expression filterExpr = pinotQuery.getFilterExpression();
    Function function = filterExpr.getFunctionCall();
    assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "O'Brien");

    // Without legacy: "O''Brien" (two quotes)
    RequestUtils.setUseLegacyLiteralUnescaping(false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    filterExpr = pinotQuery.getFilterExpression();
    function = filterExpr.getFunctionCall();
    assertEquals(function.getOperator(), "EQUALS");
    assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "O''Brien");
  }

  @Test
  public void testJsonMatchWithFourQuotes() {
    // JSON_MATCH filter with 4 quotes to include actual single quotes in the JSON path filter
    // SQL: JSON_MATCH(jsonCol, '"$.name" = ''''John''''')
    // This means: "$.name" = ''John'' (value with surrounding single quotes)
    String query = "SELECT col FROM testTable WHERE JSON_MATCH(jsonCol, '\"$.name\" = ''''John''''')";

    // With legacy: reduces quotes (double unescaping)
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Expression filterExpr = pinotQuery.getFilterExpression();
    Function function = filterExpr.getFunctionCall();
    assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "\"$.name\" = 'John'");

    // Without legacy: preserves the two single quotes around John
    RequestUtils.setUseLegacyLiteralUnescaping(false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    filterExpr = pinotQuery.getFilterExpression();
    function = filterExpr.getFunctionCall();
    assertEquals(function.getOperator(), "JSON_MATCH");
    assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "\"$.name\" = ''John''");
  }

  @Test
  public void testNoQuotesUnaffected() {
    // Strings without quotes should be unaffected by either setting
    String query = "SELECT 'simple string' FROM testTable";

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "simple string");

    RequestUtils.setUseLegacyLiteralUnescaping(true);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "simple string");
  }

  @Test
  public void testEightQuotes() {
    // SQL: 'test''''''''value' - 8 quotes = 4 escaped quotes = 4 actual quotes
    // After Calcite parsing: "test''''value" (4 single quotes)
    // With legacy: "test''value" (2 quotes - lost half)
    // Without legacy: "test''''value" (correct)
    String query = "SELECT 'test''''''''value' FROM testTable";

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test''value");

    RequestUtils.setUseLegacyLiteralUnescaping(false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getStringValue(), "test''''value");
  }

  @Test
  public void testComplexQueryWithMultipleQuotePatterns() {
    // A complex query with different quote patterns in projection and filter
    String query = "SELECT 'He said ''''hello'''' and ''''goodbye''''' AS greeting FROM testTable "
        + "WHERE message = 'It''''s fine'";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);

    // Check projection: 'He said ''''hello'''' and ''''goodbye'''''
    // After Calcite: He said ''hello'' and ''goodbye''
    List<Expression> selectList = pinotQuery.getSelectList();
    Function asFunc = selectList.get(0).getFunctionCall();
    assertEquals(asFunc.getOperands().get(0).getLiteral().getStringValue(), "He said 'hello' and 'goodbye'");

    // Check filter: 'It''''s fine' -> It''s fine
    Expression filterExpr = pinotQuery.getFilterExpression();
    Function function = filterExpr.getFunctionCall();
    assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "It's fine");
  }
}
