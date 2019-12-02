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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.pql.parsers.PinotQuery2BrokerRequestConverter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some tests for the SQL compiler.
 */
public class CalciteSqlCompilerTest {

  @Test
  public void testQuotedStrings() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "Martha''s Vineyard");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = 'Martha\"\"s Vineyard'");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "Martha\"\"s Vineyard");

    pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = \"Martha\"\"s Vineyard\"");
    Assert
        .assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getIdentifier().getName(),
            "Martha\"s Vineyard");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = \"Martha''s Vineyard\"");
    Assert
        .assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getIdentifier().getName(),
            "Martha''s Vineyard");
  }

  @Test
  public void testFilterCaluses() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where a > 1.5");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "a");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getDoubleValue(), 1.5);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b < 100");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 100L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where c >= 10");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where d <= 50");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "d");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 50L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where e BETWEEN 70 AND 80");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.BETWEEN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "e");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 70L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 80L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where regexp_like(E, '^U.*')");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), "regexp_like");
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "E");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getStringValue(), "^U.*");
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where f LIKE '%potato%'");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LIKE.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "f");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getStringValue(), "%potato%");
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where g IN (12, 13, 15.2, 17)");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
  }

  @Test
  public void testBrokerConverter() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where g IN (12, 13, 15.2, 17)");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
    Assert.assertEquals(tempBrokerRequest.getQuerySource().getTableName(), "vegetables");
    Assert.assertEquals(tempBrokerRequest.getSelections().getSelectionColumns().get(0), "*");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getOperator(), FilterOperator.IN);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getColumn(), "g");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().size(), 4);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(0), "12");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(1), "13");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(2), "15.2");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(3), "17");
  }

  @Test
  public void testSelectAs() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select sum(A) as sum_A, count(B) as count_B  from vegetables where g IN (12, 13, 15.2, 17)");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
    Assert.assertEquals(tempBrokerRequest.getQuerySource().getTableName(), "vegetables");
    Assert.assertNull(tempBrokerRequest.getSelections());
    Assert.assertEquals(tempBrokerRequest.getAggregationsInfo().get(0).getAggregationType(), "SUM");
    Assert.assertEquals(tempBrokerRequest.getAggregationsInfo().get(1).getAggregationType(), "COUNT");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getColumn(), "g");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().size(), 4);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(0), "12");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(1), "13");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(2), "15.2");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(3), "17");
  }

  @Test
  public void testDuplicateClauses() {
    assertCompilationFails("select top 5 count(*) from a top 8");
    assertCompilationFails("select count(*) from a where a = 1 limit 5 where b = 2");
    assertCompilationFails("select count(*) from a group by b limit 5 group by b");
    assertCompilationFails("select count(*) from a having sum(a) = 8 limit 5 having sum(a) = 9");
    assertCompilationFails("select count(*) from a order by b limit 5 order by c");
    assertCompilationFails("select count(*) from a limit 5 limit 5");
  }

  @Test
  public void testTopZero() {
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 100", 100, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 0", 0, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 1", 1, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT -1", -1, true);
  }

  private void assertCompilationFails(String query) {
    try {
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (SqlCompilationException e) {
      // Expected
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  private void testTopZeroFor(String s, final int expectedTopN, boolean parseException) {
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(s);
    } catch (SqlCompilationException e) {
      if (parseException) {
        return;
      }
      throw e;
    }

    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertEquals(expectedTopN, pinotQuery.getLimit());
  }

  @Test
  public void testRejectInvalidLexerToken() {
    assertCompilationFails("select foo from bar where baz ?= 2");
    assertCompilationFails("select foo from bar where baz =! 2");
  }

  @Test
  public void testRejectInvalidParses() {
    assertCompilationFails("select foo from bar where baz < > 2");
    assertCompilationFails("select foo from bar where baz ! = 2");
  }

  @Test
  public void testParseExceptionHasCharacterPosition() {
    final String query = "select foo from bar where baz ? 2";

    try {
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (SqlCompilationException e) {
      // Expected
      Assert.assertTrue(e.getMessage().contains("at line 1, column 31."),
          "Compilation exception should contain line and character for error message. Error message is " + e
              .getMessage());
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  @Test
  public void testCStyleInequalityOperator() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "NOT_EQUALS");

    // Bang equal '!=' is not allowed under the current SQL conformance level
    assertCompilationFails("select * from vegetables where name != 'Brussels sprouts'");
  }

  @Test
  public void testQueryOptions() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0);
    Assert.assertNull(pinotQuery.getQueryOptions());

    pinotQuery = CalciteSqlParser
        .compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes)");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes, foo=1234, bar='potato')");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes) option(foo=1234) option(bar='potato')");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");
  }

  @Test
  public void testIdentifierQuoteCharacter() {
    PinotQuery pinotQuery = CalciteSqlParser
        .compileToPinotQuery("select avg(attributes.age) as avg_age from person group by attributes.address_city");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "attributes.age");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "attributes.address_city");
  }

  @Test
  public void testStringLiteral() {
    // Do not allow string literal column in selection query
    assertCompilationFails("SELECT 'foo' FROM table");

    // Allow string literal column in aggregation and group-by query
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT SUM('foo'), MAX(bar) FROM myTable GROUP BY 'foo', bar");
    List<Expression> selectFunctionList = pinotQuery.getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 2);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(),
        "foo");
    Assert.assertEquals(selectFunctionList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "bar");
    List<Expression> groupbyList = pinotQuery.getGroupByList();
    Assert.assertEquals(groupbyList.size(), 2);
    Assert.assertEquals(groupbyList.get(0).getLiteral().getStringValue(), "foo");
    Assert.assertEquals(groupbyList.get(1).getIdentifier().getName(), "bar");

    // For UDF, string literal won't be treated as column but as LITERAL
    pinotQuery = CalciteSqlParser
        .compileToPinotQuery("SELECT SUM(ADD(foo, 'bar')) FROM myTable GROUP BY sub(foo, bar), SUB(BAR, FOO)");
    selectFunctionList = pinotQuery.getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 1);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperator(), "SUM");
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().size(), 1);
    Assert
        .assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
            "ADD");
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "foo");
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getStringValue(), "bar");
    groupbyList = pinotQuery.getGroupByList();
    Assert.assertEquals(groupbyList.size(), 2);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperator(), "sub");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "bar");

    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperator(), "SUB");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "BAR");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "FOO");
  }

  @Test
  public void testPqlAndSqlCompatible()
      throws IOException {
    try (BufferedReader sqlReader = new BufferedReader(
        new InputStreamReader(CalciteSqlCompilerTest.class.getClassLoader().getResourceAsStream("sql_queries.list")));
        BufferedReader pqlReader = new BufferedReader(new InputStreamReader(
            CalciteSqlCompilerTest.class.getClassLoader().getResourceAsStream("pql_queries.list")))) {
      PinotQuery2BrokerRequestConverter pinotQuery2BrokerRequestConverter = new PinotQuery2BrokerRequestConverter();
      Pql2Compiler pqlCompiler = new Pql2Compiler();
      String sql;
      while ((sql = sqlReader.readLine()) != null) {
        // Compilation should not throw exception
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
        BrokerRequest sqlBrokerRequest = pinotQuery2BrokerRequestConverter.convert(pinotQuery);

        BrokerRequest pqlBrokerRequest = pqlCompiler.compileToBrokerRequest(pqlReader.readLine());
        // TODO: compare SQL/PQL broker request
      }
    }
  }

  @Test
  public void testFilterUdf() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select count(*) from baseballStats where DIV(numberOfGames,10) = 100");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "COUNT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "DIV");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "numberOfGames");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 10);
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 100);

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = 1394323200");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "COUNT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "timeConvert");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "DaysSinceEpoch");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getStringValue(), "DAYS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(2)
            .getLiteral().getStringValue(), "SECONDS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(),
        1394323200);
  }

  @Test
  public void testSelectionTransformFunction() {
    PinotQuery pinotQuery = CalciteSqlParser
        .compileToPinotQuery("  select mapKey(mapField,k1) from baseballStats where mapKey(mapField,k1) = 'v1'");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "mapKey");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "mapField");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "k1");

    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "mapKey");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "mapField");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "k1");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "v1");
  }

  @Test
  public void testSqlDistinctQueryCompilation() {
    Pql2Compiler.ENABLE_DISTINCT = true;

    // test single column DISTINCT
    String sql = "SELECT DISTINCT c1 FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    List<Expression> selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    Function distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Identifier c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");

    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest brokerRequest = converter.convert(pinotQuery);
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    AggregationInfo aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "c1");

    // test multi column DISTINCT
    sql = "SELECT DISTINCT c1, c2 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 2);

    c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Identifier c2 = distinctFunction.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");
    Assert.assertEquals(c2.getName(), "c2");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "c1:c2");

    // test multi column DISTINCT with filter
    sql = "SELECT DISTINCT c1, c2, c3 FROM foo WHERE c3 > 100";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 3);

    final Expression filter = pinotQuery.getFilterExpression();
    Assert.assertEquals(filter.getFunctionCall().getOperator(), "GREATER_THAN");
    Assert.assertEquals(filter.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "c3");
    Assert.assertEquals(filter.getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 100);

    c1 = distinctFunction.getOperands().get(0).getIdentifier();
    c2 = distinctFunction.getOperands().get(1).getIdentifier();
    Identifier c3 = distinctFunction.getOperands().get(2).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");
    Assert.assertEquals(c2.getName(), "c2");
    Assert.assertEquals(c3.getName(), "c3");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "c1:c2:c3");

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT sum(c1), DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 15."));
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT c1, DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 10."));
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT DIV(c1,c2), DISTINCT c3 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 18."));
    }

    // The following query although a valid SQL syntax is not
    // very helpful since the result will be one row -- probably a
    // a single random value from c1 and sum of c2.
    // we can support this if underlying engine
    // implements sum as a transform function which is not the case today
    // this is a multi column distinct so we cant treat this query
    // as having 2 independent functions -- sum(c2) should be an input
    // into distinct and that can't happen unless it is implemented as a
    // transform
    try {
      sql = "SELECT DISTINCT c1, sum(c2) FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(
          e.getMessage().contains("Syntax error: Use of DISTINCT with aggregation functions is not supported"));
    }

    // same reason as above
    try {
      sql = "SELECT DISTINCT sum(c1) FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(
          e.getMessage().contains("Syntax error: Use of DISTINCT with aggregation functions is not supported"));
    }

    // Pinot currently does not support DISTINCT * syntax
    try {
      sql = "SELECT DISTINCT * FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains(
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after DISTINCT keyword"));
    }

    // Pinot currently does not support DISTINCT * syntax
    try {
      sql = "SELECT DISTINCT *, C1 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains(
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after DISTINCT keyword"));
    }

    // Pinot currently does not support GROUP BY with DISTINCT
    try {
      sql = "SELECT DISTINCT C1, C2 FROM foo GROUP BY C1";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("DISTINCT with GROUP BY is not supported"));
    }

    // distinct with transform is supported since the output of
    // transform can be piped into distinct function

    // test DISTINCT with single transform function
    sql = "SELECT DISTINCT add(col1,col2) FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Function add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    c1 = add.getOperands().get(0).getIdentifier();
    c2 = add.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "add(col1,col2)");

    // multi-column distinct with multiple transform functions
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4) FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 2);

    // check for DISTINCT's first operand ADD(....)
    add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    Function div = add.getOperands().get(0).getFunctionCall();
    Function mul = add.getOperands().get(1).getFunctionCall();

    // check for ADD's first operand DIV(...)
    Assert.assertEquals(div.getOperator(), "div");
    Assert.assertEquals(div.getOperands().size(), 2);
    c1 = div.getOperands().get(0).getIdentifier();
    c2 = div.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    // check for ADD's second operand MUL(...)
    Assert.assertEquals(mul.getOperator(), "mul");
    Assert.assertEquals(mul.getOperands().size(), 2);
    c1 = mul.getOperands().get(0).getIdentifier();
    c2 = mul.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's second operand SUB(...)
    Function sub = distinctFunction.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(sub.getOperator(), "sub");
    Assert.assertEquals(sub.getOperands().size(), 2);
    c1 = sub.getOperands().get(0).getIdentifier();
    c2 = sub.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check conversion to broker request
    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "add(div(col1,col2),mul(col3,col4)):sub(col3,col4)");

    // multi-column distinct with multiple transform columns and additional identifiers
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4), col5, col6 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 4);

    // check for DISTINCT's first operand ADD(....)
    add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    div = add.getOperands().get(0).getFunctionCall();
    mul = add.getOperands().get(1).getFunctionCall();

    // check for ADD's first operand DIV(...)
    Assert.assertEquals(div.getOperator(), "div");
    Assert.assertEquals(div.getOperands().size(), 2);
    c1 = div.getOperands().get(0).getIdentifier();
    c2 = div.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    // check for ADD's second operand MUL(...)
    Assert.assertEquals(mul.getOperator(), "mul");
    Assert.assertEquals(mul.getOperands().size(), 2);
    c1 = mul.getOperands().get(0).getIdentifier();
    c2 = mul.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's second operand SUB(...)
    sub = distinctFunction.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(sub.getOperator(), "sub");
    Assert.assertEquals(sub.getOperands().size(), 2);
    c1 = sub.getOperands().get(0).getIdentifier();
    c2 = sub.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's third operand col5
    c1 = distinctFunction.getOperands().get(2).getIdentifier();
    Assert.assertEquals(c1.getName(), "col5");

    // check for DISTINCT's fourth operand col6
    c2 = distinctFunction.getOperands().get(3).getIdentifier();
    Assert.assertEquals(c2.getName(), "col6");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO),
        "add(div(col1,col2),mul(col3,col4)):sub(col3,col4):col5:col6");
  }
}
